use crate::core::MtopError;
use crate::dns::core::{RecordClass, RecordType};
use crate::dns::message::{Flags, Message, MessageId, Question};
use crate::dns::name::Name;
use crate::dns::resolv::ResolvConf;
use crate::timeout::Timeout;
use std::io::{self, Cursor};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, UdpSocket};

const DEFAULT_MESSAGE_BUFFER: usize = 512;

#[derive(Debug)]
pub struct DnsClient {
    config: ResolvConf,
    server: AtomicUsize,
}

impl DnsClient {
    /// Create a new DnsClient that will resolve names using UDP or TCP connections
    /// and behavior based on a resolv.conf configuration file.
    pub fn new(config: ResolvConf) -> Self {
        Self {
            config,
            server: AtomicUsize::new(0),
        }
    }

    /// Perform a DNS lookup with the configured nameservers.
    ///
    /// Timeouts and network errors will result in up to one additional attempt
    /// to perform a DNS lookup when using the default configuration.
    pub async fn resolve(&self, name: Name, rtype: RecordType, rclass: RecordClass) -> Result<Message, MtopError> {
        let full = name.to_fqdn();
        let id = MessageId::random();
        let flags = Flags::default().set_recursion_desired();
        let question = Question::new(full, rtype).set_qclass(rclass);
        let message = Message::new(id, flags).add_question(question);

        let mut attempt = 0;
        loop {
            match self.exchange(&message, usize::from(attempt)).await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if attempt + 1 >= self.config.options.attempts {
                        return Err(e);
                    }

                    tracing::debug!(message = "retrying failed query", attempt = attempt + 1, max_attempts = self.config.options.attempts, err = %e);
                    attempt += 1;
                }
            }
        }
    }

    async fn exchange(&self, msg: &Message, attempt: usize) -> Result<Message, MtopError> {
        let server = self.nameserver(attempt);

        let res = async {
            let client = self.udp_client(server).await?;
            client.exchange(msg).await
        }
        .timeout(self.config.options.timeout, format!("client.exchange udp://{}", server))
        .await?;

        // If the UDP response indicates the message was truncated, we discard
        // it and repeat the query using TCP.
        if res.flags().is_truncated() {
            tracing::debug!(message = "UDP response truncated, retrying with TCP", flags = ?res.flags(), server = %server);
            async {
                let mut client = self.tcp_client(server).await?;
                client.exchange(msg).await
            }
            .timeout(self.config.options.timeout, format!("client.exchange tcp://{}", server))
            .await
        } else {
            Ok(res)
        }
    }

    async fn udp_client(&self, server: SocketAddr) -> Result<UdpClient<UdpSocket>, MtopError> {
        let local = if server.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" };
        let sock = UdpSocket::bind(local).await?;
        sock.connect(server).await?;
        Ok(UdpClient::new(sock, DEFAULT_MESSAGE_BUFFER))
    }

    async fn tcp_client(&self, server: SocketAddr) -> Result<TcpClient<OwnedReadHalf, OwnedWriteHalf>, MtopError> {
        let stream = TcpStream::connect(server).await?;
        let (read, write) = stream.into_split();
        Ok(TcpClient::new(read, write, DEFAULT_MESSAGE_BUFFER))
    }

    fn nameserver(&self, attempt: usize) -> SocketAddr {
        let idx = if self.config.options.rotate {
            self.server.fetch_add(1, Ordering::Relaxed)
        } else {
            attempt
        };

        self.config.nameservers[idx % self.config.nameservers.len()]
    }
}

impl Clone for DnsClient {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            server: AtomicUsize::new(0),
        }
    }
}

struct TcpClient<R, W>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
{
    read: BufReader<R>,
    write: BufWriter<W>,
    size: usize,
}

impl<R, W> TcpClient<R, W>
where
    R: AsyncRead + Unpin + Sync + Send + 'static,
    W: AsyncWrite + Unpin + Sync + Send + 'static,
{
    fn new(read: R, write: W, size: usize) -> Self {
        Self {
            read: BufReader::new(read),
            write: BufWriter::new(write),
            size,
        }
    }

    async fn exchange(&mut self, msg: &Message) -> Result<Message, MtopError> {
        let mut buf = Vec::with_capacity(self.size);

        // Write the message to a local buffer and then send it, prefixed
        // with the size of the message.
        msg.write_network_bytes(&mut buf)?;
        self.write.write_u16(buf.len() as u16).await?;
        self.write.write_all(&buf).await?;
        self.write.flush().await?;

        // Read the prefixed size of the response in big-endian (network)
        // order and then read exactly that many bytes into our buffer.
        let sz = self.read.read_u16().await?;
        buf.clear();
        buf.resize(usize::from(sz), 0);
        self.read.read_exact(&mut buf).await?;

        let mut cur = Cursor::new(buf);
        let res = Message::read_network_bytes(&mut cur)?;
        if res.id() != msg.id() {
            Err(MtopError::runtime(format!(
                "unexpected DNS MessageId; expected {}, got {}",
                msg.id(),
                res.id()
            )))
        } else {
            Ok(res)
        }
    }
}

struct UdpClient<T>
where
    T: AsyncDatagram + Unpin + Send + Sync + 'static,
{
    sock: T,
    size: usize,
}

impl<T> UdpClient<T>
where
    T: AsyncDatagram + Unpin + Send + Sync + 'static,
{
    fn new(sock: T, size: usize) -> Self {
        Self { sock, size }
    }

    async fn exchange(&self, msg: &Message) -> Result<Message, MtopError> {
        let mut buf = Vec::with_capacity(self.size);
        msg.write_network_bytes(&mut buf)?;
        self.sock.send(&buf).await?;

        buf.clear();
        buf.resize(self.size, 0);

        loop {
            let n = self.sock.recv(&mut buf).await?;
            let cur = Cursor::new(&buf[0..n]);
            let res = Message::read_network_bytes(cur)?;
            if res.id() == msg.id() {
                return Ok(res);
            }
        }
    }
}

trait AsyncDatagram {
    async fn send(&self, buf: &[u8]) -> io::Result<usize>;
    async fn recv(&self, buf: &mut [u8]) -> io::Result<usize>;
}

impl AsyncDatagram for UdpSocket {
    async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        UdpSocket::send(self, buf).await
    }

    async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        UdpSocket::recv(self, buf).await
    }
}

#[cfg(test)]
mod test {
    use super::{AsyncDatagram, TcpClient, UdpClient};
    use crate::core::ErrorKind;
    use crate::dns::core::{RecordClass, RecordType};
    use crate::dns::message::{Flags, Message, MessageId, Question, Record};
    use crate::dns::name::Name;
    use crate::dns::rdata::{RecordData, RecordDataA};
    use std::io::{self, Cursor, Write};
    use std::net::Ipv4Addr;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Mutex;

    #[rustfmt::skip]
    fn new_message_bytes(id: u16, size_prefix: bool) -> Vec<u8> {
        let body = &[
            // Header, NOT including the two byte ID
            128, 0, // Flags: response, query op, no error
            0, 1,   // questions
            0, 1,   // answers
            0, 0,   // authority
            0, 0,   // extra

            // Question
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root
            0, 1,                             // record type, A
            0, 1,                             // record class, INET

            // Answer
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root
            0, 1,                             // record type, A
            0, 1,                             // record class, INET
            0, 0, 0, 60,                      // TTL
            0, 4,                             // rdata size
            127, 0, 0, 100,                   // rdata, A address
        ];

        let mut out = Vec::new();
        if size_prefix {
            let size = 2 /* ID */ + body.len();
            let size_bytes = (size as u16).to_be_bytes();
            out.extend_from_slice(&size_bytes);
        }

        let id_bytes = id.to_be_bytes();
        out.extend_from_slice(&id_bytes);
        out.extend_from_slice(body);

        out
    }

    #[tokio::test]
    async fn test_tcp_client_eof_reading_length() {
        let write = Vec::new();
        let read = Cursor::new(Vec::new());

        let question = Question::new(Name::from_str("example.com.").unwrap(), RecordType::A);
        let message =
            Message::new(MessageId::from(123), Flags::default().set_recursion_desired()).add_question(question);

        let mut client = TcpClient::new(read, write, 512);
        let res = client.exchange(&message).await;
        let err = res.unwrap_err();

        assert_eq!(ErrorKind::IO, err.kind());
    }

    #[tokio::test]
    async fn test_tcp_client_eof_reading_message() {
        let write = Vec::new();
        let read = Cursor::new(vec![
            0, 200, // message length
        ]);

        let question = Question::new(Name::from_str("example.com.").unwrap(), RecordType::A);
        let message =
            Message::new(MessageId::from(123), Flags::default().set_recursion_desired()).add_question(question);

        let mut client = TcpClient::new(read, write, 512);
        let res = client.exchange(&message).await;
        let err = res.unwrap_err();

        assert_eq!(ErrorKind::IO, err.kind());
    }

    #[tokio::test]
    async fn test_tcp_client_id_mismatch() {
        let write: Vec<u8> = Vec::new();
        let read = Cursor::new(new_message_bytes(111 /* purposefully wrong */, true));

        let question = Question::new(Name::from_str("example.com.").unwrap(), RecordType::A);
        let message =
            Message::new(MessageId::from(123), Flags::default().set_recursion_desired()).add_question(question);

        let mut client = TcpClient::new(read, write, 512);
        let res = client.exchange(&message).await;
        let err = res.unwrap_err();

        assert_eq!(ErrorKind::Runtime, err.kind());
    }

    #[tokio::test]
    async fn test_tcp_client_success() {
        let write = Vec::new();
        let read = Cursor::new(new_message_bytes(123, true));

        let question = Question::new(Name::from_str("example.com.").unwrap(), RecordType::A);
        let message =
            Message::new(MessageId::from(123), Flags::default().set_recursion_desired()).add_question(question);

        let mut client = TcpClient::new(read, write, 512);
        let res = client.exchange(&message).await.unwrap();

        assert_eq!(message.id(), res.id());
        assert_eq!(message.questions()[0], res.questions()[0]);
        assert_eq!(
            Record::new(
                Name::from_str("example.com.").unwrap(),
                RecordType::A,
                RecordClass::INET,
                60,
                RecordData::A(RecordDataA::new(Ipv4Addr::new(127, 0, 0, 100))),
            ),
            res.answers()[0]
        );
    }

    #[derive(Debug, Default)]
    struct MockAsyncDatagram {
        writes: Mutex<Vec<Vec<u8>>>,
        reads: Mutex<Vec<Vec<u8>>>,
        reads_idx: AtomicUsize,
    }

    impl MockAsyncDatagram {
        fn new(reads: Vec<Vec<u8>>, writes: Vec<Vec<u8>>) -> Self {
            Self {
                writes: Mutex::new(writes),
                reads: Mutex::new(reads),
                reads_idx: AtomicUsize::new(0),
            }
        }

        fn num_reads(&self) -> usize {
            self.reads_idx.load(Ordering::Relaxed)
        }
    }

    impl AsyncDatagram for MockAsyncDatagram {
        async fn send(&self, buf: &[u8]) -> io::Result<usize> {
            let len = buf.len();
            let mut writes = self.writes.lock().await;
            writes.push(buf.to_vec());
            Ok(len)
        }

        async fn recv(&self, mut buf: &mut [u8]) -> io::Result<usize> {
            let reads = self.reads.lock().await;
            if reads.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected EOF reading buffer",
                ));
            }

            let idx = self.reads_idx.fetch_add(1, Ordering::Relaxed) % reads.len();
            let bytes = &reads[idx];
            buf.write(bytes)
        }
    }

    #[tokio::test]
    async fn test_udp_client_one_id_mismatch() {
        let write = Vec::new();
        let read = vec![
            new_message_bytes(111 /* purposefully wrong */, false),
            new_message_bytes(123, false),
        ];

        let question = Question::new(Name::from_str("example.com.").unwrap(), RecordType::A);
        let message =
            Message::new(MessageId::from(123), Flags::default().set_recursion_desired()).add_question(question);

        let sock = MockAsyncDatagram::new(read, write);
        let client = UdpClient::new(sock, 512);
        let res = client.exchange(&message).await.unwrap();

        assert_eq!(message.id(), res.id());
        assert_eq!(message.questions()[0], res.questions()[0]);
        assert_eq!(
            Record::new(
                Name::from_str("example.com.").unwrap(),
                RecordType::A,
                RecordClass::INET,
                60,
                RecordData::A(RecordDataA::new(Ipv4Addr::new(127, 0, 0, 100))),
            ),
            res.answers()[0]
        );
        assert_eq!(2, client.sock.num_reads());
    }

    #[tokio::test]
    async fn test_udp_client_success() {
        let write = Vec::new();
        let read = vec![new_message_bytes(123, false)];

        let question = Question::new(Name::from_str("example.com.").unwrap(), RecordType::A);
        let message =
            Message::new(MessageId::from(123), Flags::default().set_recursion_desired()).add_question(question);

        let sock = MockAsyncDatagram::new(read, write);
        let client = UdpClient::new(sock, 512);
        let res = client.exchange(&message).await.unwrap();

        assert_eq!(message.id(), res.id());
        assert_eq!(message.questions()[0], res.questions()[0]);
        assert_eq!(
            Record::new(
                Name::from_str("example.com.").unwrap(),
                RecordType::A,
                RecordClass::INET,
                60,
                RecordData::A(RecordDataA::new(Ipv4Addr::new(127, 0, 0, 100))),
            ),
            res.answers()[0]
        );
        assert_eq!(1, client.sock.num_reads());
    }
}
