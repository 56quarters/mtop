use crate::core::MtopError;
use crate::dns::core::{RecordClass, RecordType};
use crate::dns::message::{Flags, Message, MessageId, Question};
use crate::dns::name::Name;
use crate::net::tcp_connect;
use crate::pool::{ClientFactory, ClientPool, ClientPoolConfig};
use crate::timeout::Timeout;
use std::fmt;
use std::future::Future;
use std::io::{self, Cursor, Error};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter, ReadBuf};
use tokio::net::UdpSocket;

const DEFAULT_NAMESERVER: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 53);
const DEFAULT_MESSAGE_BUFFER: usize = 512;

/// Configuration for creating a new `DnsClient` instance.
#[derive(Debug, Clone)]
pub struct DnsClientConfig {
    /// One or more DNS nameservers to use for resolution. These servers will be tried
    /// in order for each resolution unless `rotate` is set.
    pub nameservers: Vec<SocketAddr>,

    /// Timeout for each resolution. This timeout is applied to each attempt and so a
    /// single call to `DnsClient::resolve` may take longer based on the value of `attempts`.
    pub timeout: Duration,

    /// Number of attempts to make performing a resolution for a single name. Note that
    /// any response from a DNS server counts as "success". Only timeout or network errors
    /// will trigger retries.
    pub attempts: u8,

    /// If true, `nameservers` will be round-robin load balanced for each resolution. If false
    /// the nameservers are tried in-order for each resolution.
    pub rotate: bool,

    /// Max number of open sockets or connections to each nameserver. Default is to keep one
    /// open socket or connection per nameserver. Set to 0 to disable this behavior.
    pub pool_max_idle: u64,
}

impl Default for DnsClientConfig {
    fn default() -> Self {
        // Default values picked based on `man 5 resolv.conf` when relevant.
        Self {
            nameservers: vec![DEFAULT_NAMESERVER],
            timeout: Duration::from_secs(5),
            attempts: 2,
            rotate: false,
            pool_max_idle: 1,
        }
    }
}

/// Client for performing DNS queries and returning the results.
///
/// There is currently only a single non-test implementation because this
/// trait exists to make testing consumers easier.
pub trait DnsClient {
    fn resolve(
        &self,
        name: Name,
        rtype: RecordType,
        rclass: RecordClass,
    ) -> impl Future<Output = Result<Message, MtopError>>;
}

/// Implementation of a `DnsClient` that uses UDP with TCP fallback.
///
/// Supports nameserver rotation, retries, timeouts, and pooling of client
/// connections. Names are assumed to already be fully qualified, meaning
/// that they are not combined with a search domain.
///
/// Timeouts are handled by the client itself and so callers should _not_
/// add a timeout on the `resolve` method. Note that timeouts are per-network
/// operation. This means that a single call to `resolve` make take longer
/// than the timeout since failed network operations are retried.
#[derive(Debug)]
pub struct DefaultDnsClient<U = UdpConnectionFactory, T = TcpConnectionFactory>
where
    U: ClientFactory<SocketAddr, UdpConnection> + Send + Sync + 'static,
    T: ClientFactory<SocketAddr, TcpConnection> + Send + Sync + 'static,
{
    config: DnsClientConfig,
    server_idx: AtomicUsize,
    udp_pool: ClientPool<SocketAddr, UdpConnection, U>,
    tcp_pool: ClientPool<SocketAddr, TcpConnection, T>,
}

impl<U, T> DefaultDnsClient<U, T>
where
    U: ClientFactory<SocketAddr, UdpConnection> + Send + Sync + 'static,
    T: ClientFactory<SocketAddr, TcpConnection> + Send + Sync + 'static,
{
    /// Create a new DnsClient that will resolve names using UDP or TCP connections
    /// and behavior based on a resolv.conf configuration file.
    pub fn new(config: DnsClientConfig, udp_factory: U, tcp_factory: T) -> Self {
        let udp_config = ClientPoolConfig {
            name: "dns-udp".to_owned(),
            max_idle: config.pool_max_idle,
        };

        let tcp_config = ClientPoolConfig {
            name: "dns-tcp".to_owned(),
            max_idle: config.pool_max_idle,
        };

        Self {
            config,
            server_idx: AtomicUsize::new(0),
            udp_pool: ClientPool::new(udp_config, udp_factory),
            tcp_pool: ClientPool::new(tcp_config, tcp_factory),
        }
    }

    async fn exchange(&self, msg: &Message, attempt: usize) -> Result<Message, MtopError> {
        let server = self.nameserver(attempt);

        let res = async {
            let mut conn = self.udp_pool.get(&server).await?;
            let res = conn.exchange(msg).await;
            if res.is_ok() {
                self.udp_pool.put(conn).await;
            }

            res
        }
        .timeout(self.config.timeout, format!("client.exchange udp://{}", server))
        .await?;

        // If the UDP response indicates the message was truncated, we discard
        // it and repeat the query using TCP.
        if res.flags().is_truncated() {
            tracing::debug!(message = "UDP response truncated, retrying with TCP", flags = ?res.flags(), server = %server);
            async {
                let mut conn = self.tcp_pool.get(&server).await?;
                let res = conn.exchange(msg).await;
                if res.is_ok() {
                    self.tcp_pool.put(conn).await;
                }

                res
            }
            .timeout(self.config.timeout, format!("client.exchange tcp://{}", server))
            .await
        } else {
            Ok(res)
        }
    }

    fn nameserver(&self, attempt: usize) -> SocketAddr {
        let idx = if self.config.rotate {
            self.server_idx.fetch_add(1, Ordering::Relaxed)
        } else {
            attempt
        };

        self.config.nameservers[idx % self.config.nameservers.len()]
    }
}

impl<U, T> DnsClient for DefaultDnsClient<U, T>
where
    U: ClientFactory<SocketAddr, UdpConnection> + Send + Sync + 'static,
    T: ClientFactory<SocketAddr, TcpConnection> + Send + Sync + 'static,
{
    async fn resolve(&self, name: Name, rtype: RecordType, rclass: RecordClass) -> Result<Message, MtopError> {
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
                    if attempt + 1 >= self.config.attempts {
                        return Err(e);
                    }

                    tracing::debug!(message = "retrying failed query", attempt = attempt + 1, max_attempts = self.config.attempts, err = %e);
                    attempt += 1;
                }
            }
        }
    }
}

/// Connection for unconditionally sending and receiving DNS messages using TCP streams.
/// Messages are sent with a two byte prefix that indicates the size of the message.
/// Responses are expected to have the same prefix. The message ID of responses is
/// checked to ensure it matches the request ID. If it does not, an error is returned.
pub struct TcpConnection {
    read: BufReader<Box<dyn AsyncRead + Send + Sync + Unpin>>,
    write: BufWriter<Box<dyn AsyncWrite + Send + Sync + Unpin>>,
    buffer: Vec<u8>,
    bytes_read: AtomicUsize,
    bytes_written: AtomicUsize,
}

impl TcpConnection {
    pub fn new<R, W>(read: R, write: W, size: usize) -> Self
    where
        R: AsyncRead + Unpin + Sync + Send + 'static,
        W: AsyncWrite + Unpin + Sync + Send + 'static,
    {
        Self {
            read: BufReader::new(Box::new(read)),
            write: BufWriter::new(Box::new(write)),
            buffer: Vec::with_capacity(size),
            bytes_read: AtomicUsize::new(0),
            bytes_written: AtomicUsize::new(0),
        }
    }

    pub async fn exchange(&mut self, msg: &Message) -> Result<Message, MtopError> {
        // Write the message to a local buffer and then send it, prefixed
        // with the size of the message.
        self.buffer.clear();
        msg.write_network_bytes(&mut self.buffer)?;
        self.write.write_u16(self.buffer.len() as u16).await?;
        self.write.write_all(&self.buffer).await?;
        self.write.flush().await?;

        // Increment total bytes written including the request size prefix.
        self.bytes_written.fetch_add(self.buffer.len() + 2, Ordering::Relaxed);

        // Read the prefixed size of the response in big-endian (network)
        // order and then read exactly that many bytes into our buffer.
        let sz = self.read.read_u16().await?;
        self.buffer.clear();
        self.buffer.resize(usize::from(sz), 0);
        self.read.read_exact(&mut self.buffer).await?;

        // Increment total bytes read including the response size prefix.
        self.bytes_read.fetch_add(self.buffer.len() + 2, Ordering::Relaxed);

        let mut cur = Cursor::new(&self.buffer);
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

    pub fn bytes_written(&self) -> usize {
        self.bytes_written.load(Ordering::Relaxed)
    }

    pub fn bytes_read(&self) -> usize {
        self.bytes_read.load(Ordering::Relaxed)
    }
}

impl fmt::Debug for TcpConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TcpConnection {{ ... }}")
    }
}

/// Connection for unconditionally sending and receiving DNS messages using UDP packets.
/// The message ID of responses is checked to ensure it matches the request ID. If it
/// does not, the response is discarded and the client will wait for another response
/// until it gets one with a matching ID.
pub struct UdpConnection {
    read: Box<dyn AsyncRead + Send + Sync + Unpin>,
    write: Box<dyn AsyncWrite + Send + Sync + Unpin>,
    buffer: Vec<u8>,
    packet: usize,
}

impl UdpConnection {
    pub fn new<R, W>(read: R, write: W, size: usize) -> Self
    where
        R: AsyncRead + Unpin + Sync + Send + 'static,
        W: AsyncWrite + Unpin + Sync + Send + 'static,
    {
        Self {
            read: Box::new(read),
            write: Box::new(write),
            buffer: Vec::with_capacity(size),
            packet: size,
        }
    }

    pub async fn exchange(&mut self, msg: &Message) -> Result<Message, MtopError> {
        self.buffer.clear();
        msg.write_network_bytes(&mut self.buffer)?;
        // We expect this to be a datagram socket so we only do a single write.
        let n = self.write.write(&self.buffer).await?;
        if n != self.buffer.len() {
            return Err(MtopError::runtime(format!(
                "short write to UDP socket. expected {}, got {}",
                self.buffer.len(),
                n
            )));
        }
        self.write.flush().await?;

        // Resize to our packet size since the .read() call will only read up to
        // the size of the buffer at most.
        self.buffer.clear();
        self.buffer.resize(self.packet, 0);

        loop {
            let n = self.read.read(&mut self.buffer).await?;
            let cur = Cursor::new(&self.buffer[0..n]);
            let res = Message::read_network_bytes(cur)?;
            if res.id() == msg.id() {
                return Ok(res);
            }
        }
    }
}

impl fmt::Debug for UdpConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UdpConnection {{ ... }}")
    }
}

/// Adapter for reading and writing to a `UdpSocket` using the `AsyncRead` and `AsyncWrite`
/// traits. This exists to enable easier testing of `UdpClient` by allowing alternate
/// implementations of those traits to be used.
pub(crate) struct SocketAdapter(UdpSocket);

impl SocketAdapter {
    pub fn new(sock: UdpSocket) -> Self {
        Self(sock)
    }
}

impl AsyncRead for SocketAdapter {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        self.0.poll_recv(cx, buf)
    }
}

impl AsyncWrite for SocketAdapter {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Error>> {
        self.0.poll_send(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}

/// Implementation of `ClientFactory` for creating concrete `UdpConnection` instances
/// that use a UDP socket.
#[derive(Debug, Clone, Default)]
pub struct UdpConnectionFactory;

impl ClientFactory<SocketAddr, UdpConnection> for UdpConnectionFactory {
    async fn make(&self, address: &SocketAddr) -> Result<UdpConnection, MtopError> {
        let local = if address.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" };
        let sock = UdpSocket::bind(local).await?;
        sock.connect(address).await?;

        let adapter = SocketAdapter::new(sock);
        let (read, write) = tokio::io::split(adapter);
        Ok(UdpConnection::new(read, write, DEFAULT_MESSAGE_BUFFER))
    }
}

/// Implementation of `ClientFactory` for creating concrete `TcpConnection` instances
/// that use a TCP socket.
#[derive(Debug, Clone, Default)]
pub struct TcpConnectionFactory;

impl ClientFactory<SocketAddr, TcpConnection> for TcpConnectionFactory {
    async fn make(&self, address: &SocketAddr) -> Result<TcpConnection, MtopError> {
        let (read, write) = tcp_connect(address).await?;
        Ok(TcpConnection::new(read, write, DEFAULT_MESSAGE_BUFFER))
    }
}

#[cfg(test)]
mod test {
    use super::{TcpConnection, UdpConnection};
    use crate::core::ErrorKind;
    use crate::dns::core::{RecordClass, RecordType};
    use crate::dns::message::{Flags, Message, MessageId, Question, Record};
    use crate::dns::name::Name;
    use crate::dns::rdata::{RecordData, RecordDataA};
    use std::collections::VecDeque;
    use std::io::Cursor;
    use std::net::Ipv4Addr;
    use std::pin::Pin;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, ReadBuf};

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

        let mut client = TcpConnection::new(read, write, 512);
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

        let mut client = TcpConnection::new(read, write, 512);
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

        let mut client = TcpConnection::new(read, write, 512);
        let res = client.exchange(&message).await;
        let err = res.unwrap_err();

        assert_eq!(ErrorKind::Runtime, err.kind());
    }

    #[tokio::test]
    async fn test_tcp_client_multiple_messages() {
        let write = Vec::new();
        let read = {
            let response1 = new_message_bytes(123, true);
            let response2 = new_message_bytes(456, true);
            let mut bytes = Vec::new();
            bytes.extend(response1);
            bytes.extend(response2);
            Cursor::new(bytes)
        };

        let mut client = TcpConnection::new(read, write, 512);

        let question = Question::new(Name::from_str("example.com.").unwrap(), RecordType::A);
        let message1 =
            Message::new(MessageId::from(123), Flags::default().set_recursion_desired()).add_question(question.clone());
        let message2 =
            Message::new(MessageId::from(456), Flags::default().set_recursion_desired()).add_question(question.clone());

        let res1 = client.exchange(&message1).await.unwrap();
        assert_eq!(message1.id(), res1.id());
        assert_eq!(message1.questions()[0], res1.questions()[0]);
        assert_eq!(
            Record::new(
                Name::from_str("example.com.").unwrap(),
                RecordType::A,
                RecordClass::INET,
                60,
                RecordData::A(RecordDataA::new(Ipv4Addr::new(127, 0, 0, 100))),
            ),
            res1.answers()[0]
        );

        let res2 = client.exchange(&message2).await.unwrap();
        assert_eq!(message2.id(), res2.id());
        assert_eq!(message2.questions()[0], res2.questions()[0]);
        assert_eq!(
            Record::new(
                Name::from_str("example.com.").unwrap(),
                RecordType::A,
                RecordClass::INET,
                60,
                RecordData::A(RecordDataA::new(Ipv4Addr::new(127, 0, 0, 100))),
            ),
            res2.answers()[0]
        );

        let expected_bytes = message1.size() + message2.size() + 2 + 2;
        assert_eq!(expected_bytes, client.bytes_written());
    }

    #[tokio::test]
    async fn test_tcp_client_success() {
        let write = Vec::new();
        let read = Cursor::new(new_message_bytes(123, true));

        let question = Question::new(Name::from_str("example.com.").unwrap(), RecordType::A);
        let message =
            Message::new(MessageId::from(123), Flags::default().set_recursion_desired()).add_question(question);

        let mut client = TcpConnection::new(read, write, 512);
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

    struct MockReadSocket {
        values: VecDeque<Vec<u8>>,
        reads: Arc<AtomicU64>,
    }

    impl MockReadSocket {
        fn new(values: Vec<Vec<u8>>, reads: Arc<AtomicU64>) -> Self {
            Self {
                values: VecDeque::from(values),
                reads,
            }
        }
    }

    impl AsyncRead for MockReadSocket {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            self.reads.fetch_add(1, Ordering::AcqRel);
            let b = self.values.pop_front().unwrap();
            buf.put_slice(&b);
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_udp_client_one_id_mismatch() {
        let write = Vec::new();
        let read_count = Arc::new(AtomicU64::new(0));
        let read = MockReadSocket::new(
            vec![
                new_message_bytes(111 /* purposefully wrong */, false),
                new_message_bytes(123, false),
            ],
            read_count.clone(),
        );

        let question = Question::new(Name::from_str("example.com.").unwrap(), RecordType::A);
        let message =
            Message::new(MessageId::from(123), Flags::default().set_recursion_desired()).add_question(question);

        let mut client = UdpConnection::new(read, write, 512);
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
        assert_eq!(2, read_count.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_udp_client_success() {
        let write = Vec::new();
        let read_count = Arc::new(AtomicU64::new(0));
        let read = MockReadSocket::new(vec![new_message_bytes(123, false)], read_count.clone());

        let question = Question::new(Name::from_str("example.com.").unwrap(), RecordType::A);
        let message =
            Message::new(MessageId::from(123), Flags::default().set_recursion_desired()).add_question(question);

        let mut client = UdpConnection::new(read, write, 512);
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
        assert_eq!(1, read_count.load(Ordering::Acquire));
    }
}
