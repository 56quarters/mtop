use crate::core::MtopError;
use crate::dns::core::{RecordClass, RecordType};
use crate::dns::message::{Flags, Message, MessageId, Question};
use crate::dns::name::Name;
use crate::dns::resolv::ResolvConf;
use crate::timeout::Timeout;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpStream, UdpSocket};

const DEFAULT_RECV_BUF: usize = 512;

#[derive(Debug)]
pub struct DnsClient {
    local: SocketAddr,
    config: ResolvConf,
    server: AtomicUsize,
}

impl DnsClient {
    /// Create a new DnsClient that will use a local address to open UDP or TCP
    /// connections and behavior based on a resolv.conf configuration file.
    pub fn new(local: SocketAddr, config: ResolvConf) -> Self {
        Self {
            local,
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
        let id = msg.id();
        let server = self.nameserver(attempt);

        // Wrap creating the socket, sending, and receiving in an async block
        // so that we can apply a single timeout to all operations and ensure
        // we have access to the nameserver to make the timeout message useful.
        let res = async {
            let sock = self.connect_udp(server).await?;
            self.send_udp(&sock, msg).await?;
            self.recv_udp(&sock, id).await
        }
        .timeout(self.config.options.timeout, format!("client.exchange udp://{}", server))
        .await?;

        // If the UDP response indicates the message was truncated, we discard
        // it and repeat the query using TCP.
        if res.flags().is_truncated() {
            tracing::debug!(message = "UDP response truncated, repeating with TCP", flags = ?res.flags(), server = %server);
            async {
                let mut sock = self.connect_tcp(server).await?;
                self.send_recv_tcp(&mut sock, msg).await
            }
            .timeout(self.config.options.timeout, format!("client.exchange tcp://{}", server))
            .await
        } else {
            Ok(res)
        }
    }

    async fn connect_udp(&self, server: SocketAddr) -> Result<UdpSocket, MtopError> {
        let sock = UdpSocket::bind(&self.local).await?;
        sock.connect(server).await?;
        Ok(sock)
    }

    async fn send_udp(&self, socket: &UdpSocket, msg: &Message) -> Result<(), MtopError> {
        let mut buf = Vec::new();
        msg.write_network_bytes(&mut buf)?;
        Ok(socket.send(&buf).await.map(|_| ())?)
    }

    async fn recv_udp(&self, socket: &UdpSocket, id: MessageId) -> Result<Message, MtopError> {
        let mut buf = vec![0_u8; DEFAULT_RECV_BUF];
        loop {
            let n = socket.recv(&mut buf).await?;
            let cur = Cursor::new(&buf[0..n]);
            let msg = Message::read_network_bytes(cur)?;
            if msg.id() == id {
                return Ok(msg);
            }
        }
    }

    async fn connect_tcp(&self, server: SocketAddr) -> Result<TcpStream, MtopError> {
        Ok(TcpStream::connect(server).await?)
    }

    async fn send_recv_tcp(&self, stream: &mut TcpStream, msg: &Message) -> Result<Message, MtopError> {
        let mut buf = Vec::with_capacity(DEFAULT_RECV_BUF);
        let (read, write) = stream.split();
        let mut read = BufReader::new(read);
        let mut write = BufWriter::new(write);

        // Write the message to a local buffer and then send it, prefixed
        // with the size of the message.
        msg.write_network_bytes(&mut buf)?;
        write.write_u16(buf.len() as u16).await?;
        write.write_all(&buf).await?;
        write.flush().await?;

        // Read the prefixed size of the response and then read exactly that
        // many bytes into our buffer.
        let sz = read.read_u16().await?;
        buf.clear();
        buf.resize(usize::from(sz), 0);
        read.read_exact(&mut buf).await?;

        let mut cur = Cursor::new(buf);
        let res = Message::read_network_bytes(&mut cur)?;
        if res.id() != msg.id() {
            Err(MtopError::runtime(format!(
                "unexpected DNS MessageId. expected {}, got {}",
                msg.id(),
                res.id()
            )))
        } else {
            Ok(res)
        }
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
            local: self.local,
            config: self.config.clone(),
            server: AtomicUsize::new(0),
        }
    }
}
