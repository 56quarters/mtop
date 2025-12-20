use crate::core::MtopError;
use crate::dns::core::{RecordClass, RecordType};
use crate::dns::message::{Flags, Message, MessageId, Question, ResponseCode};
use crate::dns::name::Name;
use crate::net::tcp_connect;
use crate::pool::{ClientFactory, ClientPool, ClientPoolConfig};
use crate::timeout::Timeout;
use async_trait::async_trait;
use std::fmt;
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
#[async_trait]
pub trait DnsClient {
    async fn resolve(
        &self,
        id: MessageId,
        name: Name,
        rtype: RecordType,
        rclass: RecordClass,
    ) -> Result<Message, MtopError>;
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
pub struct DefaultDnsClient {
    config: DnsClientConfig,
    server_idx: AtomicUsize,
    udp_pool: ClientPool<SocketAddr, UdpConnection>,
    tcp_pool: ClientPool<SocketAddr, TcpConnection>,
}

impl DefaultDnsClient {
    /// Create a new DnsClient that will resolve names using UDP or TCP connections
    /// and behavior based on a resolv.conf configuration file.
    pub fn new<U, T>(config: DnsClientConfig, udp_factory: U, tcp_factory: T) -> Self
    where
        U: ClientFactory<SocketAddr, UdpConnection> + Send + Sync + 'static,
        T: ClientFactory<SocketAddr, TcpConnection> + Send + Sync + 'static,
    {
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

    async fn exchange(&self, msg: &Message, server: &SocketAddr) -> Result<Message, MtopError> {
        let res = async {
            let mut conn = self.udp_pool.get(server).await?;
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
                let mut conn = self.tcp_pool.get(server).await?;
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

    // Get the index of nameserver that should be used for a query based on if the client has
    // been configured to roundrobin between nameservers or not.
    fn starting_idx(&self) -> usize {
        if self.config.rotate {
            self.server_idx.fetch_add(1, Ordering::Relaxed)
        } else {
            0
        }
    }

    // Get an iterator that will visit every nameserver once starting from the provided index.
    fn nameserver_iterator(&self, idx: usize) -> impl Iterator<Item = &SocketAddr> {
        self.config
            .nameservers
            .iter()
            .cycle()
            .skip(idx)
            .take(self.config.nameservers.len())
    }
}

#[async_trait]
impl DnsClient for DefaultDnsClient {
    async fn resolve(
        &self,
        id: MessageId,
        name: Name,
        rtype: RecordType,
        rclass: RecordClass,
    ) -> Result<Message, MtopError> {
        let full = name.to_fqdn();
        let flags = Flags::default().set_recursion_desired();
        let question = Question::new(full.clone(), rtype).set_qclass(rclass);
        let message = Message::new(id, flags).add_question(question);
        let start = self.starting_idx();

        let mut errors = Vec::new();
        for attempt in 0..self.config.attempts {
            for server in self.nameserver_iterator(start) {
                match self.exchange(&message, server).await {
                    Ok(v) => {
                        // NoError or a NameError is a conclusive answer. We either have results
                        // or this is a bad domain. Any other type of response means we have to try
                        // another server.
                        let rc = v.flags().get_response_code();
                        if rc == ResponseCode::NoError || rc == ResponseCode::NameError {
                            return Ok(v);
                        }

                        tracing::debug!(message = "unsuitable response from nameserver, trying next one", server = %server, response_code = ?rc);
                        errors.push(rc.to_string());
                    }
                    Err(e) => {
                        tracing::debug!(message = "nameserver failed, trying next one", server = %server, attempt = attempt + 1, max_attempts = self.config.attempts, err = %e);
                        errors.push(e.to_string());
                    }
                }
            }

            if attempt + 1 < self.config.attempts {
                tracing::debug!(
                    message = "all nameservers failed, retrying",
                    attempt = attempt + 1,
                    max_attempts = self.config.attempts
                );
            }
        }

        Err(MtopError::runtime(format!(
            "no nameservers returned suitable responses for name {} type {} class {}: {}",
            full,
            rtype,
            rclass,
            errors.join("; ")
        )))
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
}

impl TcpConnection {
    pub fn new<R, W>(read: R, write: W) -> Self
    where
        R: AsyncRead + Unpin + Sync + Send + 'static,
        W: AsyncWrite + Unpin + Sync + Send + 'static,
    {
        Self {
            read: BufReader::new(Box::new(read)),
            write: BufWriter::new(Box::new(write)),
            buffer: Vec::with_capacity(DEFAULT_MESSAGE_BUFFER),
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

        // Read the prefixed size of the response in big-endian (network)
        // order and then read exactly that many bytes into our buffer.
        let sz = self.read.read_u16().await?;
        self.buffer.clear();
        self.buffer.resize(usize::from(sz), 0);
        self.read.read_exact(&mut self.buffer).await?;

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
    packet_size: usize,
}

impl UdpConnection {
    pub fn new<R, W>(read: R, write: W) -> Self
    where
        R: AsyncRead + Unpin + Sync + Send + 'static,
        W: AsyncWrite + Unpin + Sync + Send + 'static,
    {
        Self {
            read: Box::new(read),
            write: Box::new(write),
            buffer: Vec::with_capacity(DEFAULT_MESSAGE_BUFFER),
            packet_size: DEFAULT_MESSAGE_BUFFER,
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
        self.buffer.resize(self.packet_size, 0);

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
/// traits. This exists to enable easier testing of `UdpConnection` by allowing alternate
/// implementations of those traits to be used.
pub(crate) struct SocketAdapter(UdpSocket);

impl SocketAdapter {
    pub(crate) fn new(sock: UdpSocket) -> Self {
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

#[async_trait]
impl ClientFactory<SocketAddr, UdpConnection> for UdpConnectionFactory {
    async fn make(&self, address: &SocketAddr) -> Result<UdpConnection, MtopError> {
        let local = if address.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" };
        let sock = UdpSocket::bind(local).await?;
        sock.connect(address).await?;

        let adapter = SocketAdapter::new(sock);
        let (read, write) = tokio::io::split(adapter);
        Ok(UdpConnection::new(read, write))
    }
}

/// Implementation of `ClientFactory` for creating concrete `TcpConnection` instances
/// that use a TCP socket.
#[derive(Debug, Clone, Default)]
pub struct TcpConnectionFactory;

#[async_trait]
impl ClientFactory<SocketAddr, TcpConnection> for TcpConnectionFactory {
    async fn make(&self, address: &SocketAddr) -> Result<TcpConnection, MtopError> {
        let (read, write) = tcp_connect(address).await?;
        Ok(TcpConnection::new(read, write))
    }
}

#[cfg(test)]
mod test {
    use super::{DefaultDnsClient, DnsClient, DnsClientConfig, TcpConnection, UdpConnection};
    use crate::core::ErrorKind;
    use crate::dns::core::{RecordClass, RecordType};
    use crate::dns::message::{Flags, Message, MessageId, Question, Record, ResponseCode};
    use crate::dns::name::Name;
    use crate::dns::rdata::{RecordData, RecordDataA};
    use crate::dns::test::{TestTcpClientFactory, TestTcpSocket, TestUdpClientFactory, TestUdpSocket};
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::str::FromStr;

    fn new_request(id: MessageId) -> Message {
        let flags = Flags::default().set_query().set_recursion_desired();
        let question = Question::new(Name::from_str("example.com.").unwrap(), RecordType::A);
        Message::new(id, flags).add_question(question)
    }

    fn new_empty_response(id: MessageId) -> Message {
        let flags = Flags::default()
            .set_response()
            .set_recursion_desired()
            .set_recursion_available();
        let question = Question::new(Name::from_str("example.com.").unwrap(), RecordType::A);
        Message::new(id, flags).add_question(question)
    }

    fn new_response(id: MessageId) -> Message {
        let response = new_empty_response(id);
        let answer = Record::new(
            Name::from_str("example.com.").unwrap(),
            RecordType::A,
            RecordClass::INET,
            300,
            RecordData::A(RecordDataA::new(Ipv4Addr::new(127, 0, 0, 1))),
        );

        response.add_answer(answer)
    }

    #[tokio::test]
    async fn test_tcp_client_eof_reading_length() {
        let write = Vec::new();
        let read = Cursor::new(Vec::new());

        let id = MessageId::from(123);
        let request = new_request(id);

        let mut client = TcpConnection::new(read, write);

        let res = client.exchange(&request).await;
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::IO, err.kind());
    }

    #[tokio::test]
    async fn test_tcp_client_eof_reading_message() {
        let write = Vec::new();
        let read = Cursor::new(vec![
            0, 200, // message length
        ]);

        let id = MessageId::from(123);
        let request = new_request(id);

        let mut client = TcpConnection::new(read, write);

        let res = client.exchange(&request).await;
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::IO, err.kind());
    }

    #[tokio::test]
    async fn test_tcp_client_id_mismatch() {
        let response_id = MessageId::from(456);
        let response = new_response(response_id);

        let request_id = MessageId::from(123);
        let request = new_request(request_id);

        let sock = TestTcpSocket::new(vec![response]);
        let (read, write) = tokio::io::split(sock);
        let mut client = TcpConnection::new(read, write);

        let result = client.exchange(&request).await;
        let err = result.unwrap_err();
        assert_eq!(ErrorKind::Runtime, err.kind());
    }

    #[tokio::test]
    async fn test_tcp_client_single_message() {
        let id = MessageId::from(123);
        let response = new_response(id);
        let request = new_request(id);

        let sock = TestTcpSocket::new(vec![response.clone()]);
        let (read, write) = tokio::io::split(sock);
        let mut client = TcpConnection::new(read, write);

        let result = client.exchange(&request).await.unwrap();
        assert_eq!(response, result);
    }

    #[tokio::test]
    async fn test_tcp_client_multiple_message() {
        let id1 = MessageId::from(123);
        let response1 = new_response(id1);
        let request1 = new_request(id1);
        let id2 = MessageId::from(456);
        let response2 = new_response(id2);
        let request2 = new_request(id2);

        let sock = TestTcpSocket::new(vec![response2.clone(), response1.clone()]);
        let (read, write) = tokio::io::split(sock);
        let mut client = TcpConnection::new(read, write);

        let result1 = client.exchange(&request1).await.unwrap();
        assert_eq!(response1, result1);

        let result2 = client.exchange(&request2).await.unwrap();
        assert_eq!(response2, result2);
    }

    #[tokio::test]
    async fn test_udp_client_success() {
        let id = MessageId::from(123);
        let response = new_response(id);
        let request = new_request(id);

        let sock = TestUdpSocket::new(vec![response.clone()]);
        let (read, write) = tokio::io::split(sock);
        let mut client = UdpConnection::new(read, write);

        let result = client.exchange(&request).await.unwrap();
        assert_eq!(response, result);
    }

    #[tokio::test]
    async fn test_udp_client_one_id_mismatch() {
        let id1 = MessageId::from(456);
        let response1 = new_response(id1);
        let id2 = MessageId::from(123);
        let response2 = new_response(id2);

        // Note that the request has the ID of the second response because
        // we are testing the that first response is discarded due to the ID
        // not matching.
        let request = new_request(id2);

        let sock = TestUdpSocket::new(vec![response2.clone(), response1.clone()]);
        let (read, write) = tokio::io::split(sock);
        let mut client = UdpConnection::new(read, write);

        let result = client.exchange(&request).await.unwrap();
        assert_eq!(response2, result);
    }

    #[tokio::test]
    async fn test_default_dns_client_resolve_name_error() {
        let id = MessageId::from(123);
        let name = Name::from_str("example.com.").unwrap();
        let server = "127.0.0.1:53".parse().unwrap();

        let udp_response = new_empty_response(id);
        let flags = udp_response.flags().set_response_code(ResponseCode::NameError);
        let udp_response = udp_response.set_flags(flags);

        let mut udp_mapping: HashMap<SocketAddr, Vec<Message>> = HashMap::new();
        udp_mapping.entry(server).or_default().push(udp_response);
        let udp_factory = TestUdpClientFactory::new(udp_mapping);
        let tcp_factory = TestTcpClientFactory::new(HashMap::new());

        let cfg = DnsClientConfig::default();
        let client = DefaultDnsClient::new(cfg, udp_factory, tcp_factory);
        let result = client.resolve(id, name, RecordType::A, RecordClass::INET).await.unwrap();

        assert_eq!(ResponseCode::NameError, result.flags().get_response_code());
        assert!(result.answers().is_empty());
    }

    #[tokio::test]
    async fn test_default_dns_client_resolve_success() {
        let id = MessageId::from(123);
        let name = Name::from_str("example.com.").unwrap();
        let server = "127.0.0.1:53".parse().unwrap();

        let udp_response = new_response(id);
        let mut udp_mapping: HashMap<SocketAddr, Vec<Message>> = HashMap::new();
        udp_mapping.entry(server).or_default().push(udp_response.clone());
        let udp_factory = TestUdpClientFactory::new(udp_mapping);
        let tcp_factory = TestTcpClientFactory::new(HashMap::new());

        let cfg = DnsClientConfig::default();
        let client = DefaultDnsClient::new(cfg, udp_factory, tcp_factory);
        let result = client.resolve(id, name, RecordType::A, RecordClass::INET).await.unwrap();

        assert_eq!(udp_response, result);
    }

    #[tokio::test]
    async fn test_default_dns_client_resolve_one_error() {
        let id = MessageId::from(123);
        let name = Name::from_str("example.com.").unwrap();
        let server = "127.0.0.1:53".parse().unwrap();

        let udp_response1 = new_empty_response(id);
        let flags = udp_response1.flags().set_response_code(ResponseCode::ServerFailure);
        let udp_response1 = udp_response1.set_flags(flags);
        let udp_response2 = new_response(id);

        let mut udp_mapping: HashMap<SocketAddr, Vec<Message>> = HashMap::new();
        let entry = udp_mapping.entry(server).or_default();
        entry.push(udp_response2.clone());
        entry.push(udp_response1);

        let udp_factory = TestUdpClientFactory::new(udp_mapping);
        let tcp_factory = TestTcpClientFactory::new(HashMap::new());

        let cfg = DnsClientConfig::default();
        let client = DefaultDnsClient::new(cfg, udp_factory, tcp_factory);
        let result = client.resolve(id, name, RecordType::A, RecordClass::INET).await.unwrap();

        assert_eq!(udp_response2, result);
    }

    #[tokio::test]
    async fn test_default_dns_client_resolve_all_errors() {
        let id = MessageId::from(123);
        let name = Name::from_str("example.com.").unwrap();
        let server = "127.0.0.1:53".parse().unwrap();

        let udp_response1 = new_empty_response(id);
        let flags = udp_response1.flags().set_response_code(ResponseCode::ServerFailure);
        let udp_response1 = udp_response1.set_flags(flags);

        let udp_response2 = new_empty_response(id);
        let flags = udp_response2.flags().set_response_code(ResponseCode::ServerFailure);
        let udp_response2 = udp_response2.set_flags(flags);

        let mut udp_mapping: HashMap<SocketAddr, Vec<Message>> = HashMap::new();
        let entry = udp_mapping.entry(server).or_default();
        entry.push(udp_response2.clone());
        entry.push(udp_response1);

        let udp_factory = TestUdpClientFactory::new(udp_mapping);
        let tcp_factory = TestTcpClientFactory::new(HashMap::new());

        let cfg = DnsClientConfig::default();
        let client = DefaultDnsClient::new(cfg, udp_factory, tcp_factory);
        let err = client.resolve(id, name, RecordType::A, RecordClass::INET).await.unwrap_err();

        assert_eq!(ErrorKind::Runtime, err.kind());
    }

    #[tokio::test]
    async fn test_default_dns_client_resolve_one_bad_server() {
        let id = MessageId::from(123);
        let name = Name::from_str("example.com.").unwrap();
        let server1 = "127.0.0.1:53".parse().unwrap();
        let server2 = "127.0.0.2:53".parse().unwrap();

        let udp_response1 = new_empty_response(id);
        let flags = udp_response1.flags().set_response_code(ResponseCode::ServerFailure);
        let udp_response1 = udp_response1.set_flags(flags);
        let udp_response2 = new_response(id);

        let mut udp_mapping: HashMap<SocketAddr, Vec<Message>> = HashMap::new();
        udp_mapping.entry(server1).or_default().push(udp_response1);
        udp_mapping.entry(server2).or_default().push(udp_response2.clone());

        let udp_factory = TestUdpClientFactory::new(udp_mapping);
        let tcp_factory = TestTcpClientFactory::new(HashMap::new());

        let cfg = DnsClientConfig {
            nameservers: vec![server1, server2],
            ..Default::default()
        };
        let client = DefaultDnsClient::new(cfg, udp_factory, tcp_factory);
        let result = client.resolve(id, name, RecordType::A, RecordClass::INET).await.unwrap();

        assert_eq!(udp_response2, result);
    }

    #[tokio::test]
    async fn test_default_dns_client_resolve_udp_truncation() {
        let id = MessageId::from(123);
        let name = Name::from_str("example.com.").unwrap();
        let server = "127.0.0.1:53".parse().unwrap();

        let udp_response = new_empty_response(id);
        let flags = udp_response.flags().set_truncated();
        let udp_response = udp_response.set_flags(flags);
        let tcp_response = new_response(id);

        let mut udp_mapping: HashMap<SocketAddr, Vec<Message>> = HashMap::new();
        udp_mapping.entry(server).or_default().push(udp_response);

        let mut tcp_mapping: HashMap<SocketAddr, Vec<Message>> = HashMap::new();
        tcp_mapping.entry(server).or_default().push(tcp_response.clone());

        let udp_factory = TestUdpClientFactory::new(udp_mapping);
        let tcp_factory = TestTcpClientFactory::new(tcp_mapping);

        let cfg = DnsClientConfig::default();
        let client = DefaultDnsClient::new(cfg, udp_factory, tcp_factory);
        let result = client.resolve(id, name, RecordType::A, RecordClass::INET).await.unwrap();

        assert_eq!(tcp_response, result);
    }
}
