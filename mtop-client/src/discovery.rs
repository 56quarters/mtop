use crate::core::MtopError;
use crate::dns::{DnsClient, Message, MessageId, Name, RecordClass, RecordData, RecordType};
use rustls_pki_types::ServerName;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

const DNS_A_PREFIX: &str = "dns+";
const DNS_SRV_PREFIX: &str = "dnssrv+";
const UNIX_SOCKET_PREFIX: &str = "/";

/// Unique ID and address for a server in a Memcached cluster for indexing responses
/// or errors and establishing connections.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum ServerID {
    Name(String),
    Socket(SocketAddr),
    Path(PathBuf),
}

impl ServerID {
    fn from_host_port<S>(host: S, port: u16) -> Self
    where
        S: AsRef<str>,
    {
        let host = host.as_ref();
        if let Ok(ip) = host.parse::<IpAddr>() {
            Self::Socket(SocketAddr::new(ip, port))
        } else {
            Self::Name(format!("{}:{}", host, port))
        }
    }
}

impl From<SocketAddr> for ServerID {
    fn from(value: SocketAddr) -> Self {
        Self::Socket(value)
    }
}

impl From<(&str, u16)> for ServerID {
    fn from(value: (&str, u16)) -> Self {
        Self::from_host_port(value.0, value.1)
    }
}

impl From<(String, u16)> for ServerID {
    fn from(value: (String, u16)) -> Self {
        Self::from_host_port(value.0, value.1)
    }
}

impl From<PathBuf> for ServerID {
    fn from(value: PathBuf) -> Self {
        Self::Path(value)
    }
}

impl fmt::Display for ServerID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerID::Name(n) => n.fmt(f),
            ServerID::Socket(s) => s.fmt(f),
            ServerID::Path(p) => fmt::Debug::fmt(p, f),
        }
    }
}

/// An individual server that is part of a Memcached cluster.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Server {
    id: ServerID,
    name: Option<ServerName<'static>>,
}

impl Server {
    pub fn new(id: ServerID, name: ServerName<'static>) -> Self {
        Self { id, name: Some(name) }
    }

    pub fn without_name(id: ServerID) -> Self {
        Self { id, name: None }
    }

    pub fn id(&self) -> &ServerID {
        &self.id
    }

    pub fn server_name(&self) -> &Option<ServerName<'static>> {
        &self.name
    }
}

impl PartialOrd for Server {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Server {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl fmt::Display for Server {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.id.fmt(f)
    }
}

/// Service discovery implementation for finding Memcached servers using DNS.
///
/// Different types of DNS records and different behaviors are used based on the
/// presence of specific prefixes for hostnames. See `resolve_by_proto` for details.
pub struct Discovery {
    client: Box<dyn DnsClient + Send + Sync>,
}

impl Discovery {
    pub fn new<C>(client: C) -> Self
    where
        C: DnsClient + Send + Sync + 'static,
    {
        Self {
            client: Box::new(client),
        }
    }

    /// Resolve a hostname to one or multiple Memcached servers based on DNS records
    /// and/or the presence of certain prefixes on the hostnames.
    ///
    /// * `dns+` will resolve a hostname into multiple A and AAAA records and use the
    ///   IP addresses from the records as Memcached servers.
    /// * `dnssrv+` will resolve a hostname into multiple SRV records and use the
    ///   unresolved targets from the SRV records as Memcached servers. Resolution of
    ///   the targets to IP addresses will happen at connection time using the system
    ///   resolver.
    /// * `/` will resolve a hostname into a UNIX socket path and will use this path
    ///   as a local Memcached server on a UNIX socket.
    /// * No prefix with an IPv4 or IPv6 address will use the IP address as a Memcached
    ///   server.
    /// * No prefix with a non-IP address will use the host as a Memcached server.
    ///   Resolution of the host to an IP address will happen at connection time using the
    ///   system resolver.
    pub async fn resolve_by_proto(&self, name: &str) -> Result<Vec<Server>, MtopError> {
        if name.starts_with(DNS_A_PREFIX) {
            Ok(self.resolve_a_aaaa(name.trim_start_matches(DNS_A_PREFIX)).await?)
        } else if name.starts_with(DNS_SRV_PREFIX) {
            Ok(self.resolve_srv(name.trim_start_matches(DNS_SRV_PREFIX)).await?)
        } else if name.starts_with(UNIX_SOCKET_PREFIX) {
            Ok(Self::resolve_unix_addr(name))
        } else if let Ok(addr) = name.parse::<SocketAddr>() {
            Ok(Self::resolve_socket_addr(name, addr)?)
        } else {
            Ok(Self::resolve_bare_host(name)?)
        }
    }

    async fn resolve_srv(&self, name: &str) -> Result<Vec<Server>, MtopError> {
        let (host, port) = Self::host_and_port(name)?;
        let server_name = Self::server_name(host)?;
        let name = host.parse()?;
        let id = MessageId::random();

        let res = self.client.resolve(id, name, RecordType::SRV, RecordClass::INET).await?;
        Ok(Self::servers_from_answers(port, &server_name, &res))
    }

    async fn resolve_a_aaaa(&self, name: &str) -> Result<Vec<Server>, MtopError> {
        let (host, port) = Self::host_and_port(name)?;
        let server_name = Self::server_name(host)?;
        let name: Name = host.parse()?;
        let id = MessageId::random();

        let res = self.client.resolve(id, name.clone(), RecordType::A, RecordClass::INET).await?;
        let mut out = Self::servers_from_answers(port, &server_name, &res);

        let res = self.client.resolve(id, name, RecordType::AAAA, RecordClass::INET).await?;
        out.extend(Self::servers_from_answers(port, &server_name, &res));

        Ok(out)
    }

    fn resolve_unix_addr(name: &str) -> Vec<Server> {
        let path = PathBuf::from(name);
        vec![Server::without_name(ServerID::from(path))]
    }

    fn resolve_socket_addr(name: &str, addr: SocketAddr) -> Result<Vec<Server>, MtopError> {
        let (host, _port) = Self::host_and_port(name)?;
        let server_name = Self::server_name(host)?;
        Ok(vec![Server::new(ServerID::from(addr), server_name)])
    }

    fn resolve_bare_host(name: &str) -> Result<Vec<Server>, MtopError> {
        let (host, port) = Self::host_and_port(name)?;
        let server_name = Self::server_name(host)?;
        Ok(vec![Server::new(ServerID::from((host, port)), server_name)])
    }

    fn servers_from_answers(port: u16, server_name: &ServerName<'static>, message: &Message) -> Vec<Server> {
        let mut servers = HashSet::new();

        for answer in message.answers() {
            let id = match answer.rdata() {
                RecordData::A(data) => {
                    let addr = SocketAddr::new(IpAddr::V4(data.addr()), port);
                    ServerID::from(addr)
                }
                RecordData::AAAA(data) => {
                    let addr = SocketAddr::new(IpAddr::V6(data.addr()), port);
                    ServerID::from(addr)
                }
                RecordData::SRV(data) => {
                    let target = data.target().to_string();

                    ServerID::from((&target as &str, port))
                }
                _ => {
                    tracing::warn!(message = "unexpected record data for answer", answer = ?answer);
                    continue;
                }
            };

            // Insert server into a HashSet to deduplicate them. We can potentially end up with
            // duplicates when a SRV query returns multiple answers per hostname (such as when
            // each host has more than a single port). Because we ignore the port number from the
            // SRV answer we need to deduplicate here.
            servers.insert(Server::new(id, server_name.to_owned()));
        }

        servers.into_iter().collect()
    }

    fn host_and_port(name: &str) -> Result<(&str, u16), MtopError> {
        name.rsplit_once(':')
            .ok_or_else(|| {
                MtopError::configuration(format!(
                    "invalid server name '{}', must be of the form 'host:port'",
                    name
                ))
            })
            // IPv6 addresses use brackets around them to disambiguate them from a port number.
            // Since we're parsing the host and port, strip the brackets because they aren't
            // needed or valid to include in a TLS ServerName.
            .map(|(host, port)| (host.trim_start_matches('[').trim_end_matches(']'), port))
            .and_then(|(host, port)| {
                port.parse().map(|p| (host, p)).map_err(|e| {
                    MtopError::configuration_cause(format!("unable to parse port number from '{}'", name), e)
                })
            })
    }

    fn server_name(host: &str) -> Result<ServerName<'static>, MtopError> {
        ServerName::try_from(host)
            .map(|s| s.to_owned())
            .map_err(|e| MtopError::configuration_cause(format!("invalid server name '{}'", host), e))
    }
}

impl fmt::Debug for Discovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Discovery").field("client", &"...").finish()
    }
}

#[cfg(test)]
mod test {
    use super::{Discovery, ServerID};
    use crate::core::MtopError;
    use crate::dns::{
        DnsClient, Flags, Message, MessageId, Name, Question, Record, RecordClass, RecordData, RecordDataA,
        RecordDataAAAA, RecordDataSRV, RecordType,
    };
    use async_trait::async_trait;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
    use std::str::FromStr;
    use tokio::sync::Mutex;

    #[test]
    fn test_server_id_from_ipv4_addr() {
        let addr = SocketAddr::from((Ipv4Addr::new(127, 1, 1, 1), 11211));
        let id = ServerID::from(addr);
        assert_eq!("127.1.1.1:11211", id.to_string());
    }

    #[test]
    fn test_server_id_from_ipv6_addr() {
        let addr = SocketAddr::from((Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), 11211));
        let id = ServerID::from(addr);
        assert_eq!("[::1]:11211", id.to_string());
    }

    #[test]
    fn test_server_id_from_ipv4_pair() {
        let pair = ("10.1.1.22", 11212);
        let id = ServerID::from(pair);
        assert_eq!("10.1.1.22:11212", id.to_string());
    }

    #[test]
    fn test_server_id_from_ipv6_pair() {
        let pair = ("::1", 11212);
        let id = ServerID::from(pair);
        assert_eq!("[::1]:11212", id.to_string());
    }

    #[test]
    fn test_server_id_from_host_pair() {
        let pair = ("cache.example.com", 11211);
        let id = ServerID::from(pair);
        assert_eq!("cache.example.com:11211", id.to_string());
    }

    struct MockDnsClient {
        responses: Mutex<Vec<Message>>,
    }

    impl MockDnsClient {
        fn new(responses: Vec<Message>) -> Self {
            Self {
                responses: Mutex::new(responses),
            }
        }
    }

    #[async_trait]
    impl DnsClient for MockDnsClient {
        async fn resolve(
            &self,
            _id: MessageId,
            _name: Name,
            _rtype: RecordType,
            _rclass: RecordClass,
        ) -> Result<Message, MtopError> {
            let mut responses = self.responses.lock().await;
            let res = responses.pop().unwrap();
            Ok(res)
        }
    }

    fn response_with_answers(rtype: RecordType, records: Vec<Record>) -> Message {
        let flags = Flags::default().set_recursion_desired().set_recursion_available();
        let mut message = Message::new(MessageId::random(), flags)
            .add_question(Question::new(Name::from_str("example.com.").unwrap(), rtype));

        for r in records {
            message = message.add_answer(r);
        }

        message
    }

    #[tokio::test]
    async fn test_dns_client_resolve_a_aaaa() {
        let response_a = response_with_answers(
            RecordType::A,
            vec![Record::new(
                Name::from_str("example.com.").unwrap(),
                RecordType::A,
                RecordClass::INET,
                300,
                RecordData::A(RecordDataA::new(Ipv4Addr::new(10, 1, 1, 1))),
            )],
        );

        let response_aaaa = response_with_answers(
            RecordType::AAAA,
            vec![Record::new(
                Name::from_str("example.com.").unwrap(),
                RecordType::AAAA,
                RecordClass::INET,
                300,
                RecordData::AAAA(RecordDataAAAA::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1))),
            )],
        );

        let client = MockDnsClient::new(vec![response_a, response_aaaa]);
        let discovery = Discovery::new(client);
        let servers = discovery.resolve_by_proto("dns+example.com:11211").await.unwrap();

        let ids = servers.iter().map(|s| s.id().clone()).collect::<Vec<_>>();
        let id_a = ServerID::from("10.1.1.1:11211".parse::<SocketAddr>().unwrap());
        let id_aaaa = ServerID::from("[::1]:11211".parse::<SocketAddr>().unwrap());

        assert!(ids.contains(&id_a), "expected {:?} to contain {:?}", ids, id_a);
        assert!(ids.contains(&id_aaaa), "expected {:?} to contain {:?}", ids, id_aaaa);
    }

    #[tokio::test]
    async fn test_dns_client_resolve_srv() {
        let response = response_with_answers(
            RecordType::SRV,
            vec![
                Record::new(
                    Name::from_str("_cache.example.com.").unwrap(),
                    RecordType::SRV,
                    RecordClass::INET,
                    300,
                    RecordData::SRV(RecordDataSRV::new(
                        100,
                        10,
                        11211,
                        Name::from_str("cache01.example.com.").unwrap(),
                    )),
                ),
                Record::new(
                    Name::from_str("_cache.example.com.").unwrap(),
                    RecordType::SRV,
                    RecordClass::INET,
                    300,
                    RecordData::SRV(RecordDataSRV::new(
                        100,
                        10,
                        11211,
                        Name::from_str("cache02.example.com.").unwrap(),
                    )),
                ),
            ],
        );

        let client = MockDnsClient::new(vec![response]);
        let discovery = Discovery::new(client);
        let servers = discovery.resolve_by_proto("dnssrv+_cache.example.com:11211").await.unwrap();

        let ids = servers.iter().map(|s| s.id().clone()).collect::<Vec<_>>();
        let id1 = ServerID::from(("cache01.example.com.", 11211));
        let id2 = ServerID::from(("cache02.example.com.", 11211));

        assert!(ids.contains(&id1), "expected {:?} to contain {:?}", ids, id1);
        assert!(ids.contains(&id2), "expected {:?} to contain {:?}", ids, id2);
    }

    #[tokio::test]
    async fn test_dns_client_resolve_srv_dupes() {
        let response = response_with_answers(
            RecordType::SRV,
            vec![
                Record::new(
                    Name::from_str("_cache.example.com.").unwrap(),
                    RecordType::SRV,
                    RecordClass::INET,
                    300,
                    RecordData::SRV(RecordDataSRV::new(
                        100,
                        10,
                        11211,
                        Name::from_str("cache01.example.com.").unwrap(),
                    )),
                ),
                Record::new(
                    Name::from_str("_cache.example.com.").unwrap(),
                    RecordType::SRV,
                    RecordClass::INET,
                    300,
                    RecordData::SRV(RecordDataSRV::new(
                        100,
                        10,
                        9105,
                        Name::from_str("cache01.example.com.").unwrap(),
                    )),
                ),
            ],
        );

        let client = MockDnsClient::new(vec![response]);
        let discovery = Discovery::new(client);
        let servers = discovery.resolve_by_proto("dnssrv+_cache.example.com:11211").await.unwrap();

        let ids = servers.iter().map(|s| s.id().clone()).collect::<Vec<_>>();
        let id = ServerID::from(("cache01.example.com.", 11211));

        assert_eq!(ids, vec![id]);
    }

    #[tokio::test]
    async fn test_dns_client_resolve_socket_addr() {
        let name = "127.0.0.2:11211";
        let sock: SocketAddr = "127.0.0.2:11211".parse().unwrap();

        let client = MockDnsClient::new(vec![]);
        let discovery = Discovery::new(client);
        let servers = discovery.resolve_by_proto(name).await.unwrap();

        let ids = servers.iter().map(|s| s.id().clone()).collect::<Vec<_>>();
        let id = ServerID::from(sock);

        assert!(ids.contains(&id), "expected {:?} to contain {:?}", ids, id);
    }

    #[tokio::test]
    async fn test_dns_client_resolve_bare_host() {
        let name = "localhost:11211";

        let client = MockDnsClient::new(vec![]);
        let discovery = Discovery::new(client);
        let servers = discovery.resolve_by_proto(name).await.unwrap();

        let ids = servers.iter().map(|s| s.id().clone()).collect::<Vec<_>>();
        let id = ServerID::from(("localhost", 11211));

        assert!(ids.contains(&id), "expected {:?} to contain {:?}", ids, id);
    }
}
