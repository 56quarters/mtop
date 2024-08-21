use crate::core::MtopError;
use crate::dns::{DefaultDnsClient, DnsClient, Message, Name, RecordClass, RecordData, RecordType};
use rustls_pki_types::ServerName;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::net::{IpAddr, SocketAddr};

const DNS_A_PREFIX: &str = "dns+";
const DNS_SRV_PREFIX: &str = "dnssrv+";

/// Unique ID for a server in a Memcached cluster.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[repr(transparent)]
pub struct ServerID(String);

impl ServerID {
    fn from_host_port<S>(host: S, port: u16) -> Self
    where
        S: AsRef<str>,
    {
        let host = host.as_ref();
        if let Ok(ip) = host.parse::<IpAddr>() {
            Self(SocketAddr::from((ip, port)).to_string())
        } else {
            Self(format!("{}:{}", host, port))
        }
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

impl From<SocketAddr> for ServerID {
    fn from(value: SocketAddr) -> Self {
        Self(value.to_string())
    }
}

impl fmt::Display for ServerID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for ServerID {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// An individual server that is part of a Memcached cluster.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Server {
    id: ServerID,
    name: ServerName<'static>,
}

impl Server {
    pub fn new(id: ServerID, name: ServerName<'static>) -> Self {
        Self { id, name }
    }

    pub fn id(&self) -> &ServerID {
        &self.id
    }

    pub fn server_name(&self) -> &ServerName<'static> {
        &self.name
    }

    pub fn address(&self) -> &str {
        self.id.as_ref()
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
        write!(f, "{}", self.id)
    }
}

/// Service discovery implementation for finding Memcached servers using DNS.
///
/// Different types of DNS records and different behaviors are used based on the
/// presence of specific prefixes for hostnames. See `resolve_by_proto` for details.
#[derive(Debug)]
pub struct Discovery<C = DefaultDnsClient>
where
    C: DnsClient + Send + Sync + 'static,
{
    client: C,
}

impl<C> Discovery<C>
where
    C: DnsClient + Send + Sync + 'static,
{
    pub fn new(client: C) -> Self {
        Self { client }
    }

    /// Resolve a hostname to one or multiple Memcached servers based on DNS records
    /// and the presence of `proto+` prefixes on the hostnames.
    ///
    /// * `dns+` will resolve a hostname into multiple A and AAAA records and use the
    ///   IP addresses from the records as Memcached servers.
    /// * `dnssrv+` will resolve a hostname into multiple SRV records and use the
    ///   unresolved targets from the SRV records as Memcached servers. Resolution of
    ///   the targets to IP addresses will happen at connection time using the system
    ///   resolver.
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
        } else if let Ok(addr) = name.parse::<SocketAddr>() {
            Ok(Self::resolv_socket_addr(name, addr)?)
        } else {
            Ok(Self::resolv_bare_host(name)?)
        }
    }

    async fn resolve_srv(&self, name: &str) -> Result<Vec<Server>, MtopError> {
        let (host, port) = Self::host_and_port(name)?;
        let server_name = Self::server_name(host)?;
        let name = host.parse()?;

        let res = self.client.resolve(name, RecordType::SRV, RecordClass::INET).await?;
        Ok(Self::servers_from_answers(port, &server_name, &res))
    }

    async fn resolve_a_aaaa(&self, name: &str) -> Result<Vec<Server>, MtopError> {
        let (host, port) = Self::host_and_port(name)?;
        let server_name = Self::server_name(host)?;
        let name: Name = host.parse()?;

        let res = self.client.resolve(name.clone(), RecordType::A, RecordClass::INET).await?;
        let mut out = Self::servers_from_answers(port, &server_name, &res);

        let res = self.client.resolve(name, RecordType::AAAA, RecordClass::INET).await?;
        out.extend(Self::servers_from_answers(port, &server_name, &res));

        Ok(out)
    }

    fn resolv_socket_addr(name: &str, addr: SocketAddr) -> Result<Vec<Server>, MtopError> {
        let (host, _port) = Self::host_and_port(name)?;
        let server_name = Self::server_name(host)?;
        Ok(vec![Server::new(ServerID::from(addr), server_name)])
    }

    fn resolv_bare_host(name: &str) -> Result<Vec<Server>, MtopError> {
        let (host, port) = Self::host_and_port(name)?;
        let server_name = Self::server_name(host)?;
        Ok(vec![Server::new(ServerID::from((host, port)), server_name)])
    }

    fn servers_from_answers(port: u16, server_name: &ServerName, message: &Message) -> Vec<Server> {
        let mut ids = HashSet::new();

        for answer in message.answers() {
            let server_id = match answer.rdata() {
                RecordData::A(data) => ServerID::from(SocketAddr::new(IpAddr::V4(data.addr()), port)),
                RecordData::AAAA(data) => ServerID::from(SocketAddr::new(IpAddr::V6(data.addr()), port)),
                RecordData::SRV(data) => ServerID::from((data.target().to_string(), port)),
                _ => {
                    tracing::warn!(message = "unexpected record data for answer", answer = ?answer);
                    continue;
                }
            };

            // Insert IDs into a HashSet to deduplicate them. We can potentially end up with duplicates
            // when a SRV query returns multiple answers per hostname (such as when each host has more
            // than a single port). Because we ignore the port number from the SRV answer we need to
            // deduplicate here.
            ids.insert(server_id);
        }

        ids.into_iter().map(|id| Server::new(id, server_name.to_owned())).collect()
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

#[cfg(test)]
mod test {
    use super::{Discovery, Server, ServerID};
    use crate::core::MtopError;
    use crate::dns::{
        DnsClient, Flags, Message, MessageId, Name, Question, Record, RecordClass, RecordData, RecordDataA,
        RecordDataAAAA, RecordDataSRV, RecordType,
    };
    use rustls_pki_types::ServerName;
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

    #[test]
    fn test_server_resolved_id() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 11211));
        let id = ServerID::from(addr);
        let name = ServerName::try_from("cache.example.com").unwrap();
        let server = Server::new(id, name);
        assert_eq!("127.0.0.1:11211", server.id().to_string());
    }

    #[test]
    fn test_server_resolved_address() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 11211));
        let id = ServerID::from(addr);
        let name = ServerName::try_from("cache.example.com").unwrap();
        let server = Server::new(id, name);
        assert_eq!("127.0.0.1:11211", server.address());
    }

    #[test]
    fn test_server_unresolved_id() {
        let id = ServerID::from(("cache01.example.com", 11211));
        let name = ServerName::try_from("cache.example.com").unwrap();
        let server = Server::new(id, name);
        assert_eq!("cache01.example.com:11211", server.id().to_string());
    }

    #[test]
    fn test_server_unresolved_address() {
        let id = ServerID::from(("cache01.example.com", 11211));
        let name = ServerName::try_from("cache.example.com").unwrap();
        let server = Server::new(id, name);
        assert_eq!("cache01.example.com:11211", server.address());
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

    impl DnsClient for MockDnsClient {
        async fn resolve(&self, _name: Name, _rtype: RecordType, _rclass: RecordClass) -> Result<Message, MtopError> {
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

        let id_a = ServerID::from(("10.1.1.1", 11211));
        let id_aaaa = ServerID::from(("[::1]", 11211));

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
        let addr: SocketAddr = "127.0.0.2:11211".parse().unwrap();

        let client = MockDnsClient::new(vec![]);
        let discovery = Discovery::new(client);
        let servers = discovery.resolve_by_proto(name).await.unwrap();
        let ids = servers.iter().map(|s| s.id().clone()).collect::<Vec<_>>();

        let id = ServerID::from(addr);

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
