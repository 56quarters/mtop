use crate::core::MtopError;
use crate::dns::{DnsClient, Name, Record, RecordClass, RecordData, RecordType};
use std::cmp::Ordering;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use webpki::types::ServerName;

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

    pub fn id(&self) -> ServerID {
        self.id.clone()
    }

    pub fn server_name(&self) -> ServerName<'static> {
        self.name.clone()
    }

    pub fn address(&self) -> String {
        self.id.to_string()
    }
}

impl PartialOrd for Server {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Server {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id().cmp(&other.id())
    }
}

impl fmt::Display for Server {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id())
    }
}

#[derive(Debug, Clone)]
pub struct DiscoveryDefault {
    client: DnsClient,
}

impl DiscoveryDefault {
    pub fn new(client: DnsClient) -> Self {
        Self { client }
    }

    pub async fn resolve_by_proto(&self, name: &str) -> Result<Vec<Server>, MtopError> {
        if name.starts_with(DNS_A_PREFIX) {
            Ok(self.resolve_a_aaaa(name.trim_start_matches(DNS_A_PREFIX)).await?)
        } else if name.starts_with(DNS_SRV_PREFIX) {
            Ok(self.resolve_srv(name.trim_start_matches(DNS_SRV_PREFIX)).await?)
        } else if let Ok(addr) = name.parse::<SocketAddr>() {
            Ok(self.resolv_socket_addr(name, addr)?)
        } else {
            Ok(self.resolve_a_aaaa(name).await?.pop().into_iter().collect())
        }
    }

    async fn resolve_srv(&self, name: &str) -> Result<Vec<Server>, MtopError> {
        let (host, port) = Self::host_and_port(name)?;
        let server_name = Self::server_name(host)?;
        let name = host.parse()?;

        let res = self.client.resolve(name, RecordType::SRV, RecordClass::INET).await?;
        Ok(self.servers_from_answers(port, &server_name, res.answers()))
    }

    async fn resolve_a_aaaa(&self, name: &str) -> Result<Vec<Server>, MtopError> {
        let (host, port) = Self::host_and_port(name)?;
        let server_name = Self::server_name(host)?;
        let name: Name = host.parse()?;

        let res = self.client.resolve(name.clone(), RecordType::A, RecordClass::INET).await?;
        let mut out = self.servers_from_answers(port, &server_name, res.answers());

        let res = self.client.resolve(name, RecordType::AAAA, RecordClass::INET).await?;
        out.extend(self.servers_from_answers(port, &server_name, res.answers()));

        Ok(out)
    }

    fn resolv_socket_addr(&self, name: &str, addr: SocketAddr) -> Result<Vec<Server>, MtopError> {
        let (host, _port) = Self::host_and_port(name)?;
        let server_name = Self::server_name(host)?;
        Ok(vec![Server::new(ServerID::from(addr), server_name)])
    }

    fn servers_from_answers(&self, port: u16, server_name: &ServerName, answers: &[Record]) -> Vec<Server> {
        let mut out = Vec::new();

        for answer in answers {
            let server_id = match answer.rdata() {
                RecordData::A(data) => ServerID::from(SocketAddr::new(IpAddr::V4(data.addr()), port)),
                RecordData::AAAA(data) => ServerID::from(SocketAddr::new(IpAddr::V6(data.addr()), port)),
                RecordData::SRV(data) => ServerID::from((data.target().to_string(), port)),
                _ => {
                    tracing::warn!(message = "unexpected record data for answer", answer = ?answer);
                    continue;
                }
            };

            let server = Server::new(server_id, server_name.to_owned());
            out.push(server);
        }

        out
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
    use super::{Server, ServerID};
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
    use webpki::types::ServerName;

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
}
