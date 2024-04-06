use crate::core::MtopError;
use crate::dns::{DnsClient, RecordData};
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
    repr: ServerRepr,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum ServerRepr {
    Resolved(ServerID, ServerName<'static>, SocketAddr),
    Unresolved(ServerID, ServerName<'static>),
}

impl Server {
    pub fn from_id(id: ServerID, name: ServerName<'static>) -> Self {
        Self {
            repr: ServerRepr::Unresolved(id, name),
        }
    }

    pub fn from_addr(addr: SocketAddr, name: ServerName<'static>) -> Self {
        Self {
            repr: ServerRepr::Resolved(ServerID::from(addr), name, addr),
        }
    }

    pub fn id(&self) -> ServerID {
        match &self.repr {
            ServerRepr::Resolved(id, _, _) => id.clone(),
            ServerRepr::Unresolved(id, _) => id.clone(),
        }
    }

    pub fn server_name(&self) -> ServerName<'static> {
        match &self.repr {
            ServerRepr::Resolved(_, name, _) => name.clone(),
            ServerRepr::Unresolved(_, name) => name.clone(),
        }
    }

    pub fn address(&self) -> String {
        match &self.repr {
            ServerRepr::Resolved(_, _, addr) => addr.to_string(),
            ServerRepr::Unresolved(id, _) => id.to_string(),
        }
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
            Ok(self.resolve_a(name.trim_start_matches(DNS_A_PREFIX)).await?)
        } else if name.starts_with(DNS_SRV_PREFIX) {
            Ok(self.resolve_srv(name.trim_start_matches(DNS_SRV_PREFIX)).await?)
        } else {
            Ok(self.resolve_a(name).await?.pop().into_iter().collect())
        }
    }

    async fn resolve_srv(&self, name: &str) -> Result<Vec<Server>, MtopError> {
        let server_name = Self::server_name(name)?;
        let (host_name, port) = Self::host_and_port(name)?;
        let mut out = Vec::new();

        let res = self.client.resolve_srv(host_name).await?;
        for a in res.answers() {
            let target = if let RecordData::SRV(srv) = a.rdata() {
                srv.target().to_string()
            } else {
                tracing::warn!(message = "unexpected record data for answer", name = host_name, answer = ?a);
                continue;
            };
            let server_id = ServerID::from((target, port));
            let server = Server::from_id(server_id, server_name.clone());
            out.push(server);
        }

        Ok(out)
    }

    async fn resolve_a(&self, name: &str) -> Result<Vec<Server>, MtopError> {
        let server_name = Self::server_name(name)?;

        let mut out = Vec::new();
        for addr in tokio::net::lookup_host(name).await? {
            out.push(Server::from_addr(addr, server_name.clone()));
        }

        Ok(out)
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
            .map(|(hostname, port)| (hostname.trim_start_matches('[').trim_end_matches(']'), port))
            .and_then(|(hostname, port)| {
                port.parse().map(|p| (hostname, p)).map_err(|e| {
                    MtopError::configuration_cause(format!("unable to parse port number from '{}'", name), e)
                })
            })
    }

    fn server_name(name: &str) -> Result<ServerName<'static>, MtopError> {
        Self::host_and_port(name).and_then(|(host, _)| {
            ServerName::try_from(host)
                .map(|s| s.to_owned())
                .map_err(|e| MtopError::configuration_cause(format!("invalid server name '{}'", host), e))
        })
    }
}

#[cfg(test)]
mod test {
    use super::ServerID;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

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
}
