use mtop_client::{
    Discovery, MemcachedClient, MemcachedClientConfig, MtopError, RendezvousSelector, Selector, Server, ServerID,
    TcpClientFactory, Timeout, TlsConfig, TlsTcpClientFactory,
};
use std::time::Duration;
use tokio::runtime::Handle;
use tracing::{Instrument, Level};

pub async fn new_client(
    servers: &[Server],
    max_connections: u64,
    tls: Option<TlsConfig>,
) -> Result<MemcachedClient, MtopError> {
    let is_unix = servers.iter().any(|s| matches!(s.id(), ServerID::Path(_)));
    let selector = RendezvousSelector::new(servers.to_vec());
    let config = MemcachedClientConfig {
        pool_max_idle: max_connections,
        pool_name: if is_unix { "memcached-unix" } else { "memcached-tcp" }.to_string(),
    };

    if is_unix {
        new_client_unix(selector, config, tls).await
    } else {
        new_client_tcp(selector, config, tls).await
    }
}

#[cfg(unix)]
async fn new_client_unix<S>(
    selector: S,
    config: MemcachedClientConfig,
    tls: Option<TlsConfig>,
) -> Result<MemcachedClient, MtopError>
where
    S: Selector + Send + Sync + 'static,
{
    use mtop_client::UnixClientFactory;

    if let Some(_tls) = tls {
        Err(MtopError::configuration("TLS is not supported when using UNIX sockets"))
    } else {
        let factory = UnixClientFactory;
        Ok(MemcachedClient::new(config, Handle::current(), selector, factory))
    }
}

#[cfg(not(unix))]
async fn new_client_unix<S>(
    _selector: S,
    _config: MemcachedClientConfig,
    _tls: Option<TlsConfig>,
) -> Result<MemcachedClient, MtopError>
where
    S: Selector + Send + Sync + 'static,
{
    Err(MtopError::configuration(
        "UNIX sockets are not supported on non-UNIX platforms",
    ))
}

async fn new_client_tcp<S>(
    selector: S,
    config: MemcachedClientConfig,
    tls: Option<TlsConfig>,
) -> Result<MemcachedClient, MtopError>
where
    S: Selector + Send + Sync + 'static,
{
    if let Some(tls) = tls {
        let factory = TlsTcpClientFactory::new(tls).await?;
        Ok(MemcachedClient::new(config, Handle::current(), selector, factory))
    } else {
        let factory = TcpClientFactory;
        Ok(MemcachedClient::new(config, Handle::current(), selector, factory))
    }
}

/// Perform validation and resolve provided hostnames or UNIX sockets. For hostnames
/// DNS resolution is performed, expanding any "dns+" or "dnssrv+" prefixed hosts that
/// to multiple A, AAAA, or SRV records. Hosts must be all hostnames OR UNIX sockets.
/// If hostnames are mixed with UNIX sockets, an error is returned.
pub async fn resolve(hosts: &[String], discovery: &Discovery, timeout: Duration) -> Result<Vec<Server>, MtopError> {
    let mut out = Vec::with_capacity(hosts.len());
    for h in hosts {
        out.extend(resolve_single(h, discovery, timeout).await?);
    }

    // After resolving the provided hostnames, make sure that there aren't a
    // combination of UNIX sockets and TCP sockets since we use a single client
    // for the entire application and each client is UNIX _or_ TCP only.
    let mut unix: usize = 0;
    let mut tcp: usize = 0;
    for s in out.iter() {
        match s.id() {
            ServerID::Path(_) => {
                unix += 1;
            }
            _ => {
                tcp += 1;
            }
        }
    }

    if unix > 0 && tcp > 0 {
        return Err(MtopError::configuration(format!(
            "hostnames and UNIX sockets cannot be mixed. Got {} hostnames and {} UNIX sockets",
            tcp, unix
        )));
    }

    out.sort();
    Ok(out)
}

/// Resolve a hostname or UNIX socket. For a hostname DNS resolution is performed,
/// expanding any "dns+" or "dnssrv+" prefix to multiple A, AAAA, or SRV records.
pub async fn resolve_single(host: &str, discovery: &Discovery, timeout: Duration) -> Result<Vec<Server>, MtopError> {
    discovery
        .resolve_by_proto(host)
        .timeout(timeout, "discovery.resolve_by_proto")
        .instrument(tracing::span!(Level::INFO, "discovery.resolve_by_proto"))
        .await
}
