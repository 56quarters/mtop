use mtop_client::dns::{DefaultDnsClient, DnsClientConfig, ResolvConf};
use mtop_client::MtopError;
use std::fmt;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use tokio::fs::File;

/// Load configuration from the provided resolv.conf file and create a new DnsClient
/// based on it. If the resolv.conf file cannot be opened or is malformed, default
/// configuration values will be used. See `man 5 resolv.conf` for more information.
pub async fn new_client<P>(resolv: P, nameserver: Option<SocketAddr>, timeout: Option<Duration>) -> DefaultDnsClient
where
    P: AsRef<Path> + fmt::Debug,
{
    let mut client_config = load_config(&resolv)
        .await
        .map(|c| {
            let mut cfg = DnsClientConfig::default();
            if !c.nameservers.is_empty() {
                cfg.nameservers = c.nameservers;
            }
            if let Some(timeout) = c.options.timeout {
                cfg.timeout = timeout;
            }
            if let Some(attempts) = c.options.attempts {
                cfg.attempts = attempts;
            }
            if let Some(rotate) = c.options.rotate {
                cfg.rotate = rotate;
            }
            cfg
        })
        .unwrap_or_else(|e| {
            tracing::warn!(message = "unable to load resolv.conf", path = ?resolv, err = %e);
            DnsClientConfig::default()
        });

    if let Some(n) = nameserver {
        client_config.nameservers = vec![n];
    }

    if let Some(t) = timeout {
        client_config.timeout = t;
    }

    // Use default instances of the UDP and TCP connection factories, alternate
    // implementations are only useful for unit testing.
    DefaultDnsClient::new(client_config, Default::default(), Default::default())
}

async fn load_config<P>(resolv: P) -> Result<ResolvConf, MtopError>
where
    P: AsRef<Path> + fmt::Debug,
{
    let handle = File::open(&resolv)
        .await
        .map_err(|e| MtopError::configuration_cause(format!("unable to open {:?}", resolv), e))?;
    mtop_client::dns::config(handle).await
}
