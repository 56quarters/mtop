use mtop_client::dns::{DnsClient, DnsClientConfig, ResolvConf};
use mtop_client::MtopError;
use std::fmt;
use std::path::Path;
use tokio::fs::File;

/// Load configuration from the provided resolv.conf file and create a new DnsClient
/// based on it. If the resolv.conf file cannot be opened or is malformed, default
/// configuration values will be used. See `man 5 resolv.conf` for more information.
pub async fn new_client<P>(resolv: P) -> DnsClient
where
    P: AsRef<Path> + fmt::Debug,
{
    let client_config = load_config(&resolv)
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

    DnsClient::new(client_config)
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
