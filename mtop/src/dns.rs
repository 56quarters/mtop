use mtop_client::dns::{DnsClient, ResolvConf};
use mtop_client::MtopError;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use tokio::fs::File;

const DEFAULT_SERVER: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 53);

/// Load configuration from the provided resolv.conf file and crated a new DnsClient
/// based on it. If the resolv.conf file cannot be opened or is malformed, default
/// configuration values will be used. See `man 5 resolv.conf` for more information.
pub async fn new_client<P>(local: SocketAddr, resolv: P) -> DnsClient
where
    P: AsRef<Path> + fmt::Debug,
{
    let mut cfg = match load_config(&resolv).await {
        Ok(cfg) => cfg,
        Err(e) => {
            tracing::warn!(message = "unable to load resolv.conf", path = ?resolv, err = %e);
            ResolvConf::default()
        }
    };

    // Either the resolv.conf file doesn't list any nameservers or we had to
    // use Default::default() which also doesn't include any. Use localhost in
    // the hopes that it will work.
    if cfg.nameservers.is_empty() {
        cfg.nameservers.push(DEFAULT_SERVER);
    }

    DnsClient::new(local, cfg)
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
