use clap::{Parser, ValueHint};
use mtop::queue::{BlockingStatsQueue, Host, StatsQueue};
use mtop::ui::{Theme, TAILWIND};
use mtop_client::{
    DiscoveryDefault, MemcachedClient, MemcachedPool, MemcachedPoolConfig, MtopError, SelectorRendezvous, Server,
    Timeout, TlsConfig,
};
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::task;
use tracing::instrument::WithSubscriber;
use tracing::{Instrument, Level};
use webpki::types::{InvalidDnsNameError, ServerName};

const DEFAULT_LOG_LEVEL: Level = Level::INFO;
const DEFAULT_THEME: Theme = TAILWIND;
// Update interval of more than a second to minimize the chance that stats returned by the
// memcached server have the exact same "time" value (which has one-second granularity).
const DEFAULT_STATS_INTERVAL: Duration = Duration::from_millis(1073);
const DEFAULT_TIMEOUT_SECS: u64 = 5;
const NUM_MEASUREMENTS: usize = 10;

/// mtop: top for memcached
#[derive(Debug, Parser)]
#[command(name = "mtop", version = clap::crate_version!())]
struct MtopConfig {
    /// Logging verbosity. Allowed values are 'trace', 'debug', 'info', 'warn', and 'error'
    /// (case-insensitive).
    #[arg(long, default_value_t = DEFAULT_LOG_LEVEL)]
    log_level: Level,

    /// Path to resolv.conf file for loading DNS configuration information. If this file
    /// can't be loaded, default values for DNS configuration are used instead.
    #[arg(long, default_value = default_resolv_conf().into_os_string(), value_hint = ValueHint::FilePath)]
    resolv_conf: PathBuf,

    /// Timeout for connecting to Memcached and fetching statistics, in seconds.
    #[arg(long, default_value_t = DEFAULT_TIMEOUT_SECS)]
    timeout_secs: u64,

    /// File to log errors to since they cannot be logged to the console. If the path is not
    /// writable, mtop will not start.
    #[arg(long, default_value = default_log_file().into_os_string(), value_hint = ValueHint::FilePath)]
    log_file: PathBuf,

    /// Color scheme to use for the UI. Available options are "ansi", "material", and "tailwind".
    #[arg(long, default_value_t = DEFAULT_THEME)]
    theme: Theme,

    /// Enable TLS connections to the Memcached server.
    #[arg(long)]
    tls_enabled: bool,

    /// Optional certificate authority to use for validating the server certificate instead of
    /// the default root certificates.
    #[arg(long, value_hint = ValueHint::FilePath)]
    tls_ca: Option<PathBuf>,

    /// Optional server name to use for validating the server certificate. If not set, the
    /// hostname of the server is used for checking that the certificate matches the server.
    #[arg(long, value_parser = parse_server_name)]
    tls_server_name: Option<ServerName<'static>>,

    /// Optional client certificate to use to authenticate with the Memcached server. Note that
    /// this may or may not be required based on how the Memcached server is configured.
    #[arg(long, requires = "tls_key", value_hint = ValueHint::FilePath)]
    tls_cert: Option<PathBuf>,

    /// Optional client key to use to authenticate with the Memcached server. Note that this may
    /// or may not be required based on how the Memcached server is configured.
    #[arg(long, requires = "tls_cert", value_hint = ValueHint::FilePath)]
    tls_key: Option<PathBuf>,

    /// Memcached hosts to connect to in the form 'hostname:port'. Must be specified at least
    /// once and may be used multiple times (separated by spaces).
    ///
    /// Hostnames may be prefixed by the strings 'dns+' or 'dnssrv+'. When prefixed with 'dns+',
    /// the hostname will be resolved to A or AAAA records and each IP address will be connected
    /// to at the provided port. When prefixed with 'dnssrv+', the hostname will be resolved to
    /// SRV records and the target of each record will be connected to at the provided port. Note
    /// that the port from the SRV record is ignored.
    #[arg(required = true, value_hint = ValueHint::Hostname)]
    hosts: Vec<String>,
}

fn default_resolv_conf() -> PathBuf {
    PathBuf::from("/etc/resolv.conf")
}

fn parse_server_name(s: &str) -> Result<ServerName<'static>, InvalidDnsNameError> {
    ServerName::try_from(s).map(|n| n.to_owned())
}

fn default_log_file() -> PathBuf {
    env::temp_dir().join("mtop").join("mtop.log")
}

#[tokio::main]
async fn main() -> ExitCode {
    let opts = MtopConfig::parse();

    let console_subscriber =
        mtop::tracing::console_subscriber(opts.log_level).expect("failed to setup console logging");
    tracing::subscriber::set_global_default(console_subscriber).expect("failed to initialize console logging");

    // Create a file subscriber for log messages generated while the UI is running
    // since we can't log to stdout or stderr.
    let file_subscriber = match mtop::tracing::file_subscriber(opts.log_level, &opts.log_file).map(Arc::new) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(message = "failed to initialize file logging", error = %e);
            return ExitCode::FAILURE;
        }
    };

    let timeout = Duration::from_secs(opts.timeout_secs);
    let measurements = Arc::new(StatsQueue::new(NUM_MEASUREMENTS));
    let dns_client = mtop::dns::new_client(&opts.resolv_conf).await;
    let resolver = DiscoveryDefault::new(dns_client);

    let servers = match expand_hosts(&opts.hosts, &resolver, timeout).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(message = "unable to resolve host names", hosts = ?opts.hosts, error = %e);
            return ExitCode::FAILURE;
        }
    };

    if servers.is_empty() {
        tracing::error!(message = "resolving host names did not return any results", hosts = ?opts.hosts);
        return ExitCode::FAILURE;
    }

    let client = match new_client(&opts, &servers).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(message = "unable to initialize memcached client", hosts = fmt_servers(&servers), err = %e);
            return ExitCode::FAILURE;
        }
    };

    let update_task = UpdateTask::new(client, measurements.clone(), timeout);
    if let Err(e) = update_task.connect().await {
        tracing::error!(message = "unable to connect to memcached servers", hosts = fmt_servers(&servers), err = %e);
        return ExitCode::FAILURE;
    }

    task::spawn(
        async move {
            let mut interval = tokio::time::interval(DEFAULT_STATS_INTERVAL);
            loop {
                let _ = interval.tick().await;
                if let Err(e) = update_task
                    .update()
                    .instrument(tracing::span!(Level::INFO, "periodic.update"))
                    .await
                {
                    tracing::error!(message = "unable to update server metrics", err = %e);
                }
            }
        }
        .with_subscriber(file_subscriber.clone()),
    );

    let ui_res = task::spawn_blocking(move || {
        let mut term = mtop::ui::initialize_terminal()?;
        mtop::ui::install_panic_handler();

        let blocking_measurements = BlockingStatsQueue::new(measurements.clone(), Handle::current());
        let hosts: Vec<Host> = servers.iter().map(|s| Host::from(s.id())).collect();
        let app = mtop::ui::Application::new(&hosts, blocking_measurements, opts.theme);

        // Run the terminal reset unconditionally but prefer to return an error from the
        // application, if available, for logging.
        mtop::ui::run(&mut term, app).and(mtop::ui::reset_terminal())
    })
    .await;

    match ui_res {
        Err(e) => {
            tracing::error!(message = "unable to run UI in dedicated thread", err = %e);
            ExitCode::FAILURE
        }
        Ok(Err(e)) => {
            tracing::error!(message = "error setting up terminal or running UI", err = %e);
            ExitCode::FAILURE
        }
        _ => ExitCode::SUCCESS,
    }
}

/// Convert a list of `Server`s into something that can be displayed by tracing.
fn fmt_servers(servers: &[Server]) -> String {
    let ids: Vec<String> = servers.iter().map(|s| s.id().to_string()).collect();
    format!("[{}]", ids.join(", "))
}

/// Perform DNS resolution on provided hostnames, expanding any "dns+" prefixed hosts
/// that have multiple A or AAAA records.
async fn expand_hosts(
    hosts: &[String],
    resolver: &DiscoveryDefault,
    timeout: Duration,
) -> Result<Vec<Server>, MtopError> {
    let mut out = Vec::with_capacity(hosts.len());

    for host in hosts {
        out.extend(
            resolver
                .resolve_by_proto(host)
                .timeout(timeout, "resolver.resolve_by_proto")
                .instrument(tracing::span!(Level::INFO, "resolver.resolve_by_proto"))
                .await?,
        );
    }

    out.sort();
    Ok(out)
}

async fn new_client(opts: &MtopConfig, servers: &[Server]) -> Result<MemcachedClient, MtopError> {
    let tls_config = TlsConfig {
        enabled: opts.tls_enabled,
        ca_path: opts.tls_ca.clone(),
        cert_path: opts.tls_cert.clone(),
        key_path: opts.tls_key.clone(),
        server_name: opts.tls_server_name.clone(),
    };

    let pool_config = MemcachedPoolConfig {
        tls: tls_config,
        ..Default::default()
    };

    let selector = SelectorRendezvous::new(servers.to_vec());
    let pool = MemcachedPool::new(Handle::current(), pool_config).await?;
    Ok(MemcachedClient::new(Handle::current(), selector, pool))
}

#[derive(Debug)]
struct UpdateTask {
    client: MemcachedClient,
    queue: Arc<StatsQueue>,
    timeout: Duration,
}

impl UpdateTask {
    fn new(client: MemcachedClient, queue: Arc<StatsQueue>, timeout: Duration) -> Self {
        UpdateTask { client, queue, timeout }
    }

    async fn connect(&self) -> Result<(), MtopError> {
        let pings = self
            .client
            .ping()
            .timeout(self.timeout, "client.ping")
            .instrument(tracing::span!(Level::INFO, "client.ping"))
            .await?;

        if let Some((_server, err)) = pings.errors.into_iter().next() {
            return Err(err);
        }

        Ok(())
    }

    async fn update(&self) -> Result<(), MtopError> {
        let stats = self
            .client
            .stats()
            .timeout(self.timeout, "client.stats")
            .instrument(tracing::span!(Level::INFO, "client.stats"))
            .await?;

        let mut slabs = self
            .client
            .slabs()
            .timeout(self.timeout, "client.slabs")
            .instrument(tracing::span!(Level::INFO, "client.slabs"))
            .await?;

        let mut items = self
            .client
            .items()
            .timeout(self.timeout, "client.items")
            .instrument(tracing::span!(Level::INFO, "client.items"))
            .await?;

        for (id, stats) in stats.values {
            let slabs = match slabs.values.remove(&id) {
                Some(v) => v,
                None => continue,
            };

            let items = match items.values.remove(&id) {
                Some(v) => v,
                None => continue,
            };

            self.queue
                .insert(Host::from(id), stats, slabs, items)
                .instrument(tracing::span!(Level::INFO, "queue.insert"))
                .await;
        }

        for (id, e) in stats.errors {
            tracing::warn!(message = "error fetching stats", server = %id, err = %e);
        }

        for (id, e) in slabs.errors {
            tracing::warn!(message = "error fetching slabs", server = %id, err = %e);
        }

        for (id, e) in items.errors {
            tracing::warn!(message = "error fetching items", server = %id, err = %e);
        }

        Ok(())
    }
}
