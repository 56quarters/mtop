use clap::{Parser, ValueHint};
use mtop::queue::{BlockingStatsQueue, StatsQueue};
use mtop_client::{
    MemcachedClient, MemcachedPool, MtopError, PoolConfig, SelectorRendezvous, Server, TLSConfig, Timeout,
};
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::ToSocketAddrs;
use tokio::runtime::Handle;
use tokio::task;
use tracing::instrument::WithSubscriber;
use tracing::{Instrument, Level};

const DEFAULT_LOG_LEVEL: Level = Level::INFO;
// Update interval of more than a second to minimize the chance that stats returned by the
// memcached server have the exact same "time" value (which has one-second granularity).
const DEFAULT_STATS_INTERVAL: Duration = Duration::from_millis(1073);
const DEFAULT_TIMEOUT_SECS: u64 = 5;
const NUM_MEASUREMENTS: usize = 10;
const DNS_HOST_PREFIX: &str = "dns+";

/// mtop: top for memcached
#[derive(Debug, Parser)]
#[command(name = "mtop", version = clap::crate_version!())]
struct MtopConfig {
    /// Logging verbosity. Allowed values are 'trace', 'debug', 'info', 'warn', and 'error'
    /// (case insensitive).
    #[arg(long, default_value_t = DEFAULT_LOG_LEVEL)]
    log_level: Level,

    /// Timeout for connecting to Memcached and fetching statistics, in seconds.
    #[arg(long, default_value_t = DEFAULT_TIMEOUT_SECS)]
    timeout_secs: u64,

    /// File to log errors to since they cannot be logged to the console. If the path is not
    /// writable, mtop will not start.
    #[arg(long, default_value=default_log_file().into_os_string(), value_hint = ValueHint::FilePath)]
    log_file: PathBuf,

    /// Enable TLS connections to the Memcached server.
    #[arg(long)]
    tls_enabled: bool,

    /// Optional certificate authority to use for validating the server certificate instead of
    /// the default root certificates.
    #[arg(long, value_hint = ValueHint::FilePath)]
    tls_ca: Option<PathBuf>,

    /// Optional server name to use for validating the server certificate. If not set, the
    /// hostname of the server is used for checking that the certificate matches the server.
    #[arg(long)]
    tls_server_name: Option<String>,

    /// Optional client certificate to use to authenticate with the Memcached server. Note that
    /// this may or may not be required based on how the Memcached server is configured.
    #[arg(long, requires = "tls_key", value_hint = ValueHint::FilePath)]
    tls_cert: Option<PathBuf>,

    /// Optional client key to use to authenticate with the Memcached server. Note that this may
    /// or may not be required based on how the Memcached server is configured.
    #[arg(long, requires = "tls_cert", value_hint = ValueHint::FilePath)]
    tls_key: Option<PathBuf>,

    /// Memcached hosts to connect to in the form 'hostname:port'. Must be specified at least
    /// once and may be used multiple times (separated by spaces). Hostnames may be prefixed by
    /// the string 'dns+'. When prefixed, the hostname will be resolved to A or AAAA records and
    /// each IP address will be connected to at the provided port.
    #[arg(required = true, value_hint = ValueHint::Hostname)]
    hosts: Vec<String>,
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

    // Do DNS lookups on any "dns+" hostnames to expand them to multiple IPs based on A records.
    let hosts = match expand_hosts(&opts.hosts, timeout).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(message = "unable to resolve host names", hosts = ?opts.hosts, error = %e);
            return ExitCode::FAILURE;
        }
    };

    let client = match new_client(&opts, &hosts).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(message = "unable to initialize memcached client", hosts = ?hosts, err = %e);
            return ExitCode::FAILURE;
        }
    };

    let update_task = UpdateTask::new(client, measurements.clone(), timeout);
    if let Err(e) = update_task.connect().await {
        tracing::error!(message = "unable to connect to memcached servers", hosts = ?hosts, err = %e);
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
        let app = mtop::ui::Application::new(&hosts, blocking_measurements);

        // Run the terminal reset unconditionally but prefer to return an error from the
        // application, if available, for logging.
        mtop::ui::run(&mut term, app).and(mtop::ui::reset_terminal())
    })
    .await;

    match ui_res {
        Err(e) => {
            tracing::error!(message = "unable to run UI in dedicated thread", error = %e);
            ExitCode::FAILURE
        }
        Ok(Err(e)) => {
            tracing::error!(message = "error setting up terminal or running UI", error = %e);
            ExitCode::FAILURE
        }
        _ => ExitCode::SUCCESS,
    }
}

async fn expand_hosts(hosts: &[String], timeout: Duration) -> Result<Vec<String>, MtopError> {
    let mut out = Vec::with_capacity(hosts.len());

    for host in hosts {
        if host.starts_with(DNS_HOST_PREFIX) {
            let name = host.trim_start_matches(DNS_HOST_PREFIX);
            for addr in lookup_host(name)
                .timeout(timeout, "lookup_host")
                .instrument(tracing::span!(Level::INFO, "lookup_host"))
                .await?
            {
                out.push(addr.to_string());
            }
        } else {
            out.push(host.clone());
        }
    }

    out.sort();
    Ok(out)
}

async fn lookup_host<T>(host: T) -> Result<impl Iterator<Item = SocketAddr>, MtopError>
where
    T: ToSocketAddrs,
{
    // This function only exists to translate io::Error to MtopError so we can .timeout()
    Ok(tokio::net::lookup_host(host).await?)
}

async fn new_client(opts: &MtopConfig, hosts: &[String]) -> Result<MemcachedClient, MtopError> {
    let tls = TLSConfig {
        enabled: opts.tls_enabled,
        ca_path: opts.tls_ca.clone(),
        cert_path: opts.tls_cert.clone(),
        key_path: opts.tls_key.clone(),
        server_name: opts.tls_server_name.clone(),
    };

    MemcachedPool::new(
        Handle::current(),
        PoolConfig {
            tls,
            ..Default::default()
        },
    )
    .await
    .map(|pool| {
        let selector = SelectorRendezvous::new(hosts.iter().map(Server::new).collect());
        MemcachedClient::new(Handle::current(), selector, pool)
    })
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

        for (server, stats) in stats.values {
            let slabs = match slabs.values.remove(&server) {
                Some(v) => v,
                None => continue,
            };

            let items = match items.values.remove(&server) {
                Some(v) => v,
                None => continue,
            };

            self.queue
                .insert(server.name, stats, slabs, items)
                .instrument(tracing::span!(Level::INFO, "queue.insert"))
                .await;
        }

        for (server, e) in stats.errors {
            tracing::warn!(message = "error fetching stats", host = server.name, err = %e);
        }

        for (server, e) in slabs.errors {
            tracing::warn!(message = "error fetching slabs", host = server.name, err = %e);
        }

        for (server, e) in items.errors {
            tracing::warn!(message = "error fetching items", host = server.name, err = %e);
        }

        Ok(())
    }
}
