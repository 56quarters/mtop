use clap::{Parser, ValueHint};
use mtop::client::{MemcachedPool, MtopError, SlabItems, Slabs, Stats, TLSConfig};
use mtop::queue::{BlockingStatsQueue, StatsQueue};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::{env, error, process};
use tokio::net::lookup_host;
use tokio::runtime::Handle;
use tokio::task;
use tracing::instrument::WithSubscriber;
use tracing::{Instrument, Level};

const DEFAULT_LOG_LEVEL: Level = Level::INFO;
// Update interval of more than a second to minimize the chance that stats returned by the
// memcached server have the exact same "time" value (which has one-second granularity).
const DEFAULT_STATS_INTERVAL: Duration = Duration::from_millis(1073);
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

    /// File to log errors to since they cannot be logged to the console. If the path is not
    /// writable, mtop will not start.
    /// [default: $TEMP/mtop/mtop.log]
    #[arg(long, value_hint = ValueHint::FilePath)]
    log_file: Option<PathBuf>,

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
    #[arg(long, value_hint = ValueHint::FilePath)]
    tls_cert: Option<PathBuf>,

    /// Optional client key to use to authenticate with the Memcached server. Note that this may
    /// or may not be required based on how the Memcached server is configured.
    #[arg(long, value_hint = ValueHint::FilePath)]
    tls_key: Option<PathBuf>,

    /// Memcached hosts to connect to in the form 'hostname:port'. Must be specified at least
    /// once and may be used multiple times (separated by spaces).
    #[arg(required = true, value_hint = ValueHint::Hostname)]
    hosts: Vec<String>,
}

fn default_log_file() -> PathBuf {
    env::temp_dir().join("mtop").join("mtop.log")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error + Send + Sync>> {
    let opts = MtopConfig::parse();

    let console_subscriber = mtop::tracing::console_subscriber(opts.log_level)?;
    tracing::subscriber::set_global_default(console_subscriber).expect("failed to initialize console logging");

    // Create a file subscriber for log messages generated while the UI is running
    // since we can't log to stdout or stderr.
    let log_file = opts.log_file.unwrap_or_else(default_log_file);
    let file_subscriber = mtop::tracing::file_subscriber(opts.log_level, log_file)
        .map(Arc::new)
        .unwrap_or_else(|e| {
            tracing::error!(message = "failed to initialize file logging", error = %e);
            process::exit(1);
        });

    let measurements = Arc::new(StatsQueue::new(NUM_MEASUREMENTS));
    let pool = MemcachedPool::new(
        Handle::current(),
        TLSConfig {
            enabled: opts.tls_enabled,
            ca_path: opts.tls_ca,
            cert_path: opts.tls_cert,
            key_path: opts.tls_key,
            server_name: opts.tls_server_name,
        },
    )
    .await
    .unwrap_or_else(|e| {
        tracing::error!(message = "unable to initialize memcached client", hosts = ?opts.hosts, error = %e);
        process::exit(1);
    });

    // Do DNS lookups on any "dns+" hostnames to expand them to multiple IPs based on A records.
    let hosts = expand_hosts(&opts.hosts).await.unwrap_or_else(|e| {
        tracing::error!(message = "unable to resolve host names", hosts = ?opts.hosts, error = %e);
        process::exit(1);
    });

    // Run the initial connection to each server once in the main thread to make bad hostnames
    // easier to spot.
    let update_task = UpdateTask::new(&hosts, pool, measurements.clone(), Handle::current());
    update_task.connect().await.unwrap_or_else(|e| {
        tracing::error!(message = "unable to connect to memcached servers", hosts = ?opts.hosts, error = %e);
        process::exit(1);
    });

    task::spawn(
        async move {
            let mut interval = tokio::time::interval(DEFAULT_STATS_INTERVAL);
            loop {
                let _ = interval.tick().await;
                update_task.update().await;
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
            process::exit(1);
        }
        Ok(Err(e)) => {
            tracing::error!(message = "error setting up terminal or running UI", error = %e);
            process::exit(1);
        }
        _ => {}
    }

    Ok(())
}

async fn expand_hosts(hosts: &[String]) -> Result<Vec<String>, MtopError> {
    let mut out = Vec::with_capacity(hosts.len());

    for host in hosts {
        if host.starts_with(DNS_HOST_PREFIX) {
            let name = host.trim_start_matches(DNS_HOST_PREFIX);
            for addr in lookup_host(name).await? {
                out.push(addr.to_string());
            }
        } else {
            out.push(host.clone());
        }
    }

    out.sort();
    Ok(out)
}

#[derive(Debug)]
pub struct UpdateTask {
    hosts: Vec<String>,
    pool: Arc<MemcachedPool>,
    queue: Arc<StatsQueue>,
    handle: Handle,
}

impl UpdateTask {
    pub fn new(hosts: &[String], pool: MemcachedPool, queue: Arc<StatsQueue>, handle: Handle) -> Self {
        UpdateTask {
            hosts: Vec::from(hosts),
            pool: Arc::new(pool),
            queue,
            handle,
        }
    }

    pub async fn connect(&self) -> Result<(), MtopError> {
        for host in self.hosts.iter() {
            let mut client = self.pool.get(host).await?;
            client.ping().await?;
            self.pool.put(client).await;
        }

        Ok(())
    }

    async fn update_host(host: String, pool: Arc<MemcachedPool>) -> Result<(Stats, Slabs, SlabItems), MtopError> {
        let mut client = pool.get(&host).await?;
        let stats = client
            .stats()
            .instrument(tracing::span!(Level::DEBUG, "client.stats"))
            .await?;
        let slabs = client
            .slabs()
            .instrument(tracing::span!(Level::DEBUG, "client.slabs"))
            .await?;
        let items = client
            .items()
            .instrument(tracing::span!(Level::DEBUG, "client.items"))
            .await?;
        pool.put(client).await;
        Ok((stats, slabs, items))
    }

    pub async fn update(&self) {
        let mut tasks = Vec::with_capacity(self.hosts.len());
        for host in self.hosts.clone() {
            tasks.push((
                host.clone(),
                self.handle.spawn(Self::update_host(host, self.pool.clone())),
            ));
        }

        for (host, task) in tasks {
            match task.await {
                Err(e) => tracing::error!(message = "failed to run server update task", "host" = host, "err" = %e),
                Ok(Err(e)) => tracing::warn!(message = "failed to update stats for server", "host" = host, "err" = %e),
                Ok(Ok((stats, slabs, items))) => {
                    self.queue
                        .insert(host, stats, slabs, items)
                        .instrument(tracing::span!(Level::DEBUG, "queue.insert"))
                        .await;
                }
            }
        }
    }
}
