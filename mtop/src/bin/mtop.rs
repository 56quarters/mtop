use clap::{Parser, ValueHint};
use mtop::queue::{BlockingStatsQueue, StatsQueue};
use mtop_client::{MemcachedPool, MtopError, PoolConfig, SlabItems, Slabs, Stats, TLSConfig, Timeout};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::{env, error, process};
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
    #[arg(long, requires = "tls_key", value_hint = ValueHint::FilePath)]
    tls_cert: Option<PathBuf>,

    /// Optional client key to use to authenticate with the Memcached server. Note that this may
    /// or may not be required based on how the Memcached server is configured.
    #[arg(long, requires = "tls_cert", value_hint = ValueHint::FilePath)]
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

    let timeout = Duration::from_secs(opts.timeout_secs);
    let measurements = Arc::new(StatsQueue::new(NUM_MEASUREMENTS));
    let pool = MemcachedPool::new(
        Handle::current(),
        PoolConfig {
            tls: TLSConfig {
                enabled: opts.tls_enabled,
                ca_path: opts.tls_ca,
                cert_path: opts.tls_cert,
                key_path: opts.tls_key,
                server_name: opts.tls_server_name,
            },
            ..Default::default()
        },
    )
    .await
    .unwrap_or_else(|e| {
        tracing::error!(message = "unable to initialize memcached client", hosts = ?opts.hosts, error = %e);
        process::exit(1);
    });

    // Do DNS lookups on any "dns+" hostnames to expand them to multiple IPs based on A records.
    let hosts = expand_hosts(&opts.hosts, timeout).await.unwrap_or_else(|e| {
        tracing::error!(message = "unable to resolve host names", hosts = ?opts.hosts, error = %e);
        process::exit(1);
    });

    // Run the initial connection to each server once in the main thread to make bad hostnames
    // easier to spot.
    let update_task = UpdateTask::new(&hosts, pool, measurements.clone(), timeout, Handle::current());
    update_task.connect().await.unwrap_or_else(|e| {
        tracing::error!(message = "unable to connect to memcached servers", hosts = ?opts.hosts, error = %e);
        process::exit(1);
    });

    task::spawn(
        async move {
            let mut interval = tokio::time::interval(DEFAULT_STATS_INTERVAL);
            loop {
                let _ = interval.tick().await;
                update_task
                    .update()
                    .instrument(tracing::span!(Level::INFO, "periodic.update"))
                    .await;
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

#[derive(Debug)]
struct UpdateTask {
    hosts: Vec<String>,
    pool: Arc<MemcachedPool>,
    queue: Arc<StatsQueue>,
    timeout: Duration,
    handle: Handle,
}

impl UpdateTask {
    fn new(hosts: &[String], pool: MemcachedPool, queue: Arc<StatsQueue>, timeout: Duration, handle: Handle) -> Self {
        UpdateTask {
            hosts: Vec::from(hosts),
            pool: Arc::new(pool),
            queue,
            timeout,
            handle,
        }
    }

    async fn connect(&self) -> Result<(), MtopError> {
        for host in self.hosts.iter() {
            let client = self
                .pool
                .get(host)
                .timeout(self.timeout, "client.connect")
                .instrument(tracing::span!(Level::INFO, "client.connect"))
                .await?;
            self.pool.put(client).await;
        }

        Ok(())
    }

    async fn update_host(
        host: String,
        pool: Arc<MemcachedPool>,
        timeout: Duration,
    ) -> Result<(Stats, Slabs, SlabItems), MtopError> {
        let mut client = pool
            .get(&host)
            .timeout(timeout, "client.connect")
            .instrument(tracing::span!(Level::INFO, "client.connect"))
            .await?;
        let stats = client
            .stats()
            .timeout(timeout, "client.stats")
            .instrument(tracing::span!(Level::INFO, "client.stats"))
            .await?;
        let slabs = client
            .slabs()
            .timeout(timeout, "client.slabs")
            .instrument(tracing::span!(Level::INFO, "client.slabs"))
            .await?;
        let items = client
            .items()
            .timeout(timeout, "client.items")
            .instrument(tracing::span!(Level::INFO, "client.items"))
            .await?;

        pool.put(client).await;
        Ok((stats, slabs, items))
    }

    async fn update(&self) {
        let mut tasks = Vec::with_capacity(self.hosts.len());
        for host in self.hosts.iter() {
            tasks.push((
                host,
                self.handle
                    .spawn(Self::update_host(host.clone(), self.pool.clone(), self.timeout)),
            ));
        }

        for (host, task) in tasks {
            match task.await {
                Err(e) => tracing::error!(message = "failed to run server update task", host = host, err = %e),
                Ok(Err(e)) => tracing::warn!(message = "failed to update stats for server", host = host, err = %e),
                Ok(Ok((stats, slabs, items))) => {
                    self.queue
                        .insert(host.clone(), stats, slabs, items)
                        .instrument(tracing::span!(Level::INFO, "queue.insert"))
                        .await;
                }
            }
        }
    }
}
