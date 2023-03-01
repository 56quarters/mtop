use clap::{Parser, ValueHint};
use mtop::client::{MemcachedPool, MtopError};
use mtop::queue::{BlockingStatsQueue, StatsQueue};
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::{error, process};
use tokio::runtime::Handle;
use tokio::task;
use tracing::instrument::WithSubscriber;
use tracing::{Instrument, Level};

const DEFAULT_LOG_LEVEL: Level = Level::INFO;
// Update interval of more than a second to minimize the chance that stats returned by the
// memcached server have the exact same "time" value (which has one-second granularity).
const DEFAULT_STATS_INTERVAL: Duration = Duration::from_millis(1073);
const NUM_MEASUREMENTS: usize = 10;

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
    let file_subscriber = mtop::tracing::file_subscriber(opts.log_level, log_file).unwrap_or_else(|e| {
        tracing::error!(message = "failed to initialize file logging", error = %e);
        process::exit(1);
    });

    let measurements = Arc::new(StatsQueue::new(NUM_MEASUREMENTS));
    let measurements_ref = measurements.clone();

    // Run the initial connection to each server once in the main thread to make
    // bad hostnames easier to spot.
    let update_task = UpdateTask::new(&opts.hosts, measurements_ref);
    update_task.connect().await.unwrap_or_else(|e| {
        tracing::error!(message = "failed to initialize memcached clients", hosts = ?opts.hosts, error = %e);
        process::exit(1);
    });

    task::spawn(
        async move {
            let mut interval = tokio::time::interval(DEFAULT_STATS_INTERVAL);
            loop {
                let _ = interval.tick().await;
                if let Err(e) = update_task.update().await {
                    tracing::warn!(message = "failed to fetch stats", "err" = %e);
                }
            }
        }
        .with_subscriber(file_subscriber),
    );

    let measurements_ref = measurements.clone();
    let ui_res = task::spawn_blocking(move || {
        let mut term = mtop::ui::initialize_terminal()?;
        mtop::ui::install_panic_handler();

        let blocking_measurements = BlockingStatsQueue::new(measurements_ref, Handle::current());
        let app = mtop::ui::Application::new(&opts.hosts, blocking_measurements);

        // Run the terminal reset unconditionally but prefer to return an error from the
        // application, if available, for logging.
        mtop::ui::run_app(&mut term, app).and(mtop::ui::reset_terminal())
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

pub struct UpdateTask {
    hosts: Vec<String>,
    pool: MemcachedPool,
    queue: Arc<StatsQueue>,
}

impl UpdateTask {
    pub fn new(hosts: &[String], queue: Arc<StatsQueue>) -> Self {
        UpdateTask {
            queue,
            hosts: Vec::from(hosts),
            pool: MemcachedPool::new(),
        }
    }

    pub async fn connect(&self) -> Result<(), MtopError> {
        for host in self.hosts.iter() {
            let client = self.pool.get(host).await?;
            self.pool.put(client).await;
        }

        Ok(())
    }

    pub async fn update(&self) -> Result<(), MtopError> {
        for host in self.hosts.iter() {
            let mut client = self.pool.get(host).await?;
            let stats = client
                .stats()
                .instrument(tracing::span!(Level::DEBUG, "read_stats"))
                .await?;

            self.queue.insert(host.to_owned(), stats).await;
            self.pool.put(client).await;
        }

        Ok(())
    }
}
