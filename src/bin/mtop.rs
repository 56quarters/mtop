use clap::Parser;
use mtop::client::{MemcachedPool, MtopError, StatsCommand};
use mtop::queue::{BlockingMeasurementQueue, MeasurementQueue};
use std::error;
use std::panic;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::task;
use tracing::{Instrument, Level};

const DEFAULT_LOG_LEVEL: Level = Level::INFO;
// Update interval of more than a second to minimize the chance that stats returned by the
// memcached server have the exact same "time" value (which has one-second granularity).
const DEFAULT_STATS_INTERVAL_MS: u64 = 1073;
const NUM_MEASUREMENTS: usize = 10;

/// mtop: top for memcached
#[derive(Debug, Parser)]
#[clap(name = "mtop", version = clap::crate_version ! ())]
struct MtopConfig {
    /// Logging verbosity. Allowed values are 'trace', 'debug', 'info', 'warn', and 'error'
    /// (case insensitive)
    #[clap(long, default_value_t = DEFAULT_LOG_LEVEL)]
    log_level: Level,

    /// Memcached hosts to connect to in the form 'hostname:port'. Must be specified at least
    /// once and may be used multiple times (separated by spaces).
    #[clap(required = true)]
    hosts: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error + Send + Sync>> {
    let opts = MtopConfig::parse();

    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(opts.log_level)
            .finish(),
    )
    .expect("failed to set tracing subscriber");

    let queue = Arc::new(MeasurementQueue::new(NUM_MEASUREMENTS));
    let queue_ref = queue.clone();

    // Run the initial connection to each server once in the main thread to make
    // bad hostnames easier to spot.
    let update_task = UpdateTask::new(&opts.hosts, queue_ref);
    update_task.connect().await.unwrap_or_else(|e| {
        tracing::error!(message = "failed to initialize memcached clients", hosts = ?opts.hosts, error = %e);
        process::exit(1)
    });

    task::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(DEFAULT_STATS_INTERVAL_MS));
        loop {
            let _ = interval.tick().await;
            if let Err(e) = update_task.update().await {
                tracing::warn!(message = "failed to fetch stats", "err" = %e);
            }
        }
    });

    let queue_ref = queue.clone();

    let _ = task::spawn_blocking(move || {
        let mut term = mtop::ui::initialize_terminal().unwrap();

        let original_hook = panic::take_hook();
        panic::set_hook(Box::new(move |p| {
            mtop::ui::reset_terminal().unwrap();
            original_hook(p);
        }));

        let blocking = BlockingMeasurementQueue::new(queue_ref, Handle::current());
        let app = mtop::ui::Application::new(&opts.hosts, blocking);
        let _res = mtop::ui::run_app(&mut term, app);

        mtop::ui::reset_terminal().unwrap();
    })
    .await;

    Ok(())
}

pub struct UpdateTask {
    hosts: Vec<String>,
    pool: MemcachedPool,
    queue: Arc<MeasurementQueue>,
}

impl UpdateTask {
    pub fn new(hosts: &[String], queue: Arc<MeasurementQueue>) -> Self {
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
                .stats(StatsCommand::Default)
                .instrument(tracing::span!(Level::DEBUG, "read_stats"))
                .await?;

            self.queue.insert(host.to_owned(), stats).await;
            self.pool.put(client).await;
        }

        Ok(())
    }
}
