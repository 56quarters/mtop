use clap::Parser;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::execute;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use mtop::client::{Memcached, MtopError, StatsCommand};
use mtop::queue::{BlockingMeasurementQueue, MeasurementQueue};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use std::{error, process};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio::task;
use tracing::{Instrument, Level};
use tui::{backend::CrosstermBackend, Terminal};

const DEFAULT_LOG_LEVEL: Level = Level::INFO;
const DEFAULT_STATS_INTERVAL_MS: u64 = 1000;
const NUM_MEASUREMENTS: usize = 10;

/// mtop: top for memcached
#[derive(Debug, Parser)]
#[clap(name = "mtop", version = clap::crate_version ! ())]
struct MtopApplication {
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
    let opts = MtopApplication::parse();

    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(opts.log_level)
            .finish(),
    )
    .expect("failed to set tracing subscriber");

    let hosts = opts.hosts.clone();
    let queue = Arc::new(MeasurementQueue::new(NUM_MEASUREMENTS));
    let queue_ref = queue.clone();

    let mut client_pool = ClientPool::new(&hosts, queue_ref).await.unwrap_or_else(|e| {
        tracing::error!(message = "failed to initialize memcached clients", hosts = ?hosts, error = %e);
        process::exit(1)
    });

    task::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(DEFAULT_STATS_INTERVAL_MS));
        loop {
            let _ = interval.tick().await;
            if let Err(e) = client_pool.update().await {
                tracing::warn!(message = "failed to fetch stats", "err" = %e);
            }
        }
    });

    let hosts = opts.hosts.clone();
    let queue_ref = queue.clone();

    // TODO: Clean all this up
    let _ = task::spawn_blocking(move || {
        enable_raw_mode().unwrap();
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture).unwrap();
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend).unwrap();

        let blocking = BlockingMeasurementQueue::new(queue_ref, Handle::current());
        let app = mtop::ui::Application::new(hosts, blocking);
        let _res = mtop::ui::run_app(&mut terminal, app);

        disable_raw_mode().unwrap();
        execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture).unwrap();
        terminal.show_cursor().unwrap();
    })
    .await;

    Ok(())
}

// TODO: This works but it's kinda gross

pub struct ClientPool {
    clients: HashMap<String, Memcached<OwnedReadHalf, OwnedWriteHalf>>,
    queue: Arc<MeasurementQueue>,
}

impl ClientPool {
    pub async fn new(hosts: &[String], queue: Arc<MeasurementQueue>) -> Result<Self, MtopError> {
        let mut clients = HashMap::new();

        for host in hosts {
            let client = connect(&host).await?;
            clients.insert(host.clone(), client);
        }

        Ok(ClientPool { clients, queue })
    }

    pub async fn update(&mut self) -> Result<(), MtopError> {
        for (host, client) in self.clients.iter_mut() {
            match client
                .stats(StatsCommand::Default)
                .instrument(tracing::span!(Level::DEBUG, "read_stats"))
                .await
            {
                Ok(v) => self.queue.insert(host.clone(), v).await,
                Err(_) => {
                    // TODO: What should we actually do here? Need to replace the client (probably?)
                    //  but we don't want to throw away the actual error that happened.
                    let replacement = connect(&host).await?;
                    *client = replacement;
                }
            }
        }

        Ok(())
    }
}

async fn connect(host: &str) -> Result<Memcached<OwnedReadHalf, OwnedWriteHalf>, MtopError> {
    let c = TcpStream::connect(host).await?;
    let (r, w) = c.into_split();
    Ok(Memcached::new(r, w))
}
