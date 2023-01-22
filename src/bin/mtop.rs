use clap::Parser;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use mtop::client::{Measurement, Memcached, StatsCommand};
use std::collections::VecDeque;
use std::error;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task;
use tracing::{Instrument, Level};
use tui::{backend::CrosstermBackend, Terminal};

const DEFAULT_LOG_LEVEL: Level = Level::INFO;
const DEFAULT_STATS_INTERVAL_MS: u64 = 1000;
const NUM_MEASUREMENTS: usize = 3;

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

    // TODO: Need a queue per hostname
    let queue = Arc::new(Mutex::new(VecDeque::with_capacity(NUM_MEASUREMENTS)));
    let queue_ref = queue.clone();

    task::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(DEFAULT_STATS_INTERVAL_MS));
        loop {
            let _ = interval.tick().await;

            for addr in opts.hosts.iter() {
                // TODO: need to somehow handle this failure in main thread
                //tracing::info!(message = "connecting", address = ?addr);
                let c = TcpStream::connect(addr).await.unwrap();
                let (r, w) = c.into_split();
                let mut cache = Memcached::new(r, w);

                match cache
                    .stats(StatsCommand::Default)
                    .instrument(tracing::span!(Level::DEBUG, "read_stats"))
                    .await
                {
                    Ok(v) => {
                        let mut q = queue_ref.lock().await;
                        q.push_back(v);
                        if q.len() > NUM_MEASUREMENTS {
                            q.pop_front();
                        }
                    }
                    Err(e) => tracing::warn!(message = "failed to fetch stats", "err" = %e),
                }
            }
        }
    });

    let mut m: Vec<Measurement> = Vec::new();
    let mut interval = tokio::time::interval(Duration::from_millis(DEFAULT_STATS_INTERVAL_MS));
    loop {
        let _ = interval.tick().await;
        let queue_ref = queue.clone();

        let mut q = queue_ref.lock().await;
        //tracing::info!(message = "queue entries", entries = q.len());

        if let Some(e) = q.pop_front() {
            m.push(e);

            if m.len() > 3 {
                break;
            }

            //tracing::info!(message = "raw stats", stats = ?e);
            //tracing::info!(message = "parsed stats", stats = ?m)
        }
    }

    task::spawn_blocking(move || {
        enable_raw_mode().unwrap();
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture).unwrap();
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend).unwrap();

        let app = mtop::ui::App::new(m);
        let res = mtop::ui::run_app(&mut terminal, app);

        disable_raw_mode().unwrap();
        execute!(
            terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        )
        .unwrap();
        terminal.show_cursor().unwrap();
    });

    Ok(())
}
