use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::fmt::Formatter;
use std::io::{self, Error as IOError, ErrorKind};
use std::num::ParseIntError;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, Lines};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task;
use tracing::{Instrument, Level};

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
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let opts = MtopApplication::parse();

    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(opts.log_level)
            .finish(),
    )
    .expect("failed to set tracing subscriber");

    let queue = Arc::new(Mutex::new(VecDeque::with_capacity(NUM_MEASUREMENTS)));
    let queue_ref = queue.clone();

    task::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(DEFAULT_STATS_INTERVAL_MS));
        loop {
            let _ = interval.tick().await;

            for addr in opts.hosts.iter() {
                // TODO: need to somehow handle this failure in main thread
                tracing::info!(message = "connecting", address = ?addr);
                let c = TcpStream::connect(addr).await.unwrap();
                let (r, w) = c.into_split();
                let mut rw = StatReader::new(r, w);

                match rw
                    .read_stats(StatsCommand::Default)
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

    let mut interval = tokio::time::interval(Duration::from_millis(DEFAULT_STATS_INTERVAL_MS));
    loop {
        let _ = interval.tick().await;
        let queue_ref = queue.clone();

        let mut q = queue_ref.lock().await;
        tracing::info!(message = "queue entries", entries = q.len());
        for e in q.iter() {
            let json = serde_json::to_string(&e)?;
            println!("{}", json);
            tracing::info!(message = "raw stats", stats = ?e);
            tracing::info!(message = "parsed stats", stats = ?parse_stats(e))
        }
    }

    Ok(())
}

fn parse_stats(raw: &[RawStat]) -> io::Result<Vec<ParsedStat>> {
    let mut out = Vec::new();
    for e in raw {
        if e.key == "version" || e.key == "libevent" {
            out.push(ParsedStat::String(e.key.clone(), e.val.clone()));
        } else if e.key == "rusage_user" || e.key == "rusage_system" {
            let v = e.val.parse().unwrap();
            out.push(ParsedStat::Float(e.key.clone(), v));
        } else {
            let v = e.val.parse().unwrap();
            out.push(ParsedStat::Int(e.key.clone(), v));
        }
    }

    Ok(out)
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
enum ParsedStat {
    String(String, String),
    Float(String, f64),
    Int(String, i64),
}

// read raw stats
// parse into numbers
// compute interesting facts: reads/s, writes/s, hit rate, evictions, hostname

// read stats every N ms, push to one end of VecDeque and pop from other
// update display every N ms, read entire contents of VecDeque

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
struct RawStat {
    key: String,
    val: String,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
enum StatsCommand {
    Default,
    Items,
    Slabs,
    Sizes,
}

impl fmt::Display for StatsCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => write!(f, "stats"),
            Self::Items => write!(f, "stats items"),
            Self::Slabs => write!(f, "stats slabs"),
            Self::Sizes => write!(f, "stats sizes"),
        }
    }
}

struct StatReader<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    lines: Lines<BufReader<R>>,
    write: W,
}

impl<R, W> StatReader<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    fn new(read: R, write: W) -> Self {
        StatReader {
            lines: BufReader::new(read).lines(),
            write,
        }
    }

    async fn read_stats(&mut self, cmd: StatsCommand) -> io::Result<Vec<RawStat>> {
        self.write
            .write_all(format!("{}\r\n", cmd).as_bytes())
            .instrument(tracing::span!(Level::DEBUG, "send_command"))
            .await?;

        self.parse_stats()
            .instrument(tracing::span!(Level::DEBUG, "parse_response"))
            .await
    }

    async fn parse_stats(&mut self) -> io::Result<Vec<RawStat>> {
        let mut out = Vec::new();

        loop {
            let line = self.lines.next_line().await?;
            match line.as_deref() {
                Some("END") | None => break,
                Some(v) => {
                    let mut parts = v.split(' ');
                    match (parts.next(), parts.next(), parts.next()) {
                        (Some("STAT"), Some(key), Some(val)) => out.push(RawStat {
                            key: key.to_string(),
                            val: val.to_string(),
                        }),
                        _ => {
                            // TODO: Create our own error type with useful context
                            return Err(IOError::from(ErrorKind::InvalidData));
                        }
                    }
                }
            }
        }

        Ok(out)
    }
}
