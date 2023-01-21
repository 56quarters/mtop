use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::io::{self, Error as IOError, ErrorKind};
use std::num::{ParseFloatError, ParseIntError};
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
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
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
            let m = Measurement::try_from(e)?;
            //let json = serde_json::to_string_pretty(&e)?;
            //println!("{}", json);
            tracing::info!(message = "raw stats", stats = ?e);
            tracing::info!(message = "parsed stats", stats = ?m)
        }
    }

    Ok(())
}

struct MeasurementDelta {}

#[derive(Debug, Default, PartialEq, Clone)]
struct Measurement {
    // Server info
    pid: u64,
    uptime: u64,
    time: u64,
    version: String,

    // CPU
    rusage_user: f64,
    rusage_system: f64,

    // Connections
    max_connections: u64,
    curr_connections: u64,
    total_connections: u64,
    rejected_connections: u64,

    // Commands
    cmd_get: u64,
    cmd_set: u64,
    cmd_flush: u64,
    cmd_touch: u64,
    cmd_meta: u64,

    // Gets
    get_hits: u64,
    get_misses: u64,
    get_expired: u64,
    get_flushed: u64,

    // Sets
    store_too_large: u64,
    store_no_memory: u64,

    // Deletes
    delete_hits: u64,
    delete_misses: u64,

    // Incr/Decr
    incr_hits: u64,
    incr_misses: u64,
    decr_hits: u64,
    decr_misses: u64,

    // Touches
    touch_hits: u64,
    touch_misses: u64,

    // Bytes
    bytes_read: u64,
    bytes_written: u64,
    bytes: u64,

    // Items
    curr_items: u64,
    total_items: u64,
    evictions: u64,
}

impl TryFrom<&Vec<RawStat>> for Measurement {
    type Error = MtopError;

    fn try_from(value: &Vec<RawStat>) -> Result<Self, Self::Error> {
        parse_stats(value)
    }
}

fn parse_stats(raw: &[RawStat]) -> Result<Measurement, MtopError> {
    let mut out = Measurement::default();

    for e in raw {
        match e.key.as_ref() {
            "pid" => out.pid = parse_u64(&e.key, &e.val)?,
            "uptime" => out.uptime = parse_u64(&e.key, &e.val)?,
            "time" => out.time = parse_u64(&e.key, &e.val)?,
            "version" => out.version = e.val.clone(),

            "rusage_user" => out.rusage_user = parse_f64(&e.key, &e.val)?,
            "rusage_system" => out.rusage_system = parse_f64(&e.key, &e.val)?,

            "max_connections" => out.max_connections = parse_u64(&e.key, &e.val)?,
            "curr_connections" => out.curr_connections = parse_u64(&e.key, &e.val)?,
            "total_connections" => out.total_connections = parse_u64(&e.key, &e.val)?,
            "rejected_connections" => out.rejected_connections = parse_u64(&e.key, &e.val)?,

            "cmd_get" => out.cmd_get = parse_u64(&e.key, &e.val)?,
            "cmd_set" => out.cmd_set = parse_u64(&e.key, &e.val)?,
            "cmd_flush" => out.cmd_flush = parse_u64(&e.key, &e.val)?,
            "cmd_touch" => out.cmd_touch = parse_u64(&e.key, &e.val)?,
            "cmd_meta" => out.cmd_meta = parse_u64(&e.key, &e.val)?,

            "get_hits" => out.get_hits = parse_u64(&e.key, &e.val)?,
            "get_misses" => out.get_misses = parse_u64(&e.key, &e.val)?,
            "get_expired" => out.get_expired = parse_u64(&e.key, &e.val)?,
            "get_flushed" => out.get_flushed = parse_u64(&e.key, &e.val)?,

            "store_too_large" => out.store_too_large = parse_u64(&e.key, &e.val)?,
            "store_no_memory" => out.store_no_memory = parse_u64(&e.key, &e.val)?,

            "delete_hits" => out.delete_hits = parse_u64(&e.key, &e.val)?,
            "delete_misses" => out.decr_misses = parse_u64(&e.key, &e.val)?,

            "incr_hits" => out.incr_hits = parse_u64(&e.key, &e.val)?,
            "incr_misses" => out.incr_misses = parse_u64(&e.key, &e.val)?,
            "decr_hits" => out.delete_hits = parse_u64(&e.key, &e.val)?,
            "decr_misses" => out.decr_misses = parse_u64(&e.key, &e.val)?,

            "touch_hits" => out.touch_hits = parse_u64(&e.key, &e.val)?,
            "touch_misses" => out.touch_misses = parse_u64(&e.key, &e.val)?,

            "bytes_read" => out.bytes_read = parse_u64(&e.key, &e.val)?,
            "bytes_written" => out.bytes_written = parse_u64(&e.key, &e.val)?,
            "bytes" => out.bytes = parse_u64(&e.key, &e.val)?,

            "curr_items" => out.curr_items = parse_u64(&e.key, &e.val)?,
            "total_items" => out.total_items = parse_u64(&e.key, &e.val)?,
            "evictions" => out.evictions = parse_u64(&e.key, &e.val)?,
            _ => {}
        }
    }

    Ok(out)
}

fn parse_u64(key: &str, val: &str) -> Result<u64, MtopError> {
    val.parse().map_err(|e: ParseIntError| {
        MtopError::Internal(format!("field {} value {}, {}", key, val, e))
    })
}

fn parse_f64(key: &str, val: &str) -> Result<f64, MtopError> {
    val.parse().map_err(|e: ParseFloatError| {
        MtopError::Internal(format!("field {} value {}, {}", key, val, e))
    })
}

#[derive(Debug)]
enum MtopError {
    Internal(String),
}

impl fmt::Display for MtopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl Error for MtopError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            _ => None,
        }
    }
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
