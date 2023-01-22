use clap::Parser;
use std::collections::VecDeque;
use std::error;
use std::fmt;
use std::io;
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
async fn main() -> Result<(), Box<dyn error::Error + Send + Sync>> {
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

        if let Some(e) = q.pop_front() {
            let m = Measurement::try_from(&e)?;
            tracing::info!(message = "raw stats", stats = ?e);
            tracing::info!(message = "parsed stats", stats = ?m)
        }
    }

    Ok(())
}

#[derive(Debug, Default, PartialEq, Clone)]
struct Measurement {
    // Server info
    pid: u64,
    uptime: u64,
    time: i64,
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
            "time" => out.time = parse_i64(&e.key, &e.val)?,
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

fn parse_i64(key: &str, val: &str) -> Result<i64, MtopError> {
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
    Protocol(ProtocolError),
    IO(io::Error),
}

impl fmt::Display for MtopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
            Self::Protocol(e) => write!(f, "protocol error: {}", e),
            Self::IO(e) => fmt::Display::fmt(e, f),
        }
    }
}

impl error::Error for MtopError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::IO(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for MtopError {
    fn from(e: io::Error) -> Self {
        Self::IO(e)
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, Hash)]
enum ProtocolErrorKind {
    Syntax,
    Client,
    Server,
}

impl fmt::Display for ProtocolErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syntax => "ERROR".fmt(f),
            Self::Client => "CLIENT_ERROR".fmt(f),
            Self::Server => "SERVER_ERROR".fmt(f),
        }
    }
}

#[derive(Debug)]
struct ProtocolError {
    kind: ProtocolErrorKind,
    message: Option<String>,
}

impl TryFrom<&str> for ProtocolError {
    type Error = MtopError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut values = value.splitn(2, ' ');
        let (kind, message) = match (values.next(), values.next()) {
            (Some("ERROR"), None) => (ProtocolErrorKind::Syntax, None),
            (Some("ERROR"), Some(msg)) => (ProtocolErrorKind::Syntax, Some(msg.to_owned())),
            (Some("CLIENT_ERROR"), Some(msg)) => (ProtocolErrorKind::Client, Some(msg.to_owned())),
            (Some("SERVER_ERROR"), Some(msg)) => (ProtocolErrorKind::Server, Some(msg.to_owned())),
            _ => {
                return Err(MtopError::Internal(format!(
                    "unable to parse line '{}'",
                    value
                )));
            }
        };

        Ok(ProtocolError { kind, message })
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(msg) = &self.message {
            write!(f, "{} {}", self.kind, msg)
        } else {
            write!(f, "{}", self.kind)
        }
    }
}

impl error::Error for ProtocolError {}

#[derive(Debug, Eq, PartialEq, Clone)]
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

    async fn read_stats(&mut self, cmd: StatsCommand) -> Result<Vec<RawStat>, MtopError> {
        self.write
            .write_all(format!("{}\r\n", cmd).as_bytes())
            .instrument(tracing::span!(Level::DEBUG, "send_command"))
            .await?;

        self.parse_lines()
            .instrument(tracing::span!(Level::DEBUG, "parse_response"))
            .await
    }

    async fn parse_lines(&mut self) -> Result<Vec<RawStat>, MtopError> {
        let mut out = Vec::new();

        loop {
            let line = self.lines.next_line().await?;
            match line.as_deref() {
                Some("END") | None => break,
                Some(v) => {
                    let mut parts = v.splitn(3, ' ');
                    match (parts.next(), parts.next(), parts.next()) {
                        (Some("STAT"), Some(key), Some(val)) => out.push(RawStat {
                            key: key.to_string(),
                            val: val.to_string(),
                        }),
                        _ => {
                            // If this line doesn't look like a stat, try to parse it as a memcached
                            // protocol error which will fall back to "internal error" if it can't
                            // actually be parsed as a protocol error.
                            return Err(MtopError::Protocol(ProtocolError::try_from(v)?));
                        }
                    }
                }
            }
        }

        Ok(out)
    }
}
