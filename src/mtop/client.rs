use std::error;
use std::fmt;
use std::io;
use std::num::{ParseFloatError, ParseIntError};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, Lines};
use tracing::{Instrument, Level};

#[derive(Debug, Default, PartialEq, Clone)]
pub struct Measurement {
    // Server info
    pub pid: u64,
    pub uptime: u64,
    pub time: i64,
    pub version: String,

    // CPU
    pub rusage_user: f64,
    pub rusage_system: f64,

    // Connections
    pub max_connections: u64,
    pub curr_connections: u64,
    pub total_connections: u64,
    pub rejected_connections: u64,

    // Commands
    pub cmd_get: u64,
    pub cmd_set: u64,
    pub cmd_flush: u64,
    pub cmd_touch: u64,
    pub cmd_meta: u64,

    // Gets
    pub get_hits: u64,
    pub get_misses: u64,
    pub get_expired: u64,
    pub get_flushed: u64,

    // Sets
    pub store_too_large: u64,
    pub store_no_memory: u64,

    // Deletes
    pub delete_hits: u64,
    pub delete_misses: u64,

    // Incr/Decr
    pub incr_hits: u64,
    pub incr_misses: u64,
    pub decr_hits: u64,
    pub decr_misses: u64,

    // Touches
    pub touch_hits: u64,
    pub touch_misses: u64,

    // Bytes
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub bytes: u64,
    pub max_bytes: u64,

    // Items
    pub curr_items: u64,
    pub total_items: u64,
    pub evictions: u64,
}

impl TryFrom<Vec<RawStat>> for Measurement {
    type Error = MtopError;

    fn try_from(value: Vec<RawStat>) -> Result<Self, Self::Error> {
        let mut out = Measurement::default();
        for e in value {
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
                "limit_maxbytes" => out.max_bytes = parse_u64(&e.key, &e.val)?,

                "curr_items" => out.curr_items = parse_u64(&e.key, &e.val)?,
                "total_items" => out.total_items = parse_u64(&e.key, &e.val)?,
                "evictions" => out.evictions = parse_u64(&e.key, &e.val)?,
                _ => {}
            }
        }

        Ok(out)
    }
}

fn parse_u64(key: &str, val: &str) -> Result<u64, MtopError> {
    val.parse()
        .map_err(|e: ParseIntError| MtopError::Internal(format!("field {} value {}, {}", key, val, e)))
}

fn parse_i64(key: &str, val: &str) -> Result<i64, MtopError> {
    val.parse()
        .map_err(|e: ParseIntError| MtopError::Internal(format!("field {} value {}, {}", key, val, e)))
}

fn parse_f64(key: &str, val: &str) -> Result<f64, MtopError> {
    val.parse()
        .map_err(|e: ParseFloatError| MtopError::Internal(format!("field {} value {}, {}", key, val, e)))
}

#[derive(Debug)]
pub enum MtopError {
    Internal(String),
    Protocol(ProtocolError),
    IO(io::Error),
}

impl fmt::Display for MtopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
            Self::Protocol(e) => write!(f, "protocol error: {}", e),
            Self::IO(e) => write!(f, "io error: {}", e),
        }
    }
}

impl error::Error for MtopError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Protocol(e) => Some(e),
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

impl From<ProtocolError> for MtopError {
    fn from(e: ProtocolError) -> Self {
        Self::Protocol(e)
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, Hash)]
pub enum ProtocolErrorKind {
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
pub struct ProtocolError {
    kind: ProtocolErrorKind,
    message: Option<String>,
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
pub enum StatsCommand {
    Default,
}

impl fmt::Display for StatsCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => write!(f, "stats"),
        }
    }
}

pub struct Memcached<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    lines: Lines<BufReader<R>>,
    write: W,
}

impl<R, W> Memcached<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    pub fn new(read: R, write: W) -> Self {
        Memcached {
            lines: BufReader::new(read).lines(),
            write,
        }
    }

    pub async fn stats(&mut self, cmd: StatsCommand) -> Result<Measurement, MtopError> {
        self.write
            .write_all(format!("{}\r\n", cmd).as_bytes())
            .instrument(tracing::span!(Level::DEBUG, "send_command"))
            .await?;

        let raw = self
            .parse_lines()
            .instrument(tracing::span!(Level::DEBUG, "parse_response"))
            .await?;

        Measurement::try_from(raw)
    }

    async fn parse_lines(&mut self) -> Result<Vec<RawStat>, MtopError> {
        let mut out = Vec::new();

        loop {
            let line = self.lines.next_line().await?;
            match line.as_deref() {
                Some("END") | None => break,
                Some(v) => {
                    if let Some(raw) = self.parse_stat(v) {
                        out.push(raw);
                    } else if let Some(err) = self.parse_error(v) {
                        return Err(MtopError::Protocol(err));
                    } else {
                        return Err(MtopError::Internal(format!("unable to parse '{}'", v)));
                    }
                }
            }
        }

        Ok(out)
    }

    fn parse_stat(&self, line: &str) -> Option<RawStat> {
        let mut parts = line.splitn(3, ' ');
        match (parts.next(), parts.next(), parts.next()) {
            (Some("STAT"), Some(key), Some(val)) => Some(RawStat {
                key: key.to_string(),
                val: val.to_string(),
            }),
            _ => None,
        }
    }

    fn parse_error(&self, line: &str) -> Option<ProtocolError> {
        let mut values = line.splitn(2, ' ');
        let (kind, message) = match (values.next(), values.next()) {
            (Some("ERROR"), None) => (ProtocolErrorKind::Syntax, None),
            (Some("ERROR"), Some(msg)) => (ProtocolErrorKind::Syntax, Some(msg.to_owned())),
            (Some("CLIENT_ERROR"), Some(msg)) => (ProtocolErrorKind::Client, Some(msg.to_owned())),
            (Some("SERVER_ERROR"), Some(msg)) => (ProtocolErrorKind::Server, Some(msg.to_owned())),
            _ => return None,
        };

        Some(ProtocolError { kind, message })
    }
}
