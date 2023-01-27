use std::collections::HashMap;
use std::error;
use std::fmt;
use std::io;
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

impl TryFrom<HashMap<String, String>> for Measurement {
    type Error = MtopError;

    fn try_from(value: HashMap<String, String>) -> Result<Self, Self::Error> {
        Ok(Measurement {
            pid: parse_u64("pid", &value)?,
            uptime: parse_u64("uptime", &value)?,
            time: parse_i64("time", &value)?,
            version: parse_string("version", &value)?,

            rusage_user: parse_f64("rusage_user", &value)?,
            rusage_system: parse_f64("rusage_system", &value)?,
            max_connections: parse_u64("max_connections", &value)?,
            curr_connections: parse_u64("curr_connections", &value)?,
            total_connections: parse_u64("total_connections", &value)?,
            rejected_connections: parse_u64("rejected_connections", &value)?,

            cmd_get: parse_u64("cmd_get", &value)?,
            cmd_set: parse_u64("cmd_set", &value)?,
            cmd_flush: parse_u64("cmd_flush", &value)?,
            cmd_touch: parse_u64("cmd_touch", &value)?,
            cmd_meta: parse_u64("cmd_meta", &value)?,

            get_hits: parse_u64("get_hits", &value)?,
            get_misses: parse_u64("get_misses", &value)?,
            get_expired: parse_u64("get_expired", &value)?,
            get_flushed: parse_u64("get_flushed", &value)?,

            store_too_large: parse_u64("store_too_large", &value)?,
            store_no_memory: parse_u64("store_no_memory", &value)?,

            delete_hits: parse_u64("delete_hits", &value)?,
            delete_misses: parse_u64("delete_misses", &value)?,

            incr_hits: parse_u64("incr_hits", &value)?,
            incr_misses: parse_u64("incr_misses", &value)?,

            decr_hits: parse_u64("decr_hits", &value)?,
            decr_misses: parse_u64("decr_misses", &value)?,

            touch_hits: parse_u64("touch_hits", &value)?,
            touch_misses: parse_u64("touch_misses", &value)?,

            bytes_read: parse_u64("bytes_read", &value)?,
            bytes_written: parse_u64("bytes_written", &value)?,
            bytes: parse_u64("bytes", &value)?,
            max_bytes: parse_u64("limit_maxbytes", &value)?,

            curr_items: parse_u64("curr_items", &value)?,
            total_items: parse_u64("total_items", &value)?,
            evictions: parse_u64("evictions", &value)?,
        })
    }
}

fn parse_u64(key: &str, map: &HashMap<String, String>) -> Result<u64, MtopError> {
    map.get(key)
        .ok_or_else(|| MtopError::Internal(format!("field {} missing", key)))
        .and_then(|v| {
            v.parse()
                .map_err(|e| MtopError::Internal(format!("field {} value {}, {}", key, v, e)))
        })
}

fn parse_i64(key: &str, map: &HashMap<String, String>) -> Result<i64, MtopError> {
    map.get(key)
        .ok_or_else(|| MtopError::Internal(format!("field {} missing", key)))
        .and_then(|v| {
            v.parse()
                .map_err(|e| MtopError::Internal(format!("field {} value {}, {}", key, v, e)))
        })
}

fn parse_f64(key: &str, map: &HashMap<String, String>) -> Result<f64, MtopError> {
    map.get(key)
        .ok_or_else(|| MtopError::Internal(format!("field {} missing", key)))
        .and_then(|v| {
            v.parse()
                .map_err(|e| MtopError::Internal(format!("field {} value {}, {}", key, v, e)))
        })
}

fn parse_string(key: &str, map: &HashMap<String, String>) -> Result<String, MtopError> {
    map.get(key)
        .ok_or_else(|| MtopError::Internal(format!("field {} missing", key)))
        .map(|v| v.clone())
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

    async fn parse_lines(&mut self) -> Result<HashMap<String, String>, MtopError> {
        let mut out = HashMap::new();

        loop {
            let line = self.lines.next_line().await?;
            match line.as_deref() {
                Some("END") | None => break,
                Some(v) => {
                    if let Some(raw) = self.parse_stat(v) {
                        out.insert(raw.key, raw.val);
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
