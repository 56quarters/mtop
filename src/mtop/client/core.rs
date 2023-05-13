use std::cmp::Ordering;
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::io;
use std::ops::Deref;
use std::str::FromStr;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter, Lines};

#[derive(Debug, Default, PartialEq, Clone)]
pub struct Stats {
    // Server info
    pub pid: u64,
    pub uptime: u64,
    pub server_time: u64,
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

impl TryFrom<&HashMap<String, String>> for Stats {
    type Error = MtopError;

    fn try_from(value: &HashMap<String, String>) -> Result<Self, Self::Error> {
        Ok(Stats {
            pid: parse_field("pid", value)?,
            uptime: parse_field("uptime", value)?,
            server_time: parse_field("time", value)?,
            version: parse_field("version", value)?,

            rusage_user: parse_field("rusage_user", value)?,
            rusage_system: parse_field("rusage_system", value)?,

            max_connections: parse_field("max_connections", value)?,
            curr_connections: parse_field("curr_connections", value)?,
            total_connections: parse_field("total_connections", value)?,
            rejected_connections: parse_field("rejected_connections", value)?,

            cmd_get: parse_field("cmd_get", value)?,
            cmd_set: parse_field("cmd_set", value)?,
            cmd_flush: parse_field("cmd_flush", value)?,
            cmd_touch: parse_field("cmd_touch", value)?,
            cmd_meta: parse_field("cmd_meta", value)?,

            get_hits: parse_field("get_hits", value)?,
            get_misses: parse_field("get_misses", value)?,
            get_expired: parse_field("get_expired", value)?,
            get_flushed: parse_field("get_flushed", value)?,

            store_too_large: parse_field("store_too_large", value)?,
            store_no_memory: parse_field("store_no_memory", value)?,

            delete_hits: parse_field("delete_hits", value)?,
            delete_misses: parse_field("delete_misses", value)?,

            incr_hits: parse_field("incr_hits", value)?,
            incr_misses: parse_field("incr_misses", value)?,

            decr_hits: parse_field("decr_hits", value)?,
            decr_misses: parse_field("decr_misses", value)?,

            touch_hits: parse_field("touch_hits", value)?,
            touch_misses: parse_field("touch_misses", value)?,

            bytes_read: parse_field("bytes_read", value)?,
            bytes_written: parse_field("bytes_written", value)?,
            bytes: parse_field("bytes", value)?,
            max_bytes: parse_field("limit_maxbytes", value)?,

            curr_items: parse_field("curr_items", value)?,
            total_items: parse_field("total_items", value)?,
            evictions: parse_field("evictions", value)?,
        })
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Meta {
    pub key: String,
    pub expires: i64, // Signed because Memcached uses '-1' for infinite/no TTL
    pub size: u64,
}

impl TryFrom<&HashMap<String, String>> for Meta {
    type Error = MtopError;

    fn try_from(value: &HashMap<String, String>) -> Result<Self, Self::Error> {
        Ok(Meta {
            key: parse_field("key", value)?,
            expires: parse_field("exp", value)?,
            size: parse_field("size", value)?,
        })
    }
}

impl PartialOrd for Meta {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl Ord for Meta {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Value {
    pub key: String,
    pub cas: u64,
    pub flags: u64,
    pub data: Vec<u8>,
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

fn parse_field<T>(key: &str, map: &HashMap<String, String>) -> Result<T, MtopError>
where
    T: FromStr,
    <T as FromStr>::Err: fmt::Display + Send + Sync + error::Error + 'static,
{
    map.get(key)
        .ok_or_else(|| MtopError::internal(format!("field {} missing", key)))
        .and_then(|v| {
            v.parse()
                .map_err(|e| MtopError::internal_cause(format!("field {} value '{}'", key, v), e))
        })
}

fn parse_value<T>(val: &str, line: &str) -> Result<T, MtopError>
where
    T: FromStr + fmt::Display,
    <T as FromStr>::Err: fmt::Display + Send + Sync + error::Error + 'static,
{
    val.parse()
        .map_err(|e| MtopError::internal_cause(format!("parsing {} from '{}'", val, line), e))
}

#[derive(Debug, PartialOrd, PartialEq, Copy, Clone)]
pub enum ErrorKind {
    Internal,
    IO,
    Protocol,
    Configuration,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Internal => write!(f, "internal error"),
            Self::IO => write!(f, "io error"),
            Self::Protocol => write!(f, "protocol error"),
            Self::Configuration => write!(f, "configuration error"),
        }
    }
}

#[derive(Debug)]
enum ErrorRepr {
    Message(String),
    Cause(Box<dyn error::Error + Send + Sync + 'static>),
    MessageCause(String, Box<dyn error::Error + Send + Sync + 'static>),
}

#[derive(Debug)]
pub struct MtopError {
    kind: ErrorKind,
    repr: ErrorRepr,
}

impl MtopError {
    pub fn internal<S>(msg: S) -> MtopError
    where
        S: Into<String>,
    {
        MtopError {
            kind: ErrorKind::Internal,
            repr: ErrorRepr::Message(msg.into()),
        }
    }

    pub fn internal_cause<S, E>(msg: S, e: E) -> MtopError
    where
        S: Into<String>,
        E: error::Error + Send + Sync + 'static,
    {
        MtopError {
            kind: ErrorKind::Internal,
            repr: ErrorRepr::MessageCause(msg.into(), Box::new(e)),
        }
    }

    pub fn configuration<S>(msg: S) -> MtopError
    where
        S: Into<String>,
    {
        MtopError {
            kind: ErrorKind::Configuration,
            repr: ErrorRepr::Message(msg.into()),
        }
    }

    pub fn configuration_cause<S, E>(msg: S, e: E) -> MtopError
    where
        S: Into<String>,
        E: error::Error + Send + Sync + 'static,
    {
        MtopError {
            kind: ErrorKind::Configuration,
            repr: ErrorRepr::MessageCause(msg.into(), Box::new(e)),
        }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl fmt::Display for MtopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            ErrorRepr::Message(msg) => write!(f, "{}: {}", self.kind, msg),
            ErrorRepr::Cause(e) => write!(f, "{}: {}", self.kind, e),
            ErrorRepr::MessageCause(msg, e) => write!(f, "{}: {}: {}", self.kind, msg, e),
        }
    }
}

impl error::Error for MtopError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match &self.repr {
            ErrorRepr::Message(_) => None,
            ErrorRepr::Cause(e) => Some(e.as_ref()),
            ErrorRepr::MessageCause(_, e) => Some(e.as_ref()),
        }
    }
}

impl From<io::Error> for MtopError {
    fn from(e: io::Error) -> Self {
        MtopError {
            kind: ErrorKind::IO,
            repr: ErrorRepr::Cause(Box::new(e)),
        }
    }
}

impl From<ProtocolError> for MtopError {
    fn from(e: ProtocolError) -> Self {
        MtopError {
            kind: ErrorKind::Protocol,
            repr: ErrorRepr::Cause(Box::new(e)),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, Hash)]
pub enum ProtocolErrorKind {
    BadClass,
    Busy,
    Client,
    NotFound,
    Server,
    Syntax,
}

impl fmt::Display for ProtocolErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadClass => "BADCLASS".fmt(f),
            Self::Busy => "BUSY".fmt(f),
            Self::Client => "CLIENT_ERROR".fmt(f),
            Self::NotFound => "NOT_FOUND".fmt(f),
            Self::Server => "SERVER_ERROR".fmt(f),
            Self::Syntax => "ERROR".fmt(f),
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
enum Command<'a> {
    CrawlerMetadump,
    Delete(&'a str),
    Gets(&'a [String]),
    Stats,
    Set(&'a str, u64, u32, &'a [u8]),
    Touch(&'a str, u32),
    Version,
}

impl<'a> From<Command<'a>> for Vec<u8> {
    fn from(value: Command<'a>) -> Self {
        let buf = match value {
            Command::CrawlerMetadump => "lru_crawler metadump all\r\n".to_owned().into_bytes(),
            Command::Delete(key) => format!("delete {}\r\n", key).into_bytes(),
            Command::Gets(keys) => format!("gets {}\r\n", keys.join(" ")).into_bytes(),
            Command::Stats => "stats\r\n".to_owned().into_bytes(),
            Command::Set(key, flags, ttl, data) => {
                let mut set = Vec::with_capacity(key.len() + data.len() + 32);
                io::Write::write_all(
                    &mut set,
                    format!("set {} {} {} {}\r\n", key, flags, ttl, data.len()).as_bytes(),
                )
                .unwrap();
                io::Write::write_all(&mut set, data).unwrap();
                io::Write::write_all(&mut set, "\r\n".as_bytes()).unwrap();
                set
            }
            Command::Touch(key, ttl) => format!("touch {} {}\r\n", key, ttl).into_bytes(),
            Command::Version => "version\r\n".to_owned().into_bytes(),
        };

        buf
    }
}

pub struct Memcached {
    read: Lines<BufReader<Box<dyn AsyncRead + Send + Sync + Unpin>>>,
    write: BufWriter<Box<dyn AsyncWrite + Send + Sync + Unpin>>,
}

impl Memcached {
    pub fn new<R, W>(read: R, write: W) -> Self
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
        W: AsyncWrite + Send + Sync + Unpin + 'static,
    {
        // Need to set the types here explicitly otherwise they get inferred as `BufReader<Box<R>>`
        let buf_reader: BufReader<Box<dyn AsyncRead + Send + Sync + Unpin>> = BufReader::new(Box::new(read));
        let buf_writer: BufWriter<Box<dyn AsyncWrite + Send + Sync + Unpin>> = BufWriter::new(Box::new(write));

        Memcached {
            read: buf_reader.lines(),
            write: buf_writer,
        }
    }

    /// Get a `Stats` object with the current values of the interesting stats for the server.
    pub async fn stats(&mut self) -> Result<Stats, MtopError> {
        self.send(Command::Stats).await?;
        let mut raw = HashMap::new();

        // Collect each of the returned `STAT` lines into key-value pairs and create
        // a single Stats object from them with each of the expected fields.
        loop {
            let line = self.read.next_line().await?;
            match line.as_deref() {
                Some("END") | None => break,
                Some(v) => {
                    let (key, val) = Self::parse_stat_line(v)?;
                    raw.insert(key, val);
                }
            }
        }

        Stats::try_from(&raw)
    }

    fn parse_stat_line(line: &str) -> Result<(String, String), MtopError> {
        let mut parts = line.splitn(3, ' ');

        match (parts.next(), parts.next(), parts.next()) {
            (Some("STAT"), Some(key), Some(val)) => Ok((key.to_owned(), val.to_owned())),
            _ => {
                if let Some(err) = Self::parse_error(line) {
                    Err(MtopError::from(err))
                } else {
                    Err(MtopError::internal(format!("unable to parse '{}'", line)))
                }
            }
        }
    }

    /// Get a `Meta` object for every item in the cache which includes its key and expiration
    /// time as a UNIX timestamp. Expiration time will be `-1` if the item was set with an
    /// infinite TTL.
    pub async fn metas(&mut self) -> Result<Vec<Meta>, MtopError> {
        self.send(Command::CrawlerMetadump).await?;
        let mut out = Vec::new();
        let mut raw = HashMap::new();

        loop {
            let line = self.read.next_line().await?;
            match line.as_deref() {
                Some("END") | None => break,
                Some(v) => {
                    // Check for an error first because the `metadump` command doesn't
                    // have any sort of prefix for each result line like `STAT` or `VALUE`
                    // so it's hard to know if it's valid without looking for an error.
                    if let Some(err) = Self::parse_error(v) {
                        return Err(MtopError::from(err));
                    }

                    let item = Self::parse_crawler_meta(v, &mut raw)?;
                    out.push(item);
                }
            }
        }

        Ok(out)
    }

    fn parse_crawler_meta(line: &str, raw: &mut HashMap<String, String>) -> Result<Meta, MtopError> {
        // Avoid allocating a new HashMap to parse every meta entry just to throw it away
        raw.clear();

        for p in line.split(' ') {
            let (key, val) = p
                .split_once('=')
                .ok_or_else(|| MtopError::internal(format!("unexpected metadump format '{}'", line)))?;
            let decoded = urlencoding::decode(val)
                .map_err(|e| MtopError::internal_cause(format!("unexpected metadump encoding '{}'", line), e))?;

            raw.insert(key.to_owned(), decoded.into_owned());
        }

        Meta::try_from(raw.deref())
    }

    /// Get a map of the requested keys and their corresponding `Value` in the cache
    /// including the key, flags, and data.
    pub async fn get(&mut self, keys: &[String]) -> Result<HashMap<String, Value>, MtopError> {
        if keys.is_empty() {
            return Err(MtopError::internal("missing required keys"));
        }

        self.send(Command::Gets(keys)).await?;
        let mut out = HashMap::with_capacity(keys.len());

        loop {
            let line = self.read.next_line().await?;
            match line.as_deref() {
                Some("END") | None => break,
                Some(v) => {
                    let value = self.parse_gets_value(v).await?;
                    out.insert(value.key.clone(), value);
                }
            }
        }

        Ok(out)
    }

    async fn parse_gets_value(&mut self, line: &str) -> Result<Value, MtopError> {
        let mut parts = line.splitn(5, ' ');

        match (parts.next(), parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some("VALUE"), Some(k), Some(flags), Some(len), Some(cas)) => {
                let flags: u64 = parse_value(flags, line)?;
                let len: u64 = parse_value(len, line)?;
                let cas: u64 = parse_value(cas, line)?;

                // Two extra bytes to read the trailing \r\n but then truncate them.
                let mut data = Vec::with_capacity(len as usize + 2);
                let reader = self.read.get_mut();
                reader.take(len + 2).read_to_end(&mut data).await?;
                data.truncate(len as usize);

                Ok(Value {
                    key: k.to_owned(),
                    flags,
                    cas,
                    data,
                })
            }
            _ => {
                // Response doesn't look like a `VALUE` line, see if the server has
                // responded with an error that we can parse. Otherwise, consider this
                // an internal error.
                if let Some(err) = Self::parse_error(line) {
                    Err(MtopError::from(err))
                } else {
                    Err(MtopError::internal(format!("unable to parse '{}'", line)))
                }
            }
        }
    }

    /// Send a simple command to verify our connection to the server is working.
    pub async fn ping(&mut self) -> Result<(), MtopError> {
        self.send(Command::Version).await?;
        if let Some(v) = self.read.next_line().await? {
            if let Some(e) = Self::parse_error(&v) {
                return Err(MtopError::from(e));
            }
        }

        Ok(())
    }

    /// Store the provided item in the cache, regardless of whether it already exists.
    pub async fn set(&mut self, key: String, flags: u64, ttl: u32, data: Vec<u8>) -> Result<(), MtopError> {
        self.send(Command::Set(&key, flags, ttl, &data)).await?;
        if let Some(v) = self.read.next_line().await? {
            Self::parse_simple_response(&v, "STORED")
        } else {
            Err(MtopError::internal("unexpected empty response"))
        }
    }

    /// Update the TTL of an item in the cache if it exists, return an error otherwise.
    pub async fn touch(&mut self, key: String, ttl: u32) -> Result<(), MtopError> {
        self.send(Command::Touch(&key, ttl)).await?;
        if let Some(v) = self.read.next_line().await? {
            Self::parse_simple_response(&v, "TOUCHED")
        } else {
            Err(MtopError::internal("unexpected empty response"))
        }
    }

    /// Delete an item in the cache if it exists, return an error otherwise.
    pub async fn delete(&mut self, key: String) -> Result<(), MtopError> {
        self.send(Command::Delete(&key)).await?;
        if let Some(v) = self.read.next_line().await? {
            Self::parse_simple_response(&v, "DELETED")
        } else {
            Err(MtopError::internal("unexpected empty response"))
        }
    }

    fn parse_simple_response(line: &str, expected: &str) -> Result<(), MtopError> {
        if line == expected {
            Ok(())
        } else if let Some(err) = Self::parse_error(line) {
            Err(MtopError::from(err))
        } else {
            Err(MtopError::internal(format!("unable to parse '{}'", line)))
        }
    }

    fn parse_error(line: &str) -> Option<ProtocolError> {
        let mut values = line.splitn(2, ' ');
        let (kind, message) = match (values.next(), values.next()) {
            (Some("BADCLASS"), Some(msg)) => (ProtocolErrorKind::BadClass, Some(msg.to_owned())),
            (Some("BUSY"), Some(msg)) => (ProtocolErrorKind::Busy, Some(msg.to_owned())),
            (Some("CLIENT_ERROR"), Some(msg)) => (ProtocolErrorKind::Client, Some(msg.to_owned())),
            (Some("ERROR"), None) => (ProtocolErrorKind::Syntax, None),
            (Some("ERROR"), Some(msg)) => (ProtocolErrorKind::Syntax, Some(msg.to_owned())),
            (Some("NOT_FOUND"), None) => (ProtocolErrorKind::NotFound, None),
            (Some("SERVER_ERROR"), Some(msg)) => (ProtocolErrorKind::Server, Some(msg.to_owned())),

            _ => return None,
        };

        Some(ProtocolError { kind, message })
    }

    async fn send<'a>(&'a mut self, cmd: Command<'a>) -> Result<(), MtopError> {
        let cmd_bytes: Vec<u8> = cmd.into();
        self.write.write_all(&cmd_bytes).await?;
        Ok(self.write.flush().await?)
    }
}

impl fmt::Debug for Memcached {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Memcached {{ read: <...>, write: <...> }}")
    }
}
