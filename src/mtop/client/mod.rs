use std::cmp::Ordering;
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::io;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter, Lines};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

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

impl TryFrom<HashMap<String, String>> for Stats {
    type Error = MtopError;

    fn try_from(value: HashMap<String, String>) -> Result<Self, Self::Error> {
        Ok(Stats {
            pid: parse_field("pid", &value)?,
            uptime: parse_field("uptime", &value)?,
            server_time: parse_field("time", &value)?,
            version: parse_field("version", &value)?,

            rusage_user: parse_field("rusage_user", &value)?,
            rusage_system: parse_field("rusage_system", &value)?,

            max_connections: parse_field("max_connections", &value)?,
            curr_connections: parse_field("curr_connections", &value)?,
            total_connections: parse_field("total_connections", &value)?,
            rejected_connections: parse_field("rejected_connections", &value)?,

            cmd_get: parse_field("cmd_get", &value)?,
            cmd_set: parse_field("cmd_set", &value)?,
            cmd_flush: parse_field("cmd_flush", &value)?,
            cmd_touch: parse_field("cmd_touch", &value)?,
            cmd_meta: parse_field("cmd_meta", &value)?,

            get_hits: parse_field("get_hits", &value)?,
            get_misses: parse_field("get_misses", &value)?,
            get_expired: parse_field("get_expired", &value)?,
            get_flushed: parse_field("get_flushed", &value)?,

            store_too_large: parse_field("store_too_large", &value)?,
            store_no_memory: parse_field("store_no_memory", &value)?,

            delete_hits: parse_field("delete_hits", &value)?,
            delete_misses: parse_field("delete_misses", &value)?,

            incr_hits: parse_field("incr_hits", &value)?,
            incr_misses: parse_field("incr_misses", &value)?,

            decr_hits: parse_field("decr_hits", &value)?,
            decr_misses: parse_field("decr_misses", &value)?,

            touch_hits: parse_field("touch_hits", &value)?,
            touch_misses: parse_field("touch_misses", &value)?,

            bytes_read: parse_field("bytes_read", &value)?,
            bytes_written: parse_field("bytes_written", &value)?,
            bytes: parse_field("bytes", &value)?,
            max_bytes: parse_field("limit_maxbytes", &value)?,

            curr_items: parse_field("curr_items", &value)?,
            total_items: parse_field("total_items", &value)?,
            evictions: parse_field("evictions", &value)?,
        })
    }
}

/// Parse the named field from a map of fields and their values or return `MtopError::Internal`
/// if it cannot be parsed.
fn parse_field<T>(key: &str, map: &HashMap<String, String>) -> Result<T, MtopError>
where
    T: FromStr,
    <T as FromStr>::Err: fmt::Display,
{
    map.get(key)
        .ok_or_else(|| MtopError::Internal(format!("field {} missing", key)))
        .and_then(|v| {
            v.parse()
                .map_err(|e| MtopError::Internal(format!("field {} value '{}': {}", key, v, e)))
        })
}

/// Parse a single value or return `MtopError::Internal` if it cannot be parsed.
fn parse_value<T>(val: &str, line: &str) -> Result<T, MtopError>
where
    T: FromStr + fmt::Display,
    <T as FromStr>::Err: fmt::Display,
{
    val.parse()
        .map_err(|e| MtopError::Internal(format!("parsing {} from '{}': {}", val, line, e)))
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

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Meta {
    pub key: String,
    pub expires: u32,
    pub last_access: u32,
}

impl TryFrom<HashMap<String, String>> for Meta {
    type Error = MtopError;

    fn try_from(value: HashMap<String, String>) -> Result<Self, Self::Error> {
        Ok(Meta {
            key: parse_field("key", &value)?,
            expires: parse_field("exp", &value)?,
            last_access: parse_field("la", &value)?,
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

#[derive(Debug)]
pub enum MtopError {
    Internal(String),
    Protocol(ProtocolError),
    IO(io::Error), // TODO: Add field here for host?
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
}

impl<'a> Command<'a> {
    fn try_into_bytes(self) -> io::Result<Vec<u8>> {
        let buf = match self {
            Self::CrawlerMetadump => "lru_crawler metadump all\r\n".to_owned().into_bytes(),
            Self::Delete(key) => format!("delete {}\r\n", key).into_bytes(),
            Self::Gets(keys) => format!("gets {}\r\n", keys.join(" ")).into_bytes(),
            Self::Stats => "stats\r\n".to_owned().into_bytes(),
            Self::Set(key, flags, ttl, data) => {
                let mut buf = Vec::new();
                io::Write::write_all(
                    &mut buf,
                    format!("set {} {} {} {}\r\n", key, flags, ttl, data.len()).as_bytes(),
                )?;
                io::Write::write_all(&mut buf, data)?;
                io::Write::write_all(&mut buf, "\r\n".as_bytes())?;
                buf
            }
            Self::Touch(key, ttl) => format!("touch {} {}\r\n", key, ttl).into_bytes(),
        };

        Ok(buf)
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

        Stats::try_from(raw)
    }

    fn parse_stat_line(line: &str) -> Result<(String, String), MtopError> {
        let mut parts = line.splitn(3, ' ');

        match (parts.next(), parts.next(), parts.next()) {
            (Some("STAT"), Some(key), Some(val)) => Ok((key.to_owned(), val.to_owned())),
            _ => {
                if let Some(err) = Self::parse_error(line) {
                    Err(MtopError::Protocol(err))
                } else {
                    Err(MtopError::Internal(format!("unable to parse '{}'", line)))
                }
            }
        }
    }

    /// Get a `Meta` object for every item in the cache which includes its key and expiration
    /// time as a UNIX timestamp.
    pub async fn metas(&mut self) -> Result<Vec<Meta>, MtopError> {
        self.send(Command::CrawlerMetadump).await?;
        let mut out = Vec::new();

        loop {
            let line = self.read.next_line().await?;
            match line.as_deref() {
                Some("END") | None => {
                    tracing::debug!("breaking on line {:?}", line);
                    break;
                }
                Some(v) => {
                    // Check for an error first because the `metadump` command doesn't
                    // have any sort of prefix for each result line like `STAT` or `VALUE`
                    // so it's hard to know if it's valid without looking for an error.
                    if let Some(err) = Self::parse_error(v) {
                        return Err(MtopError::Protocol(err));
                    }

                    let item = Self::parse_crawler_meta(v)?;
                    out.push(item);
                }
            }
        }

        Ok(out)
    }

    fn parse_crawler_meta(line: &str) -> Result<Meta, MtopError> {
        let mut raw = HashMap::new();

        for p in line.split(' ') {
            let (key, val) = p
                .split_once('=')
                .ok_or_else(|| MtopError::Internal(format!("unexpected metadump format '{}'", line)))?;
            let decoded = urlencoding::decode(val)
                .map_err(|e| MtopError::Internal(format!("unexpected metadump encoding '{}': {}", line, e)))?;

            raw.insert(key.to_owned(), decoded.into_owned());
        }

        Meta::try_from(raw)
    }

    /// Get a map of the requested keys and their corresponding `Value` in the cache
    /// including the key, flags, and data.
    pub async fn get(&mut self, keys: &[String]) -> Result<HashMap<String, Value>, MtopError> {
        if keys.is_empty() {
            return Err(MtopError::Internal("missing required keys".to_owned()));
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
                    Err(MtopError::Protocol(err))
                } else {
                    Err(MtopError::Internal(format!("unable to parse '{}'", line)))
                }
            }
        }
    }

    pub async fn set(&mut self, key: String, flags: u64, ttl: u32, data: Vec<u8>) -> Result<(), MtopError> {
        self.send(Command::Set(&key, flags, ttl, &data)).await?;
        if let Some(v) = self.read.next_line().await? {
            Self::parse_simple_response(&v, "STORED")
        } else {
            Err(MtopError::Internal("unexpected empty response".to_owned()))
        }
    }

    pub async fn touch(&mut self, key: String, ttl: u32) -> Result<(), MtopError> {
        self.send(Command::Touch(&key, ttl)).await?;
        if let Some(v) = self.read.next_line().await? {
            Self::parse_simple_response(&v, "TOUCHED")
        } else {
            Err(MtopError::Internal("unexpected empty response".to_owned()))
        }
    }

    pub async fn delete(&mut self, key: String) -> Result<(), MtopError> {
        self.send(Command::Delete(&key)).await?;
        if let Some(v) = self.read.next_line().await? {
            Self::parse_simple_response(&v, "DELETED")
        } else {
            Err(MtopError::Internal("unexpected empty response".to_owned()))
        }
    }

    fn parse_simple_response(line: &str, expected: &str) -> Result<(), MtopError> {
        if line == expected {
            Ok(())
        } else if let Some(err) = Self::parse_error(line) {
            Err(MtopError::Protocol(err))
        } else {
            Err(MtopError::Internal(format!("unable to parse '{}'", line)))
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
        let cmd_bytes = cmd.try_into_bytes()?;
        self.write.write_all(&cmd_bytes).await?;
        Ok(self.write.flush().await?)
    }
}

pub struct PooledMemcached {
    inner: Memcached,
    host: String,
}

impl Deref for PooledMemcached {
    type Target = Memcached;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PooledMemcached {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[derive(Default)]
pub struct MemcachedPool {
    clients: Mutex<HashMap<String, Memcached>>,
}

impl MemcachedPool {
    pub fn new() -> Self {
        MemcachedPool {
            clients: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get(&self, host: &str) -> Result<PooledMemcached, MtopError> {
        let mut map = self.clients.lock().await;
        let inner = match map.remove(host) {
            Some(c) => c,
            None => connect(host).await?,
        };

        Ok(PooledMemcached {
            inner,
            host: host.to_owned(),
        })
    }

    pub async fn put(&self, client: PooledMemcached) {
        let mut map = self.clients.lock().await;
        map.insert(client.host, client.inner);
    }
}

async fn connect(host: &str) -> Result<Memcached, MtopError> {
    let c = TcpStream::connect(host).await?;
    let (r, w) = c.into_split();
    Ok(Memcached::new(r, w))
}
