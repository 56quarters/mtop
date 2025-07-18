use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::error;
use std::fmt;
use std::io;
use std::ops::Deref;
use std::str::FromStr;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter, Lines};

#[derive(Debug, Default, PartialEq, Clone)]
pub struct Stats {
    // Server info
    pub pid: i64,
    pub uptime: u64,
    pub server_time: i64,
    pub threads: u64,
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
            threads: parse_field("threads", value)?,

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

#[derive(Debug, Default, PartialEq, Eq, Clone, Hash)]
pub struct Slab {
    pub id: u64,
    pub chunk_size: u64,
    pub chunks_per_page: u64,
    pub total_pages: u64,
    pub total_chunks: u64,
    pub used_chunks: u64,
    pub free_chunks: u64,
    pub get_hits: u64,
    pub cmd_set: u64,
    pub delete_hits: u64,
    pub incr_hits: u64,
    pub decr_hits: u64,
    pub cas_hits: u64,
    pub cas_badval: u64,
    pub touch_hits: u64,
}

impl PartialOrd for Slab {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Slab {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
#[repr(transparent)]
pub struct Slabs {
    slabs: Vec<Slab>,
}

impl Slabs {
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &Slab> {
        self.slabs.iter()
    }

    pub fn len(&self) -> usize {
        self.slabs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slabs.is_empty()
    }

    pub fn find_for_size(&self, size: u64) -> Option<&Slab> {
        // Find the slab with an appropriate chunk size for an item with the given
        // size. If there is no slab with a chunk size that fits the item, return the
        // last (hence largest) slab class since this is what Memcached does internally.
        self.slabs
            .get(self.slabs.partition_point(|s| s.chunk_size < size))
            .or_else(|| self.slabs.last())
    }
}

impl IntoIterator for Slabs {
    type Item = Slab;
    type IntoIter = std::vec::IntoIter<Slab>;

    fn into_iter(self) -> Self::IntoIter {
        self.slabs.into_iter()
    }
}

impl TryFrom<&HashMap<String, String>> for Slabs {
    type Error = MtopError;

    fn try_from(value: &HashMap<String, String>) -> Result<Self, Self::Error> {
        // Parse the slab IDs from each of the raw stats. We have to do this because
        // Memcached isn't guaranteed to use a particular slab ID if there are no items
        // to store in that size class. Otherwise, we could just loop from one to
        // $active_slabs + 1.
        let mut ids = BTreeSet::new();
        for k in value.keys() {
            let key_id: Option<u64> = k.split_once(':').map(|(raw, _rest)| raw).and_then(|raw| raw.parse().ok());

            if let Some(id) = key_id {
                ids.insert(id);
            }
        }

        let mut slabs = Vec::with_capacity(ids.len());

        for id in ids {
            slabs.push(Slab {
                id,
                chunk_size: parse_field(&format!("{}:chunk_size", id), value)?,
                chunks_per_page: parse_field(&format!("{}:chunks_per_page", id), value)?,
                total_pages: parse_field(&format!("{}:total_pages", id), value)?,
                total_chunks: parse_field(&format!("{}:total_chunks", id), value)?,
                used_chunks: parse_field(&format!("{}:used_chunks", id), value)?,
                free_chunks: parse_field(&format!("{}:free_chunks", id), value)?,
                get_hits: parse_field(&format!("{}:get_hits", id), value)?,
                cmd_set: parse_field(&format!("{}:cmd_set", id), value)?,
                delete_hits: parse_field(&format!("{}:delete_hits", id), value)?,
                incr_hits: parse_field(&format!("{}:incr_hits", id), value)?,
                decr_hits: parse_field(&format!("{}:decr_hits", id), value)?,
                cas_hits: parse_field(&format!("{}:cas_hits", id), value)?,
                cas_badval: parse_field(&format!("{}:cas_badval", id), value)?,
                touch_hits: parse_field(&format!("{}:touch_hits", id), value)?,
            })
        }

        Ok(Self { slabs })
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Hash)]
pub struct SlabItem {
    pub id: u64,
    pub number: u64,
    pub number_hot: u64,
    pub number_warm: u64,
    pub number_cold: u64,
    pub age_hot: u64,
    pub age_warm: u64,
    pub age: u64,
    pub mem_requested: u64,
    pub evicted: u64,
    pub evicted_nonzero: u64,
    pub evicted_time: u64,
    pub out_of_memory: u64,
    pub tail_repairs: u64,
    pub reclaimed: u64,
    pub expired_unfetched: u64,
    pub evicted_unfetched: u64,
    pub evicted_active: u64,
    pub crawler_reclaimed: u64,
    pub crawler_items_checked: u64,
    pub lrutail_reflocked: u64,
    pub moves_to_cold: u64,
    pub moves_to_warm: u64,
    pub moves_within_lru: u64,
    pub direct_reclaims: u64,
    pub hits_to_hot: u64,
    pub hits_to_warm: u64,
    pub hits_to_cold: u64,
    pub hits_to_temp: u64,
}

impl PartialOrd for SlabItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SlabItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Hash)]
#[repr(transparent)]
pub struct SlabItems {
    items: Vec<SlabItem>,
}

impl SlabItems {
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &SlabItem> {
        self.items.iter()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl IntoIterator for SlabItems {
    type Item = SlabItem;
    type IntoIter = std::vec::IntoIter<SlabItem>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

impl TryFrom<&HashMap<String, String>> for SlabItems {
    type Error = MtopError;

    fn try_from(value: &HashMap<String, String>) -> Result<Self, Self::Error> {
        // Parse the slab IDs from each of the raw stats. We have to do this because
        // Memcached isn't guaranteed to use a particular slab ID if there are no items
        // to store in that size class. Otherwise, we could just loop from one to
        // $active_slabs + 1.
        let mut ids = BTreeSet::new();
        for k in value.keys() {
            let key_id: Option<u64> = k
                .trim_start_matches("items:")
                .split_once(':')
                .map(|(raw, _rest)| raw)
                .and_then(|raw| raw.parse().ok());

            if let Some(id) = key_id {
                ids.insert(id);
            }
        }

        let mut items = Vec::with_capacity(ids.len());

        for id in ids {
            items.push(SlabItem {
                id,
                number: parse_field(&format!("items:{}:number", id), value)?,
                number_hot: parse_field(&format!("items:{}:number_hot", id), value)?,
                number_warm: parse_field(&format!("items:{}:number_warm", id), value)?,
                number_cold: parse_field(&format!("items:{}:number_cold", id), value)?,
                age_hot: parse_field(&format!("items:{}:age_hot", id), value)?,
                age_warm: parse_field(&format!("items:{}:age_warm", id), value)?,
                age: parse_field(&format!("items:{}:age", id), value)?,
                mem_requested: parse_field(&format!("items:{}:mem_requested", id), value)?,
                evicted: parse_field(&format!("items:{}:evicted", id), value)?,
                evicted_nonzero: parse_field(&format!("items:{}:evicted_nonzero", id), value)?,
                evicted_time: parse_field(&format!("items:{}:evicted_time", id), value)?,
                out_of_memory: parse_field(&format!("items:{}:outofmemory", id), value)?,
                tail_repairs: parse_field(&format!("items:{}:tailrepairs", id), value)?,
                reclaimed: parse_field(&format!("items:{}:reclaimed", id), value)?,
                expired_unfetched: parse_field(&format!("items:{}:expired_unfetched", id), value)?,
                evicted_unfetched: parse_field(&format!("items:{}:evicted_unfetched", id), value)?,
                evicted_active: parse_field(&format!("items:{}:evicted_active", id), value)?,
                crawler_reclaimed: parse_field(&format!("items:{}:crawler_reclaimed", id), value)?,
                crawler_items_checked: parse_field(&format!("items:{}:crawler_items_checked", id), value)?,
                lrutail_reflocked: parse_field(&format!("items:{}:lrutail_reflocked", id), value)?,
                moves_to_cold: parse_field(&format!("items:{}:moves_to_cold", id), value)?,
                moves_to_warm: parse_field(&format!("items:{}:moves_to_warm", id), value)?,
                moves_within_lru: parse_field(&format!("items:{}:moves_within_lru", id), value)?,
                direct_reclaims: parse_field(&format!("items:{}:direct_reclaims", id), value)?,
                hits_to_hot: parse_field(&format!("items:{}:hits_to_hot", id), value)?,
                hits_to_warm: parse_field(&format!("items:{}:hits_to_warm", id), value)?,
                hits_to_cold: parse_field(&format!("items:{}:hits_to_cold", id), value)?,
                hits_to_temp: parse_field(&format!("items:{}:hits_to_temp", id), value)?,
            })
        }

        Ok(Self { items })
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Hash)]
pub struct Meta {
    pub key: String,
    pub expires: i64,
    pub size: u64,
}

impl Meta {
    // Meta information is returned from the server as multiple key-value pairs per
    // line. We only care about a subset of those keys. Define them here to avoid doing
    // extra work when parsing the server response.
    const KEYS: &'static [&'static str] = &["key", "exp", "size"];
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
        Some(self.cmp(other))
    }
}

impl Ord for Meta {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Hash)]
pub struct Value {
    pub key: String,
    pub cas: u64,
    pub flags: u64,
    pub data: Vec<u8>,
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
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
        .ok_or_else(|| MtopError::runtime(format!("field {} missing", key)))
        .and_then(|v| {
            v.parse()
                .map_err(|e| MtopError::runtime_cause(format!("field {} value '{}'", key, v), e))
        })
}

fn parse_value<T>(val: &str, line: &str) -> Result<T, MtopError>
where
    T: FromStr + fmt::Display,
    <T as FromStr>::Err: fmt::Display + Send + Sync + error::Error + 'static,
{
    val.parse()
        .map_err(|e| MtopError::runtime_cause(format!("parsing {} from '{}'", val, line), e))
}

#[derive(Debug, PartialOrd, PartialEq, Copy, Clone)]
pub enum ErrorKind {
    Runtime,
    IO,
    Protocol,
    Configuration,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime => write!(f, "runtime error"),
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
    pub fn runtime<S>(msg: S) -> MtopError
    where
        S: Into<String>,
    {
        MtopError {
            kind: ErrorKind::Runtime,
            repr: ErrorRepr::Message(msg.into()),
        }
    }

    pub fn runtime_cause<S, E>(msg: S, e: E) -> MtopError
    where
        S: Into<String>,
        E: error::Error + Send + Sync + 'static,
    {
        MtopError {
            kind: ErrorKind::Runtime,
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

    pub fn timeout<D>(t: Duration, operation: D) -> MtopError
    where
        D: fmt::Display,
    {
        MtopError {
            kind: ErrorKind::IO,
            repr: ErrorRepr::Message(format!("operation {} timed out after {:?}", operation, t)),
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

impl From<(String, io::Error)> for MtopError {
    fn from((s, e): (String, io::Error)) -> Self {
        MtopError {
            kind: ErrorKind::IO,
            repr: ErrorRepr::MessageCause(s, Box::new(e)),
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
    NotStored,
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
            Self::NotStored => "NOT_STORED".fmt(f),
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

impl ProtocolError {
    pub fn kind(&self) -> ProtocolErrorKind {
        self.kind
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
enum Command<'a> {
    Add(&'a Key, u64, u32, &'a [u8]),
    CrawlerMetadump,
    Decr(&'a Key, u64),
    Delete(&'a Key),
    FlushAll,
    FlushAllWait(u64),
    Gets(&'a [Key]),
    Incr(&'a Key, u64),
    Replace(&'a Key, u64, u32, &'a [u8]),
    Stats,
    StatsItems,
    StatsSlabs,
    Set(&'a Key, u64, u32, &'a [u8]),
    Touch(&'a Key, u32),
    Version,
}

impl<'a> From<Command<'a>> for Vec<u8> {
    fn from(value: Command<'a>) -> Self {
        match value {
            Command::Add(key, flags, ttl, data) => storage_command("add", key, flags, ttl, data),
            Command::CrawlerMetadump => "lru_crawler metadump hash\r\n".to_owned().into_bytes(),
            Command::Decr(key, delta) => format!("decr {} {}\r\n", key, delta).into_bytes(),
            Command::Delete(key) => format!("delete {}\r\n", key).into_bytes(),
            Command::FlushAll => "flush_all\r\n".to_owned().into_bytes(),
            Command::FlushAllWait(wait) => format!("flush_all {}\r\n", wait).into_bytes(),
            Command::Gets(keys) => format!("gets {}\r\n", keys.join(" ")).into_bytes(),
            Command::Incr(key, delta) => format!("incr {} {}\r\n", key, delta).into_bytes(),
            Command::Replace(key, flags, ttl, data) => storage_command("replace", key, flags, ttl, data),
            Command::Stats => "stats\r\n".to_owned().into_bytes(),
            Command::StatsItems => "stats items\r\n".to_owned().into_bytes(),
            Command::StatsSlabs => "stats slabs\r\n".to_owned().into_bytes(),
            Command::Set(key, flags, ttl, data) => storage_command("set", key, flags, ttl, data),
            Command::Touch(key, ttl) => format!("touch {} {}\r\n", key, ttl).into_bytes(),
            Command::Version => "version\r\n".to_owned().into_bytes(),
        }
    }
}

fn storage_command(verb: &str, key: &Key, flags: u64, ttl: u32, data: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(key.len() + data.len() + 32);
    io::Write::write_all(
        &mut bytes,
        format!("{} {} {} {} {}\r\n", verb, key, flags, ttl, data.len()).as_bytes(),
    )
    .unwrap();
    io::Write::write_all(&mut bytes, data).unwrap();
    io::Write::write_all(&mut bytes, "\r\n".as_bytes()).unwrap();
    bytes
}

pub struct Memcached {
    read: Lines<BufReader<Box<dyn AsyncRead + Send + Sync + Unpin>>>,
    write: BufWriter<Box<dyn AsyncWrite + Send + Sync + Unpin>>,
}

impl Memcached {
    const MAX_PAYLOAD_SIZE: u64 = 1024 * 1024 * 1024;

    pub fn new<R, W>(read: R, write: W) -> Self
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
        W: AsyncWrite + Send + Sync + Unpin + 'static,
    {
        Memcached {
            read: BufReader::<Box<dyn AsyncRead + Send + Sync + Unpin>>::new(Box::new(read)).lines(),
            write: BufWriter::new(Box::new(write)),
        }
    }

    /// Get a `Stats` object with the current values of the interesting stats for the server.
    pub async fn stats(&mut self) -> Result<Stats, MtopError> {
        self.send(Command::Stats).await?;
        let raw = self.read_stats_response().await?;
        Stats::try_from(&raw)
    }

    /// Get a `Slabs` object with information about each set of `Slab`s maintained by
    /// the Memcached server. You can think of each `Slab` as a class of objects that
    /// are stored together in memory. Note that `Slab` IDs may not be contiguous based
    /// on the size of items actually stored by the server.
    pub async fn slabs(&mut self) -> Result<Slabs, MtopError> {
        self.send(Command::StatsSlabs).await?;
        let raw = self.read_stats_response().await?;
        Slabs::try_from(&raw)
    }

    /// Get a `SlabsItems` object with information about the `SlabItem` items stored in
    /// each slab class maintained by the Memcached server. The ID of each `SlabItem`
    /// corresponds to a `Slab` maintained by the server. Note that `SlabItem` IDs may
    /// not be contiguous based on the size of items actually stored by the server.
    pub async fn items(&mut self) -> Result<SlabItems, MtopError> {
        self.send(Command::StatsItems).await?;
        let raw = self.read_stats_response().await?;
        SlabItems::try_from(&raw)
    }

    async fn read_stats_response(&mut self) -> Result<HashMap<String, String>, MtopError> {
        let mut out = HashMap::new();

        while let Some(v) = self.read.next_line().await? {
            if v == "END" {
                break;
            }

            let (key, val) = Self::parse_stat_line(&v)?;
            out.insert(key.to_owned(), val.to_owned());
        }

        Ok(out)
    }

    fn parse_stat_line(line: &str) -> Result<(&str, &str), MtopError> {
        let mut parts = line.splitn(3, ' ');
        match (parts.next(), parts.next(), parts.next()) {
            (Some("STAT"), Some(key), Some(val)) => Ok((key, val)),
            _ => {
                if let Some(err) = Self::parse_error(line) {
                    Err(MtopError::from(err))
                } else {
                    Err(MtopError::runtime(format!("unable to parse '{}'", line)))
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

        while let Some(v) = self.read.next_line().await? {
            if v == "END" {
                break;
            }

            // Check for an error first because the `metadump` command doesn't
            // have any sort of prefix for each result line like `STAT` or `VALUE`
            // so it's hard to know if it's valid without looking for an error.
            if let Some(err) = Self::parse_error(&v) {
                return Err(MtopError::from(err));
            }

            let item = Self::parse_crawler_meta(&v, Meta::KEYS, &mut raw)?;
            out.push(item);
        }

        Ok(out)
    }

    fn parse_crawler_meta(line: &str, keys: &[&str], raw: &mut HashMap<String, String>) -> Result<Meta, MtopError> {
        // Avoid allocating a new HashMap to parse every meta entry just to throw it away
        raw.clear();

        for p in line.split(' ') {
            let (key, val) = p
                .split_once('=')
                .ok_or_else(|| MtopError::runtime(format!("unexpected metadump format '{}'", line)))?;

            // Avoid spending time decoding values or allocating for data we don't care about.
            // Use a slice here since it's faster than a HashSet when the number of entries is
            // small and the number of keys we're searching through is always small.
            if !keys.contains(&key) {
                continue;
            }

            let decoded = urlencoding::decode(val)
                .map_err(|e| MtopError::runtime_cause(format!("unexpected metadump encoding '{}'", line), e))?;
            raw.insert(key.to_owned(), decoded.into_owned());
        }

        Meta::try_from(raw.deref())
    }

    /// Send a simple command to verify our connection to the server is working.
    pub async fn ping(&mut self) -> Result<(), MtopError> {
        self.send(Command::Version).await?;
        if let Some(v) = self.read.next_line().await? {
            if let Some(e) = Self::parse_error(&v) {
                return Err(MtopError::from(e));
            }
            if !v.starts_with("VERSION") {
                return Err(MtopError::runtime(format!("unable to parse '{}'", v)));
            }
        }

        Ok(())
    }

    /// Flush all entries in the cache, optionally after a delay. When a delay is used, the
    /// server will flush entries after a delay but the call will still return immediately.
    pub async fn flush_all(&mut self, wait: Option<Duration>) -> Result<(), MtopError> {
        let cmd = if let Some(d) = wait {
            Command::FlushAllWait(d.as_secs())
        } else {
            Command::FlushAll
        };

        self.send(cmd).await?;
        self.read_simple_response("OK").await
    }

    /// Get a map of the requested keys and their corresponding `Value` in the cache
    /// including the key, flags, and data.
    pub async fn get(&mut self, keys: &[Key]) -> Result<HashMap<String, Value>, MtopError> {
        self.send(Command::Gets(keys)).await?;
        let mut out = HashMap::with_capacity(keys.len());

        while let Some(v) = self.read.next_line().await? {
            if v == "END" {
                break;
            }

            let value = self.parse_gets_value(&v).await?;
            out.insert(value.key.clone(), value);
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

                // The max size of an object in Memcached is represented with a `u64` which
                // means it's basically infinite. In practice the default max size of an object
                // in a Memcached server is 1MB but can be configured higher. Place a limit on
                // the size that we'll accept here to avoid a denial of service from bad lengths.
                if len > Self::MAX_PAYLOAD_SIZE {
                    return Err(MtopError::runtime(format!(
                        "server response of length {} exceeds client max of {}",
                        len,
                        Self::MAX_PAYLOAD_SIZE
                    )));
                }

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
                    Err(MtopError::runtime(format!("unable to parse '{}'", line)))
                }
            }
        }
    }

    /// Increment the value of a key by the given delta if the value is numeric returning
    /// the new value. Returns an error if the value is _not_ numeric.
    pub async fn incr(&mut self, key: &Key, delta: u64) -> Result<u64, MtopError> {
        self.send(Command::Incr(key, delta)).await?;
        if let Some(v) = self.read.next_line().await? {
            Self::parse_numeric_response(&v)
        } else {
            Err(MtopError::runtime("unexpected empty response"))
        }
    }

    /// Decrement the value of a key by the given delta if the value is numeric returning
    /// the new value with a minimum of 0. Returns an error if the value is _not_ numeric.
    pub async fn decr(&mut self, key: &Key, delta: u64) -> Result<u64, MtopError> {
        self.send(Command::Decr(key, delta)).await?;
        if let Some(v) = self.read.next_line().await? {
            Self::parse_numeric_response(&v)
        } else {
            Err(MtopError::runtime("unexpected empty response"))
        }
    }

    fn parse_numeric_response(line: &str) -> Result<u64, MtopError> {
        if let Some(err) = Self::parse_error(line) {
            Err(MtopError::from(err))
        } else {
            line.parse()
                .map_err(|_e| MtopError::runtime(format!("unable to parse '{}'", line)))
        }
    }

    /// Store the provided item in the cache, regardless of whether it already exists.
    pub async fn set<V>(&mut self, key: &Key, flags: u64, ttl: u32, data: V) -> Result<(), MtopError>
    where
        V: AsRef<[u8]>,
    {
        self.send(Command::Set(key, flags, ttl, data.as_ref())).await?;
        self.read_simple_response("STORED").await
    }

    /// Store the provided item in the cache only if it does not already exist.
    pub async fn add<V>(&mut self, key: &Key, flags: u64, ttl: u32, data: V) -> Result<(), MtopError>
    where
        V: AsRef<[u8]>,
    {
        self.send(Command::Add(key, flags, ttl, data.as_ref())).await?;
        self.read_simple_response("STORED").await
    }

    /// Store the provided item in the cache only if it already exists.
    pub async fn replace<V>(&mut self, key: &Key, flags: u64, ttl: u32, data: V) -> Result<(), MtopError>
    where
        V: AsRef<[u8]>,
    {
        self.send(Command::Replace(key, flags, ttl, data.as_ref())).await?;
        self.read_simple_response("STORED").await
    }

    /// Update the TTL of an item in the cache if it exists, return an error otherwise.
    pub async fn touch(&mut self, key: &Key, ttl: u32) -> Result<(), MtopError> {
        self.send(Command::Touch(key, ttl)).await?;
        self.read_simple_response("TOUCHED").await
    }

    /// Delete an item in the cache if it exists, return an error otherwise.
    pub async fn delete(&mut self, key: &Key) -> Result<(), MtopError> {
        self.send(Command::Delete(key)).await?;
        self.read_simple_response("DELETED").await
    }

    async fn read_simple_response(&mut self, expected: &str) -> Result<(), MtopError> {
        match self.read.next_line().await? {
            Some(line) if line == expected => Ok(()),
            Some(line) => {
                if let Some(err) = Self::parse_error(&line) {
                    Err(MtopError::from(err))
                } else {
                    Err(MtopError::runtime(format!("unable to parse '{}'", line)))
                }
            }
            None => Err(MtopError::runtime("unexpected empty response")),
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
            (Some("NOT_STORED"), None) => (ProtocolErrorKind::NotStored, None),
            (Some("SERVER_ERROR"), Some(msg)) => (ProtocolErrorKind::Server, Some(msg.to_owned())),

            _ => return None,
        };

        Some(ProtocolError { kind, message })
    }

    async fn send(&mut self, cmd: Command<'_>) -> Result<(), MtopError> {
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

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[repr(transparent)]
pub struct Key(String);

impl Key {
    const MAX_LENGTH: usize = 250;

    pub fn one<T>(val: T) -> Result<Key, MtopError>
    where
        T: Into<String>,
    {
        let val = val.into();
        if !Self::is_legal_val(&val) {
            Err(MtopError::runtime(format!("invalid key {}", val)))
        } else {
            Ok(Key(val))
        }
    }

    pub fn many<I, T>(vals: I) -> Result<Vec<Key>, MtopError>
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let iter = vals.into_iter();
        let (sz, _) = iter.size_hint();
        let mut out = Vec::with_capacity(sz);

        for val in iter {
            out.push(Self::one(val)?);
        }

        Ok(out)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn is_legal_val(val: &str) -> bool {
        if val.len() > Self::MAX_LENGTH {
            return false;
        }

        for c in val.chars() {
            if !c.is_ascii() || c.is_ascii_whitespace() || c.is_ascii_control() {
                return false;
            }
        }

        true
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<str> for Key {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for Key {
    fn borrow(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod test {
    use super::{ErrorKind, Key, Memcached, Meta, Slab, SlabItem, SlabItems};
    use std::io::{Cursor, Error};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::AsyncWrite;
    use tokio::sync::mpsc::{self, UnboundedSender};

    /////////
    // key //
    /////////

    #[test]
    fn test_key_one_length() {
        let val = "abc".repeat(Key::MAX_LENGTH);
        let res = Key::one(val);
        assert!(res.is_err());
    }

    #[test]
    fn test_key_one_non_ascii() {
        let val = "🤦";
        let res = Key::one(val);
        assert!(res.is_err());
    }

    #[test]
    fn test_key_one_whitespace() {
        let val = "some thing";
        let res = Key::one(val);
        assert!(res.is_err());
    }

    #[test]
    fn test_key_one_control_char() {
        let val = "\x7F";
        let res = Key::one(val);
        assert!(res.is_err());
    }

    #[test]
    fn test_key_one_success() {
        let val = "a-reasonable-key";
        let res = Key::one(val);
        assert!(res.is_ok());
    }

    struct WriteAdapter {
        tx: UnboundedSender<Vec<u8>>,
    }

    impl AsyncWrite for WriteAdapter {
        fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Error>> {
            self.tx.send(buf.to_owned()).unwrap();
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Create a new receiver channel and `Memcached` instance to read the provided server
    /// response. Anything written by the client is able to be read from the receiver channel.
    /// NOTE that it is important that the receiver not be dropped by the caller since this will
    /// cause writes to the channel to fail from within the client.
    macro_rules! client {
        () => ({
            let (tx, rx) = mpsc::unbounded_channel();
            let reads = Vec::new();
            (rx, Memcached::new(Cursor::new(reads), WriteAdapter { tx }))
        });
        ($($line:expr),+ $(,)?) => ({
            let (tx, rx) = mpsc::unbounded_channel();
            let mut reads = Vec::new();
            $(reads.extend_from_slice($line.as_bytes());)+
            (rx, Memcached::new(Cursor::new(reads), WriteAdapter { tx }))
        })
    }

    /////////
    // get //
    /////////

    #[tokio::test]
    async fn test_memcached_get_no_key() {
        let (_rx, mut client) = client!();
        let vals: Vec<String> = vec![];
        let keys = Key::many(vals).unwrap();
        let res = client.get(&keys).await.unwrap();

        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn test_memcached_get_error() {
        let (_rx, mut client) = client!("SERVER_ERROR backend failure\r\n");
        let keys = Key::many(vec!["foo", "baz"]).unwrap();
        let res = client.get(&keys).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::Protocol, err.kind());
    }

    #[tokio::test]
    async fn test_memcached_get_miss() {
        let (_rx, mut client) = client!("END\r\n");
        let keys = Key::many(vec!["foo", "baz"]).unwrap();
        let res = client.get(&keys).await.unwrap();

        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn test_memcached_get_hit() {
        let (_rx, mut client) = client!(
            "VALUE foo 32 3 1\r\n",
            "bar\r\n",
            "VALUE baz 64 3 2\r\n",
            "qux\r\n",
            "END\r\n",
        );
        let keys = Key::many(vec!["foo", "baz"]).unwrap();
        let res = client.get(&keys).await.unwrap();

        let val1 = res.get("foo").unwrap();
        assert_eq!("foo", val1.key);
        assert_eq!("bar".as_bytes(), val1.data);
        assert_eq!(32, val1.flags);
        assert_eq!(1, val1.cas);

        let val2 = res.get("baz").unwrap();
        assert_eq!("baz", val2.key);
        assert_eq!("qux".as_bytes(), val2.data);
        assert_eq!(64, val2.flags);
        assert_eq!(2, val2.cas);
    }

    //////////
    // incr //
    //////////

    #[tokio::test]
    async fn test_memcached_incr_bad_val() {
        let (mut rx, mut client) = client!("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n");
        let key = Key::one("test").unwrap();
        let res = client.incr(&key, 2).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::Protocol, err.kind());

        let bytes = rx.recv().await.unwrap();
        let command = String::from_utf8(bytes).unwrap();
        assert_eq!("incr test 2\r\n", command);
    }

    #[tokio::test]
    async fn test_memcached_incr_success() {
        let (mut rx, mut client) = client!("3\r\n");
        let key = Key::one("test").unwrap();
        let res = client.incr(&key, 2).await.unwrap();

        assert_eq!(3, res);
        let bytes = rx.recv().await.unwrap();
        let command = String::from_utf8(bytes).unwrap();
        assert_eq!("incr test 2\r\n", command);
    }

    //////////
    // decr //
    //////////

    #[tokio::test]
    async fn test_memcached_decr_bad_val() {
        let (mut rx, mut client) = client!("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n");
        let key = Key::one("test").unwrap();
        let res = client.decr(&key, 1).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::Protocol, err.kind());

        let bytes = rx.recv().await.unwrap();
        let command = String::from_utf8(bytes).unwrap();
        assert_eq!("decr test 1\r\n", command);
    }

    #[tokio::test]
    async fn test_memcached_decr_success() {
        let (mut rx, mut client) = client!("3\r\n");
        let key = Key::one("test").unwrap();
        let res = client.decr(&key, 1).await.unwrap();

        assert_eq!(3, res);
        let bytes = rx.recv().await.unwrap();
        let command = String::from_utf8(bytes).unwrap();
        assert_eq!("decr test 1\r\n", command);
    }

    //////////
    // ping //
    //////////

    #[tokio::test]
    async fn test_memcached_ping_bad_val() {
        let (mut rx, mut client) = client!("220 localhost ESMTP Postfix\r\n");
        let res = client.ping().await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::Runtime, err.kind());

        let bytes = rx.recv().await.unwrap();
        let command = String::from_utf8(bytes).unwrap();
        assert_eq!("version\r\n", command);
    }

    #[tokio::test]
    async fn test_memcached_ping_success() {
        let (mut rx, mut client) = client!("VERSION 1.6.22\r\n");
        client.ping().await.unwrap();

        let bytes = rx.recv().await.unwrap();
        let command = String::from_utf8(bytes).unwrap();
        assert_eq!("version\r\n", command);
    }

    macro_rules! test_store_command_success {
        ($method:ident, $verb:expr) => {
            let (mut rx, mut client) = client!("STORED\r\n");
            let res = client.$method(&Key::one("test").unwrap(), 0, 300, "val".as_bytes()).await;

            assert!(res.is_ok());
            let bytes = rx.recv().await.unwrap();
            let command = String::from_utf8(bytes).unwrap();
            assert_eq!(concat!($verb, " test 0 300 3\r\nval\r\n"), command);
        };
    }

    macro_rules! test_store_command_error {
        ($method:ident, $verb:expr) => {
            let (mut rx, mut client) = client!("NOT_STORED\r\n");
            let res = client.$method(&Key::one("test").unwrap(), 0, 300, "val".as_bytes()).await;

            assert!(res.is_err());
            let err = res.unwrap_err();
            assert_eq!(ErrorKind::Protocol, err.kind());

            let bytes = rx.recv().await.unwrap();
            let command = String::from_utf8(bytes).unwrap();
            assert_eq!(concat!($verb, " test 0 300 3\r\nval\r\n"), command);
        };
    }

    /////////
    // set //
    /////////

    #[tokio::test]
    async fn test_memcached_set_success() {
        test_store_command_success!(set, "set");
    }

    #[tokio::test]
    async fn test_memcached_set_error() {
        test_store_command_error!(set, "set");
    }

    /////////
    // add //
    /////////

    #[tokio::test]
    async fn test_memcached_add_success() {
        test_store_command_success!(add, "add");
    }

    #[tokio::test]
    async fn test_memcached_add_error() {
        test_store_command_error!(add, "add");
    }

    /////////////
    // replace //
    /////////////

    #[tokio::test]
    async fn test_memcached_replace_success() {
        test_store_command_success!(replace, "replace");
    }

    #[tokio::test]
    async fn test_memcached_replace_error() {
        test_store_command_error!(replace, "replace");
    }

    ///////////
    // touch //
    ///////////

    #[tokio::test]
    async fn test_memcached_touch_success() {
        let (mut rx, mut client) = client!("TOUCHED\r\n");
        let key = Key::one("test").unwrap();
        let res = client.touch(&key, 300).await;

        assert!(res.is_ok());
        let bytes = rx.recv().await.unwrap();
        let command = String::from_utf8(bytes).unwrap();
        assert_eq!("touch test 300\r\n", command);
    }

    #[tokio::test]
    async fn test_memcached_touch_error() {
        let (mut rx, mut client) = client!("NOT_FOUND\r\n");
        let key = Key::one("test").unwrap();
        let res = client.touch(&key, 300).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::Protocol, err.kind());

        let bytes = rx.recv().await.unwrap();
        let command = String::from_utf8(bytes).unwrap();
        assert_eq!("touch test 300\r\n", command);
    }

    ////////////
    // delete //
    ////////////

    #[tokio::test]
    async fn test_memcached_delete_success() {
        let (mut rx, mut client) = client!("DELETED\r\n");
        let key = Key::one("test").unwrap();
        let res = client.delete(&key).await;

        assert!(res.is_ok());
        let bytes = rx.recv().await.unwrap();
        let command = String::from_utf8(bytes).unwrap();
        assert_eq!("delete test\r\n", command);
    }

    #[tokio::test]
    async fn test_memcached_delete_error() {
        let (mut rx, mut client) = client!("NOT_FOUND\r\n");
        let key = Key::one("test").unwrap();
        let res = client.delete(&key).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::Protocol, err.kind());

        let bytes = rx.recv().await.unwrap();
        let command = String::from_utf8(bytes).unwrap();
        assert_eq!("delete test\r\n", command);
    }

    ///////////////
    // flush_all //
    ///////////////

    #[tokio::test]
    async fn test_memcached_flush_all_with_wait_success() {
        let (mut rx, mut client) = client!("OK\r\n");
        let wait = Some(Duration::from_secs(25));
        let res = client.flush_all(wait).await;

        assert!(res.is_ok());
        let bytes = rx.recv().await.unwrap();
        let command = String::from_utf8(bytes).unwrap();
        assert_eq!("flush_all 25\r\n", command);
    }

    #[tokio::test]
    async fn test_memcached_flush_all_with_wait_error() {
        let (mut rx, mut client) = client!("ERROR\r\n");
        let wait = Some(Duration::from_secs(25));
        let res = client.flush_all(wait).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::Protocol, err.kind());

        let bytes = rx.recv().await.unwrap();
        let command = String::from_utf8(bytes).unwrap();
        assert_eq!("flush_all 25\r\n", command);
    }

    #[tokio::test]
    async fn test_memcached_flush_all_no_wait_success() {
        let (mut rx, mut client) = client!("OK\r\n");
        let res = client.flush_all(None).await;

        assert!(res.is_ok());
        let bytes = rx.recv().await.unwrap();
        let command = String::from_utf8(bytes).unwrap();
        assert_eq!("flush_all\r\n", command);
    }

    #[tokio::test]
    async fn test_memcached_flush_all_no_wait_error() {
        let (mut rx, mut client) = client!("ERROR\r\n");
        let res = client.flush_all(None).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::Protocol, err.kind());

        let bytes = rx.recv().await.unwrap();
        let command = String::from_utf8(bytes).unwrap();
        assert_eq!("flush_all\r\n", command);
    }

    ///////////
    // stats //
    ///////////

    #[tokio::test]
    async fn test_memcached_stats_empty() {
        let (_rx, mut client) = client!("END\r\n");
        let res = client.stats().await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::Runtime, err.kind());
    }

    #[tokio::test]
    async fn test_memcached_stats_error() {
        let (_rx, mut client) = client!("SERVER_ERROR backend failure\r\n");
        let res = client.stats().await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::Protocol, err.kind());
    }

    #[tokio::test]
    async fn test_memcached_stats_success() {
        let (_rx, mut client) = client!(
            "STAT pid 1525\r\n",
            "STAT uptime 271984\r\n",
            "STAT time 1687212809\r\n",
            "STAT version 1.6.14\r\n",
            "STAT libevent 2.1.12-stable\r\n",
            "STAT pointer_size 64\r\n",
            "STAT rusage_user 17.544323\r\n",
            "STAT rusage_system 11.830461\r\n",
            "STAT max_connections 1024\r\n",
            "STAT curr_connections 1\r\n",
            "STAT total_connections 3\r\n",
            "STAT rejected_connections 0\r\n",
            "STAT connection_structures 2\r\n",
            "STAT response_obj_oom 0\r\n",
            "STAT response_obj_count 1\r\n",
            "STAT response_obj_bytes 32768\r\n",
            "STAT read_buf_count 4\r\n",
            "STAT read_buf_bytes 65536\r\n",
            "STAT read_buf_bytes_free 16384\r\n",
            "STAT read_buf_oom 0\r\n",
            "STAT reserved_fds 20\r\n",
            "STAT cmd_get 1\r\n",
            "STAT cmd_set 0\r\n",
            "STAT cmd_flush 0\r\n",
            "STAT cmd_touch 0\r\n",
            "STAT cmd_meta 0\r\n",
            "STAT get_hits 0\r\n",
            "STAT get_misses 1\r\n",
            "STAT get_expired 0\r\n",
            "STAT get_flushed 0\r\n",
            "STAT delete_misses 0\r\n",
            "STAT delete_hits 0\r\n",
            "STAT incr_misses 0\r\n",
            "STAT incr_hits 0\r\n",
            "STAT decr_misses 0\r\n",
            "STAT decr_hits 0\r\n",
            "STAT cas_misses 0\r\n",
            "STAT cas_hits 0\r\n",
            "STAT cas_badval 0\r\n",
            "STAT touch_hits 0\r\n",
            "STAT touch_misses 0\r\n",
            "STAT store_too_large 0\r\n",
            "STAT store_no_memory 0\r\n",
            "STAT auth_cmds 0\r\n",
            "STAT auth_errors 0\r\n",
            "STAT bytes_read 16\r\n",
            "STAT bytes_written 7\r\n",
            "STAT limit_maxbytes 67108864\r\n",
            "STAT accepting_conns 1\r\n",
            "STAT listen_disabled_num 0\r\n",
            "STAT time_in_listen_disabled_us 0\r\n",
            "STAT threads 4\r\n",
            "STAT conn_yields 0\r\n",
            "STAT hash_power_level 16\r\n",
            "STAT hash_bytes 524288\r\n",
            "STAT hash_is_expanding 0\r\n",
            "STAT slab_reassign_rescues 0\r\n",
            "STAT slab_reassign_chunk_rescues 0\r\n",
            "STAT slab_reassign_evictions_nomem 0\r\n",
            "STAT slab_reassign_inline_reclaim 0\r\n",
            "STAT slab_reassign_busy_items 0\r\n",
            "STAT slab_reassign_busy_deletes 0\r\n",
            "STAT slab_reassign_running 0\r\n",
            "STAT slabs_moved 0\r\n",
            "STAT lru_crawler_running 0\r\n",
            "STAT lru_crawler_starts 105\r\n",
            "STAT lru_maintainer_juggles 271976\r\n",
            "STAT malloc_fails 0\r\n",
            "STAT log_worker_dropped 0\r\n",
            "STAT log_worker_written 0\r\n",
            "STAT log_watcher_skipped 0\r\n",
            "STAT log_watcher_sent 0\r\n",
            "STAT log_watchers 0\r\n",
            "STAT unexpected_napi_ids 0\r\n",
            "STAT round_robin_fallback 0\r\n",
            "STAT bytes 0\r\n",
            "STAT curr_items 0\r\n",
            "STAT total_items 0\r\n",
            "STAT slab_global_page_pool 0\r\n",
            "STAT expired_unfetched 0\r\n",
            "STAT evicted_unfetched 0\r\n",
            "STAT evicted_active 0\r\n",
            "STAT evictions 0\r\n",
            "STAT reclaimed 0\r\n",
            "STAT crawler_reclaimed 0\r\n",
            "STAT crawler_items_checked 0\r\n",
            "STAT lrutail_reflocked 0\r\n",
            "STAT moves_to_cold 0\r\n",
            "STAT moves_to_warm 0\r\n",
            "STAT moves_within_lru 0\r\n",
            "STAT direct_reclaims 0\r\n",
            "STAT lru_bumps_dropped 0\r\n",
            "END\r\n",
        );
        let res = client.stats().await.unwrap();

        assert_eq!(0, res.cmd_set);
        assert_eq!(1, res.cmd_get);
        assert_eq!(1, res.get_misses);
        assert_eq!(0, res.get_hits);
    }

    ///////////
    // slabs //
    ///////////

    #[tokio::test]
    async fn test_memcached_slabs_empty() {
        let (_rx, mut client) = client!("STAT active_slabs 0\r\n", "STAT total_malloced 0\r\n", "END\r\n");
        let res = client.slabs().await.unwrap();

        assert!(res.slabs.is_empty());
    }

    #[tokio::test]
    async fn test_memcached_slabs_error() {
        let (_rx, mut client) = client!("ERROR Too many open connections\r\n");
        let res = client.slabs().await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::Protocol, err.kind());
    }

    #[tokio::test]
    async fn test_memcached_slabs_success() {
        let (_rx, mut client) = client!(
            "STAT 6:chunk_size 304\r\n",
            "STAT 6:chunks_per_page 3449\r\n",
            "STAT 6:total_pages 1\r\n",
            "STAT 6:total_chunks 3449\r\n",
            "STAT 6:used_chunks 1\r\n",
            "STAT 6:free_chunks 3448\r\n",
            "STAT 6:free_chunks_end 0\r\n",
            "STAT 6:get_hits 951\r\n",
            "STAT 6:cmd_set 100\r\n",
            "STAT 6:delete_hits 0\r\n",
            "STAT 6:incr_hits 0\r\n",
            "STAT 6:decr_hits 0\r\n",
            "STAT 6:cas_hits 0\r\n",
            "STAT 6:cas_badval 0\r\n",
            "STAT 6:touch_hits 0\r\n",
            "STAT 7:chunk_size 384\r\n",
            "STAT 7:chunks_per_page 2730\r\n",
            "STAT 7:total_pages 1\r\n",
            "STAT 7:total_chunks 2730\r\n",
            "STAT 7:used_chunks 5\r\n",
            "STAT 7:free_chunks 2725\r\n",
            "STAT 7:free_chunks_end 0\r\n",
            "STAT 7:get_hits 4792\r\n",
            "STAT 7:cmd_set 520\r\n",
            "STAT 7:delete_hits 0\r\n",
            "STAT 7:incr_hits 0\r\n",
            "STAT 7:decr_hits 0\r\n",
            "STAT 7:cas_hits 0\r\n",
            "STAT 7:cas_badval 0\r\n",
            "STAT 7:touch_hits 0\r\n",
            "STAT active_slabs 2\r\n",
            "STAT total_malloced 30408704\r\n",
            "END\r\n",
        );
        let res = client.slabs().await.unwrap();

        let expected = vec![
            Slab {
                id: 6,
                chunk_size: 304,
                chunks_per_page: 3449,
                total_pages: 1,
                total_chunks: 3449,
                used_chunks: 1,
                free_chunks: 3448,
                get_hits: 951,
                cmd_set: 100,
                delete_hits: 0,
                incr_hits: 0,
                decr_hits: 0,
                cas_hits: 0,
                cas_badval: 0,
                touch_hits: 0,
            },
            Slab {
                id: 7,
                chunk_size: 384,
                chunks_per_page: 2730,
                total_pages: 1,
                total_chunks: 2730,
                used_chunks: 5,
                free_chunks: 2725,
                get_hits: 4792,
                cmd_set: 520,
                delete_hits: 0,
                incr_hits: 0,
                decr_hits: 0,
                cas_hits: 0,
                cas_badval: 0,
                touch_hits: 0,
            },
        ];

        assert_eq!(expected, res.slabs);
    }

    ///////////
    // items //
    ///////////

    #[tokio::test]
    async fn test_memcached_items_empty() {
        let (_rx, mut client) = client!();
        let res = client.items().await.unwrap();

        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn test_memcached_items_error() {
        let (_rx, mut client) = client!("ERROR Too many open connections\r\n");
        let res = client.items().await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::Protocol, err.kind());
    }

    #[tokio::test]
    async fn test_memcached_items_success() {
        let (_rx, mut client) = client!(
            "STAT items:39:number 3\r\n",
            "STAT items:39:number_hot 0\r\n",
            "STAT items:39:number_warm 1\r\n",
            "STAT items:39:number_cold 2\r\n",
            "STAT items:39:age_hot 0\r\n",
            "STAT items:39:age_warm 7\r\n",
            "STAT items:39:age 8\r\n",
            "STAT items:39:mem_requested 1535788\r\n",
            "STAT items:39:evicted 1646\r\n",
            "STAT items:39:evicted_nonzero 1646\r\n",
            "STAT items:39:evicted_time 0\r\n",
            "STAT items:39:outofmemory 9\r\n",
            "STAT items:39:tailrepairs 0\r\n",
            "STAT items:39:reclaimed 13\r\n",
            "STAT items:39:expired_unfetched 4\r\n",
            "STAT items:39:evicted_unfetched 202\r\n",
            "STAT items:39:evicted_active 6\r\n",
            "STAT items:39:crawler_reclaimed 0\r\n",
            "STAT items:39:crawler_items_checked 40\r\n",
            "STAT items:39:lrutail_reflocked 17365\r\n",
            "STAT items:39:moves_to_cold 8703\r\n",
            "STAT items:39:moves_to_warm 7285\r\n",
            "STAT items:39:moves_within_lru 3651\r\n",
            "STAT items:39:direct_reclaims 1949\r\n",
            "STAT items:39:hits_to_hot 894\r\n",
            "STAT items:39:hits_to_warm 4079\r\n",
            "STAT items:39:hits_to_cold 8043\r\n",
            "STAT items:39:hits_to_temp 0\r\n",
            "END\r\n",
        );
        let res = client.items().await.unwrap();

        let expected = SlabItems {
            items: vec![SlabItem {
                id: 39,
                number: 3,
                number_hot: 0,
                number_warm: 1,
                number_cold: 2,
                age_hot: 0,
                age_warm: 7,
                age: 8,
                mem_requested: 1535788,
                evicted: 1646,
                evicted_nonzero: 1646,
                evicted_time: 0,
                out_of_memory: 9,
                tail_repairs: 0,
                reclaimed: 13,
                expired_unfetched: 4,
                evicted_unfetched: 202,
                evicted_active: 6,
                crawler_reclaimed: 0,
                crawler_items_checked: 40,
                lrutail_reflocked: 17365,
                moves_to_cold: 8703,
                moves_to_warm: 7285,
                moves_within_lru: 3651,
                direct_reclaims: 1949,
                hits_to_hot: 894,
                hits_to_warm: 4079,
                hits_to_cold: 8043,
                hits_to_temp: 0,
            }],
        };

        assert_eq!(expected, res);
    }

    //////////
    // meta //
    //////////

    #[tokio::test]
    async fn test_memcached_metas_empty() {
        let (_rx, mut client) = client!();
        let res = client.metas().await.unwrap();

        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn test_memcached_metas_error() {
        let (_rx, mut client) = client!("BUSY crawler is busy\r\n",);
        let res = client.metas().await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::Protocol, err.kind());
    }

    #[tokio::test]
    async fn test_memcached_metas_success() {
        let (_rx, mut client) = client!(
            "key=memcached%2Fmurmur3_hash.c exp=1687216956 la=1687216656 cas=259502 fetch=yes cls=17 size=2912\r\n",
            "key=memcached%2Fmd5.h exp=1687216956 la=1687216656 cas=259731 fetch=yes cls=17 size=3593\r\n",
            "END\r\n",
        );
        let res = client.metas().await.unwrap();

        let expected = vec![
            Meta {
                key: "memcached/murmur3_hash.c".to_string(),
                expires: 1687216956,
                size: 2912,
            },
            Meta {
                key: "memcached/md5.h".to_string(),
                expires: 1687216956,
                size: 3593,
            },
        ];

        assert_eq!(expected, res);
    }
}
