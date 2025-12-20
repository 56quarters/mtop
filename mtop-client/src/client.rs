use crate::core::{ErrorKind, Key, Memcached, Meta, MtopError, SlabItems, Slabs, Stats, Value};
use crate::discovery::{Server, ServerID};
use crate::net::{self, TlsConfig};
use crate::pool::{ClientFactory, ClientPool, ClientPoolConfig, PooledClient};
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt;
use std::hash::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::pki_types::ServerName;
use tracing::instrument::WithSubscriber;

/// Implementation of a `ClientFactory` that creates new Memcached clients that
/// use plaintext or TLS TCP connections.
#[derive(Debug)]
pub struct TcpClientFactory {
    client_config: Option<Arc<ClientConfig>>,
    server_name: Option<ServerName<'static>>,
}

impl TcpClientFactory {
    pub async fn new(tls: TlsConfig) -> Result<Self, MtopError> {
        let server_name = if tls.enabled { tls.server_name.clone() } else { None };

        let client_config = if tls.enabled {
            Some(Arc::new(net::tls_client_config(tls).await?))
        } else {
            None
        };

        Ok(Self {
            client_config,
            server_name,
        })
    }

    fn get_server_name(&self, server: &Server) -> ServerName<'static> {
        self.server_name.clone().unwrap_or_else(|| server.server_name().clone())
    }

    async fn connect(id: &ServerID) -> Result<Memcached, MtopError> {
        let (read, write) = match id {
            ServerID::Socket(sock) => net::tcp_connect(sock).await?,
            ServerID::Name(name) => net::tcp_connect(name).await?,
        };

        Ok(Memcached::new(read, write))
    }

    async fn connect_tls(
        id: &ServerID,
        server_name: ServerName<'static>,
        cfg: Arc<ClientConfig>,
    ) -> Result<Memcached, MtopError> {
        let (read, write) = match id {
            ServerID::Socket(sock) => net::tcp_tls_connect(sock, server_name, cfg.clone()).await?,
            ServerID::Name(name) => net::tcp_tls_connect(name, server_name, cfg.clone()).await?,
        };

        Ok(Memcached::new(read, write))
    }
}

#[async_trait]
impl ClientFactory<Server, Memcached> for TcpClientFactory {
    async fn make(&self, server: &Server) -> Result<Memcached, MtopError> {
        if let Some(cfg) = &self.client_config {
            Self::connect_tls(server.id(), self.get_server_name(server), cfg.clone()).await
        } else {
            Self::connect(server.id()).await
        }
    }
}

/// Logic for picking a server to "own" a particular cache key.
pub trait Selector {
    /// Get a copy of all known servers.
    fn servers(&self) -> Vec<Server>;

    /// Get the `Server` that owns the given key, or an error if there are no servers.
    fn server(&self, key: &Key) -> Result<Server, MtopError>;
}

/// Logic for picking a server to "own" a particular cache key that uses
/// rendezvous hashing.
///
/// See https://en.wikipedia.org/wiki/Rendezvous_hashing
#[derive(Debug)]
pub struct RendezvousSelector {
    servers: Vec<Server>,
}

impl RendezvousSelector {
    /// Create a new instance with the provided initial server list.
    pub fn new(servers: Vec<Server>) -> Self {
        Self { servers }
    }

    fn score(server: &Server, key: &Key) -> u64 {
        let mut hasher = DefaultHasher::new();

        // Match based on the type of ServerID to avoid calling .to_string()
        match server.id() {
            ServerID::Name(name) => name.hash(&mut hasher),
            ServerID::Socket(addr) => addr.hash(&mut hasher),
        }

        hasher.write(key.as_ref().as_bytes());
        hasher.finish()
    }
}

impl Selector for RendezvousSelector {
    fn servers(&self) -> Vec<Server> {
        self.servers.clone()
    }

    fn server(&self, key: &Key) -> Result<Server, MtopError> {
        if self.servers.is_empty() {
            Err(MtopError::runtime("no servers available"))
        } else if self.servers.len() == 1 {
            Ok(self.servers.first().cloned().unwrap())
        } else {
            let mut max = u64::MIN;
            let mut choice = None;

            for server in self.servers.iter() {
                let score = Self::score(server, key);
                if score >= max {
                    choice = Some(server);
                    max = score;
                }
            }

            Ok(choice.cloned().unwrap())
        }
    }
}

/// Response for both values and errors from multiple servers, indexed by server.
#[derive(Debug, Default)]
pub struct ServersResponse<T> {
    pub values: HashMap<ServerID, T>,
    pub errors: HashMap<ServerID, MtopError>,
}

/// Response for values indexed by key and errors indexed by server.
#[derive(Debug, Default)]
pub struct ValuesResponse {
    pub values: HashMap<String, Value>,
    pub errors: HashMap<ServerID, MtopError>,
}

/// Run a method for a particular server, getting a connection from the pool
/// and return the connection after it finishes if there were no errors.
macro_rules! run_for_host {
    ($pool:expr, $server:expr, $method:ident $(, $args:expr)* $(,)?) => {{
        let mut conn = $pool.get($server).await?;
        match conn.$method($($args,)*).await {
            Ok(v) => {
                $pool.put(conn).await;
                Ok(v)
            }
            Err(e) => {
                // Only return the client to the pool if error was due to an
                // expected server error. Otherwise, we have no way to know the
                // state of the client and associated connection.
                if e.kind() == ErrorKind::Protocol {
                    $pool.put(conn).await;
                }
                Err(e)
            }
        }
    }};
}

/// Run a method for a particular server in a spawned future.
macro_rules! spawn_for_host {
    ($self:ident, $server:expr, $method:ident $(, $args:expr)* $(,)?) => {{
        let pool = $self.pool.clone();
        $self.handle.spawn(async move {
            run_for_host!(pool, $server, $method, $($args,)*)
        }
        // Ensure this new future uses the same subscriber as the current one.
        .with_current_subscriber())
    }};
}

/// Run a method on a connection to a particular server based on the hash of a single key.
macro_rules! operation_for_key {
    ($self:ident, $method:ident, $key:expr $(, $args:expr)* $(,)?) => {{
        let key = Key::one($key)?;
        let server = $self.selector.server(&key)?;

        run_for_host!($self.pool, &server, $method, &key, $($args,)*)
    }};
}

/// Run a method on a connection to every server and bucket the results and errors by server.
macro_rules! operation_for_all {
    ($self:ident, $method:ident) => {{
        let servers = $self.selector.servers();
        let tasks = servers
            .into_iter()
            .map(|server| (server.id().clone(), spawn_for_host!($self, &server, $method)))
            .collect::<Vec<_>>();

        Ok(collect_results(tasks).await)
    }};
}

/// Await the results of a list of tasks and assemble a ServersResponse from them.
async fn collect_results<T>(tasks: Vec<(ServerID, JoinHandle<Result<T, MtopError>>)>) -> ServersResponse<T> {
    let mut values = HashMap::with_capacity(tasks.len());
    let mut errors = HashMap::new();

    for (id, task) in tasks {
        match task.await {
            Ok(Ok(result)) => {
                values.insert(id, result);
            }
            Ok(Err(e)) => {
                errors.insert(id, e);
            }
            Err(e) => {
                errors.insert(id, MtopError::runtime_cause("fetching cluster values", e));
            }
        };
    }

    ServersResponse { values, errors }
}

/// Configuration for constructing a new client instance.
#[derive(Debug, Clone)]
pub struct MemcachedClientConfig {
    pub pool_max_idle: u64,
}

impl Default for MemcachedClientConfig {
    fn default() -> Self {
        Self { pool_max_idle: 4 }
    }
}

/// Memcached client that operates on multiple servers, pooling connections
/// to them, and sharding keys via a `Selector` implementation.
pub struct MemcachedClient {
    handle: Handle,
    selector: Box<dyn Selector + Send + Sync>,
    pool: Arc<ClientPool<Server, Memcached>>,
}

impl MemcachedClient {
    /// Create a new `MemcachedClient` instance.
    ///
    /// `handle` is used to spawn multiple async tasks to fetch data from servers in
    /// parallel. `selector` is used to determine which server "owns" a particular key.
    /// `factory` is used for establishing new connections, which are pooled, to each
    /// server as needed.
    pub fn new<S, F>(config: MemcachedClientConfig, handle: Handle, selector: S, factory: F) -> Self
    where
        S: Selector + Send + Sync + 'static,
        F: ClientFactory<Server, Memcached> + Send + Sync + 'static,
    {
        let pool_config = ClientPoolConfig {
            name: "memcached-tcp".to_owned(),
            max_idle: config.pool_max_idle,
        };

        Self {
            handle,
            selector: Box::new(selector),
            pool: Arc::new(ClientPool::new(pool_config, factory)),
        }
    }

    /// Get a connection to a particular server from the pool if available, otherwise
    /// establish a new connection.
    pub async fn raw_open(&self, server: &Server) -> Result<PooledClient<Server, Memcached>, MtopError> {
        self.pool.get(server).await
    }

    /// Return a connection to a particular server to the pool if fewer than the configured
    /// number of idle connections to that server are currently in the pool, otherwise close
    /// it immediately.
    pub async fn raw_close(&self, connection: PooledClient<Server, Memcached>) {
        self.pool.put(connection).await
    }

    /// Get a `Stats` object with the current values of the interesting stats for each server.
    ///
    /// A future is spawned for each server with results and any errors indexed by server. A
    /// pooled connection to each server is used if available, otherwise new connections are
    /// established.
    pub async fn stats(&self) -> Result<ServersResponse<Stats>, MtopError> {
        operation_for_all!(self, stats)
    }

    /// Get a `Slabs` object with information about each set of `Slab`s maintained by each server.
    /// You can think of each `Slab` as a class of objects that are stored together in memory. Note
    /// that `Slab` IDs may not be contiguous based on the size of items actually stored by the server.
    ///
    /// A future is spawned for each server with results and any errors indexed by server. A
    /// pooled connection to each server is used if available, otherwise new connections are
    /// established.
    pub async fn slabs(&self) -> Result<ServersResponse<Slabs>, MtopError> {
        operation_for_all!(self, slabs)
    }

    /// Get a `SlabsItems` object with information about the `SlabItem` items stored in
    /// each slab class maintained by each server. The ID of each `SlabItem` corresponds to a
    /// `Slab` maintained by the server. Note that `SlabItem` IDs may not be contiguous based
    /// on the size of items actually stored by the server.
    ///
    /// A future is spawned for each server with results and any errors indexed by server. A
    /// pooled connection to each server is used if available, otherwise new connections are
    /// established.
    pub async fn items(&self) -> Result<ServersResponse<SlabItems>, MtopError> {
        operation_for_all!(self, items)
    }

    /// Get a `Meta` object for every item in the cache for each server which includes its key
    /// and expiration time as a UNIX timestamp. Expiration time will be `-1` if the item was
    /// set with an infinite TTL.
    ///
    /// A future is spawned for each server with results and any errors indexed by server. A
    /// pooled connection to each server is used if available, otherwise new connections are
    /// established.
    pub async fn metas(&self) -> Result<ServersResponse<Vec<Meta>>, MtopError> {
        operation_for_all!(self, metas)
    }

    /// Send a simple command to verify our connection each known server.
    ///
    /// A future is spawned for each server with results and any errors indexed by server. A
    /// pooled connection to each server is used if available, otherwise new connections are
    /// established.
    pub async fn ping(&self) -> Result<ServersResponse<()>, MtopError> {
        operation_for_all!(self, ping)
    }

    /// Flush all entries in the cache for each server, optionally after a delay. When a
    /// delay is used, each server will flush entries after a delay but the calls will still
    /// return immediately.
    ///
    /// A future is spawned for each server with results and any errors indexed by server. A
    /// pooled connection to each server is used if available, otherwise new connections are
    /// established.
    pub async fn flush_all(&self, wait: Option<Duration>) -> Result<ServersResponse<()>, MtopError> {
        // Note that we aren't using operation_for_all! here because we want to pass a
        // different delay argument to each server. This allows callers to flush the cache
        // in one server after another, with a delay between each for safety.
        let servers = self.selector.servers();

        let tasks = servers
            .into_iter()
            .enumerate()
            .map(|(i, server)| {
                let delay = wait.map(|d| d * i as u32);
                (server.id().clone(), spawn_for_host!(self, &server, flush_all, delay))
            })
            .collect::<Vec<_>>();

        Ok(collect_results(tasks).await)
    }

    /// Get a map of the requested keys and their corresponding `Value` in the cache
    /// including the key, flags, and data.
    ///
    /// This method uses a selector implementation to determine which server "owns" each of the
    /// provided keys. A future is spawned for each server and the results merged together. A
    /// pooled connection to each server is used if available, otherwise new connections are
    /// established.
    pub async fn get<I, K>(&self, keys: I) -> Result<ValuesResponse, MtopError>
    where
        I: IntoIterator<Item = K>,
        K: Into<String>,
    {
        let keys = Key::many(keys)?;
        if keys.is_empty() {
            return Ok(ValuesResponse::default());
        }

        let num_keys = keys.len();
        let mut by_server: HashMap<Server, Vec<Key>> = HashMap::new();
        for key in keys {
            let server = self.selector.server(&key)?;
            let entry = by_server.entry(server).or_default();
            entry.push(key);
        }

        let tasks = by_server
            .into_iter()
            .map(|(server, keys)| (server.id().clone(), spawn_for_host!(self, &server, get, &keys)))
            .collect::<Vec<_>>();

        let mut values = HashMap::with_capacity(num_keys);
        let mut errors = HashMap::new();

        for (id, task) in tasks {
            match task.await {
                Ok(Ok(results)) => {
                    values.extend(results);
                }
                Ok(Err(e)) => {
                    errors.insert(id, e);
                }
                Err(e) => {
                    errors.insert(id, MtopError::runtime_cause("fetching keys", e));
                }
            };
        }

        Ok(ValuesResponse { values, errors })
    }

    /// Increment the value of a key by the given delta if the value is numeric returning
    /// the new value. Returns an error if the value is not set or _not_ numeric.
    ///
    /// This method uses a selector implementation to determine which server "owns" the provided
    /// key. A pooled connection to the server is used if available, otherwise a new connection
    /// is established.
    pub async fn incr<K>(&self, key: K, delta: u64) -> Result<u64, MtopError>
    where
        K: Into<String>,
    {
        operation_for_key!(self, incr, key, delta)
    }

    /// Decrement the value of a key by the given delta if the value is numeric returning
    /// the new value with a minimum of 0. Returns an error if the value is not set or _not_
    /// numeric.
    ///
    /// This method uses a selector implementation to determine which server "owns" the provided
    /// key. A pooled connection to the server is used if available, otherwise a new connection
    /// is established.
    pub async fn decr<K>(&self, key: K, delta: u64) -> Result<u64, MtopError>
    where
        K: Into<String>,
    {
        operation_for_key!(self, decr, key, delta)
    }

    /// Store the provided item in the cache, regardless of whether it already exists.
    ///
    /// This method uses a selector implementation to determine which server "owns" the provided
    /// key. A pooled connection to the server is used if available, otherwise a new connection
    /// is established.
    pub async fn set<K, V>(&self, key: K, flags: u64, ttl: u32, data: V) -> Result<(), MtopError>
    where
        K: Into<String>,
        V: AsRef<[u8]>,
    {
        operation_for_key!(self, set, key, flags, ttl, data)
    }

    /// Store the provided item in the cache only if it does not already exist.
    ///
    /// This method uses a selector implementation to determine which server "owns" the provided
    /// key. A pooled connection to the server is used if available, otherwise a new connection
    /// is established.
    pub async fn add<K, V>(&self, key: K, flags: u64, ttl: u32, data: V) -> Result<(), MtopError>
    where
        K: Into<String>,
        V: AsRef<[u8]>,
    {
        operation_for_key!(self, add, key, flags, ttl, data)
    }

    /// Store the provided item in the cache only if it already exists.
    ///
    /// This method uses a selector implementation to determine which server "owns" the provided
    /// key. A pooled connection to the server is used if available, otherwise a new connection
    /// is established.
    pub async fn replace<K, V>(&self, key: K, flags: u64, ttl: u32, data: V) -> Result<(), MtopError>
    where
        K: Into<String>,
        V: AsRef<[u8]>,
    {
        operation_for_key!(self, replace, key, flags, ttl, data)
    }

    /// Update the TTL of an item in the cache if it exists, return an error otherwise.
    ///
    /// This method uses a selector implementation to determine which server "owns" the provided
    /// key. A pooled connection to the server is used if available, otherwise a new connection
    /// is established.
    pub async fn touch<K>(&self, key: K, ttl: u32) -> Result<(), MtopError>
    where
        K: Into<String>,
    {
        operation_for_key!(self, touch, key, ttl)
    }

    /// Delete an item from the cache if it exists, return an error otherwise.
    ///
    /// This method uses a selector implementation to determine which server "owns" the provided
    /// key. A pooled connection to the server is used if available, otherwise a new connection
    /// is established.
    pub async fn delete<K>(&self, key: K) -> Result<(), MtopError>
    where
        K: Into<String>,
    {
        operation_for_key!(self, delete, key)
    }
}

impl fmt::Debug for MemcachedClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemcachedClient")
            .field("selector", &"...")
            .field("pool", &self.pool)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::{MemcachedClient, MemcachedClientConfig, Selector};
    use crate::core::{ErrorKind, Key, Memcached, MtopError, Value};
    use crate::discovery::{Server, ServerID};
    use crate::pool::ClientFactory;
    use async_trait::async_trait;
    use rustls_pki_types::ServerName;
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::time::Duration;
    use tokio::runtime::Handle;

    #[derive(Debug, Default)]
    struct MockSelector {
        mapping: HashMap<Key, Server>,
    }

    impl Selector for MockSelector {
        fn servers(&self) -> Vec<Server> {
            self.mapping.values().cloned().collect()
        }

        fn server(&self, key: &Key) -> Result<Server, MtopError> {
            self.mapping
                .get(key)
                .cloned()
                .ok_or_else(|| MtopError::runtime("no servers available"))
        }
    }

    #[derive(Debug, Default)]
    struct MockClientFactory {
        contents: HashMap<Server, Vec<u8>>,
    }

    #[async_trait]
    impl ClientFactory<Server, Memcached> for MockClientFactory {
        async fn make(&self, key: &Server) -> Result<Memcached, MtopError> {
            let bytes = self
                .contents
                .get(key)
                .cloned()
                .ok_or_else(|| MtopError::runtime(format!("no server for {:?}", key)))?;
            let reads = Cursor::new(bytes);
            Ok(Memcached::new(reads, Vec::new()))
        }
    }

    macro_rules! new_client {
        () => {{
            let cfg = MemcachedClientConfig { pool_max_idle: 1 };
            let handle = Handle::current();
            let selector = MockSelector::default();
            let factory = MockClientFactory::default();
            MemcachedClient::new(cfg, handle, selector, factory)
        }};

        ($($host_and_port:expr => $key:expr => $contents:expr$(,)?)*) => {{
            let mut mapping = HashMap::new();
            let mut contents = HashMap::new();

            $(
            let server = {
                let (host, port_str) = $host_and_port.split_once(':').unwrap();
                let port: u16 = port_str.parse().unwrap();
                let id = ServerID::from((host, port));
                let name = ServerName::try_from(host).unwrap();

                Server::new(id, name)
            };
            mapping.insert(Key::one($key).unwrap(), server.clone());
            contents.insert(server, $contents.to_vec());
            )*

            let cfg = MemcachedClientConfig { pool_max_idle: 1 };
            let handle = Handle::current();
            let selector = MockSelector { mapping };
            let factory = MockClientFactory { contents };
            MemcachedClient::new(cfg, handle, selector, factory)
        }};
    }

    // NOTE: We aren't testing all methods of the client, just a representative
    //  selection. This is because there are only really four types of methods
    //  in the client:
    //  1 - methods that operate on every server via operation_for_all!
    //  2 - methods that operate on a single server based on the key via
    //      operation_for_key!
    //  3 - the get method that has its own non-macro implementation.
    //  4 - the flush_all method that has its own non-macro implementation.

    //////////
    // ping //
    //////////

    #[tokio::test]
    async fn test_memcached_client_ping_no_servers() {
        let client = new_client!();
        let response = client.ping().await.unwrap();

        assert!(response.values.is_empty());
        assert!(response.errors.is_empty());
    }

    #[tokio::test]
    async fn test_memcached_client_ping_no_errors() {
        let client = new_client!(
            "cache01.example.com:11211" => "unused1" => "VERSION 1.6.22\r\n".as_bytes(),
            "cache02.example.com:11211" => "unused2" => "VERSION 1.6.22\r\n".as_bytes(),
        );
        let response = client.ping().await.unwrap();

        assert!(response.values.contains_key(&ServerID::from(("cache01.example.com", 11211))));
        assert!(response.values.contains_key(&ServerID::from(("cache02.example.com", 11211))));
        assert!(response.errors.is_empty());
    }

    #[tokio::test]
    async fn test_memcached_client_ping_some_errors() {
        let client = new_client!(
            "cache01.example.com:11211" => "unused1" => "VERSION 1.6.22\r\n".as_bytes(),
            "cache02.example.com:11211" => "unused2" => "ERROR Too many open connections\r\n".as_bytes(),
        );
        let response = client.ping().await.unwrap();

        assert!(response.values.contains_key(&ServerID::from(("cache01.example.com", 11211))));
        assert!(response.errors.contains_key(&ServerID::from(("cache02.example.com", 11211))));
    }

    /////////
    // set //
    /////////

    #[tokio::test]
    async fn test_memcached_client_set_no_servers() {
        let client = new_client!();
        let res = client.set("key1", 1, 60, "foo".as_bytes()).await;
        let err = res.unwrap_err();

        assert_eq!(ErrorKind::Runtime, err.kind());
    }

    #[tokio::test]
    async fn test_memcached_client_set_success() {
        let client = new_client!(
            "cache01.example.com:11211" => "key1" => "STORED\r\n".as_bytes(),
        );
        client.set("key1", 1, 60, "foo".as_bytes()).await.unwrap();
    }

    /////////
    // get //
    /////////

    #[tokio::test]
    async fn test_memcached_client_get_invalid_keys() {
        let client = new_client!();
        let res = client.get(vec!["invalid key"]).await;
        let err = res.unwrap_err();

        assert_eq!(ErrorKind::Runtime, err.kind());
    }

    #[tokio::test]
    async fn test_memcached_client_get_no_keys() {
        let client = new_client!();
        let keys: Vec<String> = Vec::new();
        let response = client.get(keys).await.unwrap();

        assert!(response.values.is_empty());
        assert!(response.errors.is_empty());
    }

    #[tokio::test]
    async fn test_memcached_client_get_no_servers() {
        let client = new_client!();
        let res = client.get(vec!["key1", "key2"]).await;
        let err = res.unwrap_err();

        assert_eq!(ErrorKind::Runtime, err.kind());
    }

    #[tokio::test]
    async fn test_memcached_client_get_no_errors() {
        let client = new_client!(
            "cache01.example.com:11211" => "key1" => "VALUE key1 1 6 123\r\nfoobar\r\nEND\r\n".as_bytes(),
            "cache02.example.com:11211" => "key2" => "VALUE key2 2 7 456\r\nbazbing\r\nEND\r\n".as_bytes(),
        );
        let response = client.get(vec!["key1", "key2"]).await.unwrap();

        assert_eq!(
            response.values.get("key1"),
            Some(&Value {
                key: "key1".to_owned(),
                cas: 123,
                flags: 1,
                data: "foobar".as_bytes().to_owned(),
            })
        );
        assert_eq!(
            response.values.get("key2"),
            Some(&Value {
                key: "key2".to_owned(),
                cas: 456,
                flags: 2,
                data: "bazbing".as_bytes().to_owned(),
            })
        );
    }

    #[tokio::test]
    async fn test_memcached_client_get_some_errors() {
        let client = new_client!(
            "cache01.example.com:11211" => "key1" => "VALUE key1 1 6 123\r\nfoobar\r\nEND\r\n".as_bytes(),
            "cache02.example.com:11211" => "key2" => "ERROR Too many open connections\r\n".as_bytes(),
        );
        let res = client.get(vec!["key1", "key2"]).await;
        let values = res.unwrap();

        assert_eq!(
            values.values.get("key1"),
            Some(&Value {
                key: "key1".to_owned(),
                cas: 123,
                flags: 1,
                data: "foobar".as_bytes().to_owned(),
            })
        );
        assert_eq!(values.values.get("key2"), None);

        let id = ServerID::from(("cache02.example.com", 11211));
        assert_eq!(values.errors.get(&id).map(|e| e.kind()), Some(ErrorKind::Protocol))
    }

    ///////////////
    // flush_all //
    ///////////////

    #[tokio::test]
    async fn test_memcached_client_flush_all_no_wait_success() {
        let client = new_client!(
            "cache01.example.com:11211" => "unused1" => "OK\r\n".as_bytes(),
            "cache02.example.com:11211" => "unused2" => "OK\r\n".as_bytes(),
        );

        let res = client.flush_all(None).await;
        let response = res.unwrap();

        assert!(response.values.contains_key(&ServerID::from(("cache01.example.com", 11211))));
        assert!(response.values.contains_key(&ServerID::from(("cache02.example.com", 11211))));
    }

    #[tokio::test]
    async fn test_memcached_client_flush_all_no_wait_some_errors() {
        let client = new_client!(
            "cache01.example.com:11211" => "unused1" => "OK\r\n".as_bytes(),
            "cache02.example.com:11211" => "unused2" => "ERROR\r\n".as_bytes(),
        );

        let res = client.flush_all(None).await;
        let response = res.unwrap();

        assert!(response.values.contains_key(&ServerID::from(("cache01.example.com", 11211))));
        assert!(response.errors.contains_key(&ServerID::from(("cache02.example.com", 11211))));
    }

    #[tokio::test]
    async fn test_memcached_client_flush_all_wait_success() {
        let client = new_client!(
            "cache01.example.com:11211" => "unused1" => "OK\r\n".as_bytes(),
            "cache02.example.com:11211" => "unused2" => "OK\r\n".as_bytes(),
        );

        let res = client.flush_all(Some(Duration::from_secs(5))).await;
        let response = res.unwrap();

        assert!(response.values.contains_key(&ServerID::from(("cache01.example.com", 11211))));
        assert!(response.values.contains_key(&ServerID::from(("cache02.example.com", 11211))));
    }

    #[tokio::test]
    async fn test_memcached_client_flush_all_wait_some_errors() {
        let client = new_client!(
            "cache01.example.com:11211" => "unused1" => "OK\r\n".as_bytes(),
            "cache02.example.com:11211" => "unused2" => "ERROR\r\n".as_bytes(),
        );

        let res = client.flush_all(Some(Duration::from_secs(5))).await;
        let response = res.unwrap();

        assert!(response.values.contains_key(&ServerID::from(("cache01.example.com", 11211))));
        assert!(response.errors.contains_key(&ServerID::from(("cache02.example.com", 11211))));
    }
}
