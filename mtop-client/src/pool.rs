use crate::core::{Memcached, MtopError};
use crate::discovery::Server;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::fs::File;
use std::future::Future;
use std::io::{self, BufReader};
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;
use webpki::types::{CertificateDer, PrivateKeyDer, ServerName};

#[derive(Debug)]
pub struct PooledMemcached {
    inner: Memcached,
    host: Server,
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

#[derive(Debug)]
pub struct PoolConfig {
    pub max_idle_per_host: u64,
    pub check_on_get: bool,
    pub check_on_put: bool,
    pub tls: TLSConfig,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_idle_per_host: 4,
            check_on_get: false,
            check_on_put: false,
            tls: TLSConfig::default(),
        }
    }
}

#[derive(Debug, Default)]
pub struct TLSConfig {
    pub enabled: bool,
    pub ca_path: Option<PathBuf>,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub server_name: Option<ServerName<'static>>,
}

#[derive(Debug)]
pub struct MemcachedPool {
    connections: Mutex<HashMap<Server, Vec<Memcached>>>,
    client_config: Option<Arc<ClientConfig>>,
    config: PoolConfig,
}

impl MemcachedPool {
    pub async fn new(handle: Handle, config: PoolConfig) -> Result<Self, MtopError> {
        let client_config = if config.tls.enabled {
            Some(Arc::new(Self::client_config(handle, &config.tls).await?))
        } else {
            None
        };

        Ok(MemcachedPool {
            connections: Mutex::new(HashMap::new()),
            client_config,
            config,
        })
    }

    async fn client_config(handle: Handle, tls: &TLSConfig) -> Result<ClientConfig, MtopError> {
        let ca = if let Some(p) = &tls.ca_path {
            let certs = Self::load_cert(&handle, p).await?;
            tracing::debug!(message = "loaded CA certs", num_certs = certs.len(), path = ?p);
            Some(certs)
        } else {
            None
        };

        let client_cert = if let Some(p) = &tls.cert_path {
            let certs = Self::load_cert(&handle, p).await?;
            tracing::debug!(message = "loaded client certs", num_certs = certs.len(), path = ?p);
            Some(certs)
        } else {
            None
        };

        let client_key = if let Some(p) = &tls.key_path {
            let keys = Self::load_key(&handle, p).await?;
            tracing::debug!(message = "loaded client key", path = ?p);
            Some(keys)
        } else {
            None
        };

        let trust_store = Self::trust_store(ca)?;
        let builder = ClientConfig::builder().with_root_certificates(trust_store);

        let config = match (client_cert, client_key) {
            (Some(cert), Some(key)) => {
                tracing::debug!(message = "using key and cert for client authentication");
                builder
                    .with_client_auth_cert(cert, key)
                    .map_err(|e| MtopError::configuration_cause("unable to use client cert or key", e))?
            }
            _ => {
                tracing::debug!(message = "not using any client authentication");
                builder.with_no_client_auth()
            }
        };

        Ok(config)
    }

    async fn load_cert(handle: &Handle, path: &PathBuf) -> Result<Vec<CertificateDer<'static>>, MtopError> {
        let mut reader = File::open(path)
            .map(BufReader::new)
            .map_err(|e| MtopError::configuration_cause(format!("unable to load cert {:?}", path), e))?;

        // Read all certs from the file in a separate thread and then convert the awkward
        // Vec<Result<Cert>> type to a Result<Vec<Cert>> since we expect all certs to be valid
        handle
            .spawn_blocking(move || rustls_pemfile::certs(&mut reader).collect::<Vec<_>>())
            .await
            .unwrap()
            .into_iter()
            .collect::<Result<Vec<CertificateDer<'static>>, io::Error>>()
            .map_err(|e| MtopError::configuration_cause(format!("unable to parse cert {:?}", path), e))
    }

    async fn load_key(handle: &Handle, path: &PathBuf) -> Result<PrivateKeyDer<'static>, MtopError> {
        let mut reader = File::open(path)
            .map(BufReader::new)
            .map_err(|e| MtopError::configuration_cause(format!("unable to load key {:?}", path), e))?;

        // Read a single key in a separate thread returning an error if there is no key.
        handle
            .spawn_blocking(move || rustls_pemfile::private_key(&mut reader))
            .await
            .unwrap()
            .map_err(|e| MtopError::configuration_cause(format!("unable to parse key {:?}", path), e))?
            .ok_or_else(|| MtopError::configuration(format!("no keys available in {:?}", path)))
    }

    fn trust_store(ca: Option<Vec<CertificateDer<'static>>>) -> Result<RootCertStore, MtopError> {
        let mut root_cert_store = RootCertStore::empty();

        if let Some(ca_certs) = ca {
            tracing::debug!(message = "adding custom CA certs for roots", num_certs = ca_certs.len());
            for cert in ca_certs {
                root_cert_store
                    .add(cert)
                    .map_err(|e| MtopError::runtime_cause("unable to parse CA cert", e))?;
            }
        } else {
            tracing::debug!(message = "using default CA certs for roots");
            root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().map(|c| c.to_owned()));
        }

        Ok(root_cert_store)
    }

    async fn connect(&self, server: &Server) -> Result<Memcached, MtopError> {
        if let Some(cfg) = &self.client_config {
            let name = self.config.tls.server_name.clone().unwrap_or_else(|| server.server_name());
            tracing::debug!(message = "using server name for TLS validation", server_name = ?name);
            tls_connect(server.address(), name, cfg.clone()).await
        } else {
            plain_connect(server.address()).await
        }
    }

    /// Get an existing connection to the given server from the pool or establish a new one
    /// based on the configuration set when this pool was created (plaintext or TLS with
    /// optional client certificate).
    pub async fn get(&self, server: &Server) -> Result<PooledMemcached, MtopError> {
        self.get_with_connect(server, self.connect(server)).await
    }

    /// Get an existing connection to the given server from the pool or establish a new one
    /// using the provided `Future` which is expected to create a `Memcached` instance. Note
    /// that this method relies on the fact that futures do nothing unless polled to avoid
    /// making a connection when not required.
    async fn get_with_connect<F>(&self, server: &Server, connect: F) -> Result<PooledMemcached, MtopError>
    where
        F: Future<Output = Result<Memcached, MtopError>>,
    {
        let mut map = self.connections.lock().await;
        let mut inner = match map.get_mut(server).and_then(|v| v.pop()) {
            Some(c) => c,
            None => connect.await?,
        };

        if self.config.check_on_get {
            inner.ping().await?;
        }

        Ok(PooledMemcached {
            inner,
            host: server.clone(),
        })
    }

    /// Return a connection to the pool if there are currently fewer than `max_idle_per_host`
    /// connections to the host this client is for. If there are more connections, the returned
    /// client is closed immediately.
    pub async fn put(&self, mut conn: PooledMemcached) {
        if !self.config.check_on_put || conn.ping().await.is_ok() {
            let mut map = self.connections.lock().await;
            let conns = map.entry(conn.host).or_default();

            if (conns.len() as u64) < self.config.max_idle_per_host {
                conns.push(conn.inner);
            }
        }
    }
}

async fn plain_connect<A>(host: A) -> Result<Memcached, MtopError>
where
    A: tokio::net::ToSocketAddrs + fmt::Display,
{
    let tcp_stream = tcp_stream(host).await?;
    let (read, write) = tcp_stream.into_split();
    Ok(Memcached::new(read, write))
}

async fn tls_connect<A>(host: A, server: ServerName<'static>, config: Arc<ClientConfig>) -> Result<Memcached, MtopError>
where
    A: tokio::net::ToSocketAddrs + fmt::Display,
{
    let tcp_stream = tcp_stream(host).await?;
    let connector = TlsConnector::from(config);
    let tls_stream = connector.connect(server, tcp_stream).await?;
    let (read, write) = tokio::io::split(tls_stream);
    Ok(Memcached::new(read, write))
}

async fn tcp_stream<A>(host: A) -> Result<TcpStream, MtopError>
where
    A: tokio::net::ToSocketAddrs + fmt::Display,
{
    TcpStream::connect(&host)
        .await
        // The client buffers and flushes writes so we don't need delay here to
        // avoid lots of tiny packets.
        .and_then(|s| s.set_nodelay(true).map(|_| s))
        .map_err(|e| MtopError::from((host.to_string(), e)))
}

#[cfg(test)]
mod test {
    use super::{MemcachedPool, PoolConfig, PooledMemcached};
    use crate::core::{ErrorKind, Memcached, MtopError};
    use crate::discovery::{Server, ServerID};
    use std::io::{self, Cursor};
    use std::net::SocketAddr;
    use tokio::runtime::Handle;
    use webpki::types::ServerName;

    /// Create a new `Memcached` instance to read the provided server response.
    macro_rules! client {
        ($($line:expr),+ $(,)?) => ({
            let writes = Vec::new();
            let mut reads = Vec::new();
            $(reads.extend_from_slice($line.as_bytes());)+
            Memcached::new(Cursor::new(reads), writes)
        })
    }

    #[tokio::test]
    async fn test_get_new_connection() {
        let cfg = PoolConfig::default();
        let pool = MemcachedPool::new(Handle::current(), cfg).await.unwrap();
        let id = ServerID::from("127.0.0.1:11211".parse::<SocketAddr>().unwrap());
        let server = Server::new(id, ServerName::try_from("localhost").unwrap().to_owned());

        let connect = async {
            Ok(client!(
                "VERSION 1.6.20\r\n",
                "VERSION 1.6.20\r\n",
                "VERSION 1.6.20\r\n"
            ))
        };

        let client = pool.get_with_connect(&server, connect).await;
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_get_existing_connection() {
        let cfg = PoolConfig::default();
        let pool = MemcachedPool::new(Handle::current(), cfg).await.unwrap();

        let id = ServerID::from("127.0.0.1:11211".parse::<SocketAddr>().unwrap());
        let server = Server::new(id, ServerName::try_from("localhost").unwrap().to_owned());

        pool.put(PooledMemcached {
            host: server.clone(),
            inner: client!("VERSION 1.6.20\r\n", "VERSION 1.6.20\r\n", "VERSION 1.6.20\r\n"),
        })
        .await;

        let connect = async { panic!("should not be called!") };
        let res = pool.get_with_connect(&server, connect).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_get_dead_connection() {
        let cfg = PoolConfig {
            max_idle_per_host: 4,
            check_on_get: true,
            check_on_put: true,
            ..Default::default()
        };

        let pool = MemcachedPool::new(Handle::current(), cfg).await.unwrap();
        let id = ServerID::from("127.0.0.1:11211".parse::<SocketAddr>().unwrap());
        let server = Server::new(id, ServerName::try_from("localhost").unwrap().to_owned());

        pool.put(PooledMemcached {
            host: server.clone(),
            inner: client!("VERSION 1.6.20\r\n", "ERROR Too many open connections\r\n"),
        })
        .await;

        let connect = async { panic!("should not be called!") };
        let res = pool.get_with_connect(&server, connect).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::Protocol, err.kind());
    }

    #[tokio::test]
    async fn test_get_error() {
        let cfg = PoolConfig::default();
        let pool = MemcachedPool::new(Handle::current(), cfg).await.unwrap();

        let id = ServerID::from("127.0.0.1:11211".parse::<SocketAddr>().unwrap());
        let server = Server::new(id, ServerName::try_from("localhost").unwrap().to_owned());

        let connect = async { Err(MtopError::from(io::Error::new(io::ErrorKind::TimedOut, "timeout"))) };
        let res = pool.get_with_connect(&server, connect).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::IO, err.kind());
    }
}
