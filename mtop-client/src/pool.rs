use crate::core::{Memcached, MtopError};
use std::collections::HashMap;
use std::fs::File;
use std::future::Future;
use std::io::BufReader as StdBufReader;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::Arc;
use std::{fmt, io};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

#[derive(Debug)]
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

#[derive(Debug)]
pub struct PoolConfig {
    pub max_idle_per_host: usize,
    pub check_on_get: bool,
    pub check_on_put: bool,
    pub tls: TLSConfig,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_idle_per_host: 4,
            check_on_get: true,
            check_on_put: true,
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
    pub server_name: Option<String>,
}

#[derive(Debug)]
pub struct MemcachedPool {
    clients: Mutex<HashMap<String, Vec<Memcached>>>,
    client_config: Option<Arc<ClientConfig>>,
    server: Option<ServerName<'static>>,
    config: PoolConfig,
}

impl MemcachedPool {
    pub async fn new(handle: Handle, config: PoolConfig) -> Result<Self, MtopError> {
        let server = if let Some(s) = &config.tls.server_name {
            Some(Self::host_to_server_name(s)?)
        } else {
            None
        };

        let client_config = if config.tls.enabled {
            Some(Arc::new(Self::client_config(handle, &config.tls).await?))
        } else {
            None
        };

        Ok(MemcachedPool {
            clients: Mutex::new(HashMap::new()),
            client_config,
            server,
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
            .map(StdBufReader::new)
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
            .map(StdBufReader::new)
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
                    .map_err(|e| MtopError::internal_cause("unable to parse CA cert", e))?;
            }
        } else {
            tracing::debug!(message = "using default CA certs for roots");
            root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().map(|c| c.to_owned()));
        }

        Ok(root_cert_store)
    }

    async fn connect(&self, host: &str) -> Result<Memcached, MtopError> {
        if let Some(cfg) = &self.client_config {
            let server = match &self.server {
                Some(v) => v.clone(),
                None => host
                    .split_once(':')
                    .ok_or_else(|| MtopError::configuration(format!("invalid server name '{}'", host)))
                    .and_then(|(hostname, _)| Self::host_to_server_name(hostname))?,
            };

            tracing::debug!(message = "using server name for TLS validation", server_name = ?server);
            tls_connect(host, server, cfg.clone()).await
        } else {
            plain_connect(host).await
        }
    }

    fn host_to_server_name(host: &str) -> Result<ServerName<'static>, MtopError> {
        ServerName::try_from(host)
            .map(|s| s.to_owned())
            .map_err(|e| MtopError::configuration_cause(format!("invalid server name '{}'", host), e))
    }

    /// Get an existing connection to the given host from the pool or establish a new one
    /// based on the configuration set when this pool was created (plaintext or TLS with
    /// optional client certificate).
    pub async fn get(&self, host: &str) -> Result<PooledMemcached, MtopError> {
        self.get_with_connect(host, self.connect(host)).await
    }

    /// Get an existing connection to the given host from the pool or establish a new one
    /// using the provided `Future` which is expected to create a `Memcached` instance. Note
    /// that this method relies on the fact that futures do nothing unless polled to avoid
    /// making a connection when not required.
    async fn get_with_connect<F>(&self, host: &str, connect: F) -> Result<PooledMemcached, MtopError>
    where
        F: Future<Output = Result<Memcached, MtopError>>,
    {
        let mut map = self.clients.lock().await;
        let mut inner = match map.get_mut(host).and_then(|v| v.pop()) {
            Some(c) => c,
            None => connect.await?,
        };

        if self.config.check_on_get {
            inner.ping().await?;
        }

        Ok(PooledMemcached {
            inner,
            host: host.to_owned(),
        })
    }

    /// Return a connection to the pool if there are currently fewer than `max_idle_per_host`
    /// connections to the host this client is for. If there are more connections, the returned
    /// client is closed immediately.
    pub async fn put(&self, mut client: PooledMemcached) {
        if !self.config.check_on_put || client.ping().await.is_ok() {
            let mut map = self.clients.lock().await;
            let conns = map.entry(client.host).or_insert_with(Vec::new);

            if conns.len() < self.config.max_idle_per_host {
                conns.push(client.inner);
            }
        }
    }
}

async fn plain_connect<A>(host: A) -> Result<Memcached, MtopError>
where
    A: ToSocketAddrs + fmt::Display,
{
    let tcp_stream = tcp_stream(host).await?;
    let (read, write) = tcp_stream.into_split();
    Ok(Memcached::new(read, write))
}

async fn tls_connect<A>(host: A, server: ServerName<'static>, config: Arc<ClientConfig>) -> Result<Memcached, MtopError>
where
    A: ToSocketAddrs + fmt::Display,
{
    let tcp_stream = tcp_stream(host).await?;
    let connector = TlsConnector::from(config);
    let tls_stream = connector.connect(server, tcp_stream).await?;
    let (read, write) = tokio::io::split(tls_stream);
    Ok(Memcached::new(read, write))
}

async fn tcp_stream<A>(host: A) -> Result<TcpStream, MtopError>
where
    A: ToSocketAddrs + fmt::Display,
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
    use std::io::{self, Cursor};
    use tokio::runtime::Handle;

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
        let pool = MemcachedPool::new(Handle::current(), PoolConfig::default())
            .await
            .unwrap();

        let connect = async {
            Ok(client!(
                "VERSION 1.6.20\r\n",
                "VERSION 1.6.20\r\n",
                "VERSION 1.6.20\r\n"
            ))
        };

        let client = pool.get_with_connect("localhost:11211", connect).await;
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_get_existing_connection() {
        let pool = MemcachedPool::new(Handle::current(), PoolConfig::default())
            .await
            .unwrap();

        pool.put(PooledMemcached {
            host: "localhost:11211".to_owned(),
            inner: client!("VERSION 1.6.20\r\n", "VERSION 1.6.20\r\n", "VERSION 1.6.20\r\n"),
        })
        .await;

        let connect = async { panic!("should not be called!") };
        let res = pool.get_with_connect("localhost:11211", connect).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_get_dead_connection() {
        let pool = MemcachedPool::new(Handle::current(), PoolConfig::default())
            .await
            .unwrap();

        pool.put(PooledMemcached {
            host: "localhost:11211".to_owned(),
            inner: client!("VERSION 1.6.20\r\n", "ERROR Too many open connections\r\n"),
        })
        .await;

        let connect = async { panic!("should not be called!") };
        let res = pool.get_with_connect("localhost:11211", connect).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::Protocol, err.kind());
    }

    #[tokio::test]
    async fn test_get_error() {
        let pool = MemcachedPool::new(Handle::current(), PoolConfig::default())
            .await
            .unwrap();

        let connect = async { Err(MtopError::from(io::Error::new(io::ErrorKind::TimedOut, "timeout"))) };
        let res = pool.get_with_connect("localhost:11211", connect).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(ErrorKind::IO, err.kind());
    }
}
