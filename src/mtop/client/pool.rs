use crate::client::core::{Memcached, MtopError};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader as StdBufReader;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tokio_rustls::rustls::{Certificate, ClientConfig, OwnedTrustAnchor, PrivateKey, RootCertStore, ServerName};
use tokio_rustls::TlsConnector;
use webpki::TrustAnchor;

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
pub struct TLSConfig {
    pub enabled: bool,
    pub ca_path: Option<PathBuf>,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub server_name: Option<String>,
}

pub struct MemcachedPool {
    clients: Mutex<HashMap<String, Memcached>>,
    config: Option<Arc<ClientConfig>>,
    server: Option<ServerName>,
}

impl MemcachedPool {
    pub async fn new(handle: Handle, tls: TLSConfig) -> Result<Self, MtopError> {
        let server = if let Some(s) = &tls.server_name {
            Some(Self::host_to_server_name(s)?)
        } else {
            None
        };

        let config = if tls.enabled {
            Some(Arc::new(Self::config(handle, &tls).await?))
        } else {
            None
        };

        Ok(MemcachedPool {
            clients: Mutex::new(HashMap::new()),
            config,
            server,
        })
    }

    async fn config(handle: Handle, tls: &TLSConfig) -> Result<ClientConfig, MtopError> {
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

        if client_cert.is_some() != client_key.is_some() {
            // TODO: Return an error? Can we get clap to do this?
        }

        let trust_store = Self::trust_store(ca)?;
        let builder = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(trust_store);

        let config = match (client_cert, client_key) {
            (Some(cert), Some(key)) => {
                tracing::debug!(message = "using key and cert for client authentication");
                builder
                    .with_single_cert(cert, key)
                    .map_err(|e| MtopError::configuration_cause("unable to use client cert or key", e))?
            }
            _ => {
                tracing::debug!(message = "not using any client authentication");
                builder.with_no_client_auth()
            }
        };

        Ok(config)
    }

    async fn load_cert(handle: &Handle, path: &PathBuf) -> Result<Vec<Certificate>, MtopError> {
        let mut reader = File::open(path.clone())
            .map(StdBufReader::new)
            .map_err(|e| MtopError::configuration_cause(format!("unable to load cert {:?}", path), e))?;

        Ok(handle
            .spawn_blocking(move || rustls_pemfile::certs(&mut reader))
            .await
            .unwrap()
            .map_err(|e| MtopError::configuration_cause(format!("unable to parse cert {:?}", path), e))? // unwrap the spawn result, try the read result
            .into_iter()
            .map(Certificate)
            .collect())
    }

    async fn load_key(handle: &Handle, path: &PathBuf) -> Result<PrivateKey, MtopError> {
        let mut reader = File::open(path.clone())
            .map(StdBufReader::new)
            .map_err(|e| MtopError::configuration_cause(format!("unable to load key {:?}", path), e))?;

        handle
            .spawn_blocking(move || rustls_pemfile::pkcs8_private_keys(&mut reader))
            .await
            .unwrap()
            .map_err(|e| MtopError::configuration_cause(format!("unable to parse key {:?}", path), e))? // unwrap the spawn result, try the read result
            .into_iter()
            .next()
            .map(PrivateKey)
            .ok_or_else(|| MtopError::configuration(format!("no keys available in {:?}", path)))
    }

    fn trust_store(ca: Option<Vec<Certificate>>) -> Result<RootCertStore, MtopError> {
        let mut root_cert_store = RootCertStore::empty();

        if let Some(ca_certs) = ca {
            let mut anchors = Vec::with_capacity(ca_certs.len());
            for cert in ca_certs {
                let anchor = TrustAnchor::try_from_cert_der(&cert.0)
                    .map(|ta| {
                        OwnedTrustAnchor::from_subject_spki_name_constraints(ta.subject, ta.spki, ta.name_constraints)
                    })
                    .map_err(|e| MtopError::internal_cause("unable to parse CA cert", e))?;
                anchors.push(anchor);
            }

            tracing::debug!(message = "adding custom CA certs for roots", num_certs = anchors.len());
            root_cert_store.add_server_trust_anchors(anchors.into_iter());
        } else {
            tracing::debug!(message = "using default CA certs for roots");
            root_cert_store.add_server_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.0.iter().map(|ta| {
                OwnedTrustAnchor::from_subject_spki_name_constraints(ta.subject, ta.spki, ta.name_constraints)
            }));
        }

        Ok(root_cert_store)
    }

    async fn connect(&self, host: &str) -> Result<Memcached, MtopError> {
        if let Some(cfg) = &self.config {
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

    fn host_to_server_name(host: &str) -> Result<ServerName, MtopError> {
        ServerName::try_from(host)
            .map_err(|e| MtopError::configuration_cause(format!("invalid server name '{}'", host), e))
    }

    pub async fn get(&self, host: &str) -> Result<PooledMemcached, MtopError> {
        let mut map = self.clients.lock().await;
        let inner = match map.remove(host) {
            Some(c) => c,
            None => self.connect(host).await?,
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

async fn plain_connect(host: &str) -> Result<Memcached, MtopError> {
    let tcp_stream = TcpStream::connect(host).await?;
    let (read, write) = tcp_stream.into_split();
    Ok(Memcached::new(read, write))
}

async fn tls_connect(host: &str, server: ServerName, config: Arc<ClientConfig>) -> Result<Memcached, MtopError> {
    let connector = TlsConnector::from(config);
    let tcp_stream = TcpStream::connect(host).await?;
    let tls_stream = connector.connect(server, tcp_stream).await?;
    let (read, write) = tokio::io::split(tls_stream);
    Ok(Memcached::new(read, write))
}
