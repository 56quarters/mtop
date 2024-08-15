use crate::core::MtopError;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use std::fmt::{self, Debug};
use std::fs::File;
use std::io::{self, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::runtime::Handle;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

/// Configuration for establishing a TLS connection to server with optional mTLS.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Enable TLS connections to the server.
    pub enabled: bool,

    /// Path to a custom certificate authority. If not supplied, default root certificates
    /// from the `webpki_roots` crate are used.
    pub ca_path: Option<PathBuf>,

    /// Path to a PEM format client certificate for mTLS. If not supplied, no client authentication
    /// is used when connecting to the server.
    pub cert_path: Option<PathBuf>,

    /// Path to a PEM format client key for mTLS. If not supplied, no client authentication is used
    /// when connecting to the server.
    pub key_path: Option<PathBuf>,

    /// Name of the server for validating its certificate. If not supplied, the hostname
    /// of the server is used instead.
    pub server_name: Option<ServerName<'static>>,
}

pub(crate) async fn tls_client_config(config: TlsConfig, handle: Handle) -> Result<ClientConfig, MtopError> {
    fn client_config(tls: TlsConfig) -> Result<ClientConfig, MtopError> {
        let client_cert = if let Some(p) = &tls.cert_path {
            Some(load_cert(p)?)
        } else {
            None
        };

        let client_key = if let Some(p) = &tls.key_path {
            Some(load_key(p)?)
        } else {
            None
        };

        let root = if let Some(p) = &tls.ca_path {
            custom_root_store(load_cert(p)?)?
        } else {
            default_root_store()
        };

        let builder = ClientConfig::builder().with_root_certificates(root);
        let client_config = match (client_cert, client_key, tls.cert_path, tls.key_path) {
            (Some(cert), Some(key), Some(cert_path), Some(key_path)) => {
                tracing::debug!(message = "using key and cert for client authentication", key = ?key_path, cert = ?cert_path);
                builder
                    .with_client_auth_cert(cert, key)
                    .map_err(|e| MtopError::configuration_cause("unable to use client cert or key", e))?
            }
            _ => {
                tracing::debug!(message = "not using any client authentication");
                builder.with_no_client_auth()
            }
        };

        Ok(client_config)
    }

    handle.spawn_blocking(move || client_config(config)).await.unwrap()
}

pub(crate) fn load_cert(path: &PathBuf) -> Result<Vec<CertificateDer<'static>>, MtopError> {
    File::open(path)
        .map(BufReader::new)
        .map(|mut r| rustls_pemfile::certs(&mut r).collect::<Vec<_>>())
        .and_then(|v| v.into_iter().collect::<Result<Vec<CertificateDer<'static>>, io::Error>>())
        .map_err(|e| MtopError::configuration_cause(format!("unable to load or parse cert {:?}", path), e))
}

pub(crate) fn load_key(path: &PathBuf) -> Result<PrivateKeyDer<'static>, MtopError> {
    File::open(path)
        .map(BufReader::new)
        .and_then(|mut r| rustls_pemfile::private_key(&mut r))
        .map_err(|e| MtopError::configuration_cause(format!("unable to load key {:?}", path), e))?
        .ok_or_else(|| MtopError::configuration(format!("no keys available in {:?}", path)))
}

pub(crate) fn custom_root_store(ca: Vec<CertificateDer<'static>>) -> Result<RootCertStore, MtopError> {
    let mut store = RootCertStore::empty();
    for cert in ca {
        store
            .add(cert)
            .map_err(|e| MtopError::configuration_cause("unable to parse CA cert", e))?;
    }

    Ok(store)
}

pub(crate) fn default_root_store() -> RootCertStore {
    let mut store = RootCertStore::empty();
    store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().map(|c| c.to_owned()));
    store
}

pub(crate) async fn tcp_connect<A>(host: A) -> Result<(ReadHalf<TcpStream>, WriteHalf<TcpStream>), MtopError>
where
    A: ToSocketAddrs + fmt::Display,
{
    let tcp_stream = tcp_stream(host).await?;
    Ok(tokio::io::split(tcp_stream))
}

pub(crate) async fn tcp_tls_connect<A>(
    host: A,
    server: ServerName<'static>,
    config: Arc<ClientConfig>,
) -> Result<(ReadHalf<TlsStream<TcpStream>>, WriteHalf<TlsStream<TcpStream>>), MtopError>
where
    A: ToSocketAddrs + fmt::Display,
{
    let tcp_stream = tcp_stream(host).await?;
    let connector = TlsConnector::from(config);
    let tls_stream = connector.connect(server, tcp_stream).await?;
    Ok(tokio::io::split(tls_stream))
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
