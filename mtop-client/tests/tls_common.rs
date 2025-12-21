#![allow(clippy::uninlined_format_args)]

use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::runtime::Handle;
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};

const RESPONSE_VERSION: &str = "VERSION 1.6.22\r\n";
const RESPONSE_ERROR: &str = "ERROR\r\n";

/// Return a full version of path `p` relative to the integration test directory.
pub fn test_path(p: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests").join(p)
}

pub async fn handle_client_connection<S>(stream: S)
where
    S: AsyncRead + AsyncWrite,
{
    let (read, write) = tokio::io::split(stream);
    let mut read = BufReader::new(read).lines();
    let mut write = BufWriter::new(write);

    loop {
        match read.next_line().await {
            Ok(Some(v)) => {
                let response = match v.trim_end() {
                    "version" => RESPONSE_VERSION.as_bytes(),
                    _ => RESPONSE_ERROR.as_bytes(),
                };

                if let Err(e) = write.write_all(response).await {
                    eprintln!("error writing to client, closing connection: {}", e);
                    return;
                }
                if let Err(e) = write.flush().await {
                    eprintln!("error flushing to client, closing connection: {}", e);
                    return;
                }
            }
            Ok(None) => {
                eprintln!("closing connection on EOF");
                return;
            }
            Err(e) => {
                eprintln!("error reading from socket: {}", e);
                return;
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TlsServerConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: Option<PathBuf>,
}

pub async fn tls_server_config(config: TlsServerConfig, handle: Handle) -> Arc<ServerConfig> {
    fn server_config(tls: TlsServerConfig) -> Arc<ServerConfig> {
        let cert = load_cert(&tls.cert);
        let key = load_key(&tls.key);
        let root = Arc::new(if let Some(p) = &tls.ca {
            custom_root_store(load_cert(p))
        } else {
            default_root_store()
        });

        let client_verifier = WebPkiClientVerifier::builder(root.clone()).build().unwrap();
        Arc::new(
            ServerConfig::builder()
                .with_client_cert_verifier(client_verifier.clone())
                .with_single_cert(cert, key)
                .unwrap(),
        )
    }

    handle.spawn_blocking(move || server_config(config)).await.unwrap()
}

fn load_cert(path: &PathBuf) -> Vec<CertificateDer<'static>> {
    CertificateDer::pem_file_iter(path).unwrap().map(|r| r.unwrap()).collect()
}

fn load_key(path: &PathBuf) -> PrivateKeyDer<'static> {
    PrivateKeyDer::from_pem_file(path).unwrap()
}

fn custom_root_store(ca: Vec<CertificateDer<'static>>) -> RootCertStore {
    let mut store = RootCertStore::empty();
    for cert in ca {
        store.add(cert).unwrap()
    }

    store
}

fn default_root_store() -> RootCertStore {
    let mut store = RootCertStore::empty();
    store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().map(|c| c.to_owned()));
    store
}
