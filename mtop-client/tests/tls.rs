#![allow(clippy::uninlined_format_args)]

use mtop_client::{
    MemcachedClient, MemcachedClientConfig, RendezvousSelector, Server, ServerID, ServersResponse, TlsConfig,
    TlsTcpClientFactory,
};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::runtime::Handle;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};

const RESPONSE_VERSION: &str = "VERSION 1.6.22\r\n";
const RESPONSE_ERROR: &str = "ERROR\r\n";

#[derive(Debug, Clone)]
struct TlsServerConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: Option<PathBuf>,
}

/// Start a TLS stub server that responds to the Memcached `version` command for
/// integration testing. If the server cannot be started, the method will panic.
/// Any errors accepting or dealing with connections after the server has started
/// will be written to `stderr`. The address of the server and a future that must
/// be `tokio::spawn`'d are returned.
async fn tls_server<A>(config: TlsServerConfig, handle: Handle, address: A) -> (SocketAddr, impl Future<Output = ()>)
where
    A: ToSocketAddrs,
{
    let tcp_listener = TcpListener::bind(address).await.expect("error binding address");
    let local_address = tcp_listener.local_addr().expect("error getting local address");

    let server_config = tls_server_config(config, handle.clone()).await;
    let tls_acceptor = TlsAcceptor::from(server_config.clone());

    (local_address, async move {
        loop {
            let (socket, remote_addr) = match tcp_listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("error accepting TCP stream from client: {}", e);
                    continue;
                }
            };

            let tls_acceptor = tls_acceptor.clone();
            handle.spawn(async move {
                let tls_stream = match tls_acceptor.accept(socket).await {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("error accepting TLS connection from {}: {}", remote_addr, e);
                        return;
                    }
                };

                handle_client_connection(tls_stream).await;
            });
        }
    })
}

async fn handle_client_connection<S>(stream: S)
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

async fn tls_server_config(config: TlsServerConfig, handle: Handle) -> Arc<ServerConfig> {
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

fn test_path(p: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests").join(p)
}

/// Start a stub server, connect to it using TLS, and run the `ping` method from the client against it.
async fn run_server(server_config: TlsServerConfig, client_config: TlsConfig) -> (ServerID, ServersResponse<()>) {
    let (addr, server) = tls_server(server_config, Handle::current(), "localhost:0").await;
    let id = ServerID::from(addr);
    let handle = tokio::spawn(server);

    let factory = TlsTcpClientFactory::new(client_config).await.unwrap();
    let selector = RendezvousSelector::new(vec![Server::new(
        id.clone(),
        ServerName::try_from("localhost").unwrap(),
    )]);

    let cfg = MemcachedClientConfig::default();
    let client = MemcachedClient::new(cfg, Handle::current(), selector, factory);
    let res = client.ping().await.unwrap();
    handle.abort();

    (id, res)
}

#[tokio::test]
async fn test_tls_client_mtls_default_roots_server() {
    let (server_id, res) = run_server(
        TlsServerConfig {
            cert: test_path("certs/memcached-server-cert.pem"),
            key: test_path("certs/memcached-server-key.pem"),
            ca: None,
        },
        TlsConfig {
            ca_path: Some(test_path("certs/memcached-ca-cert.pem")),
            cert_path: Some(test_path("certs/memcached-client-cert.pem")),
            key_path: Some(test_path("certs/memcached-client-key.pem")),
            server_name: None,
        },
    )
    .await;

    assert!(
        res.errors.contains_key(&server_id),
        "expected errors to contain server ID {}, was {:?}",
        server_id,
        res.errors
    );
    assert!(
        res.values.is_empty(),
        "expected values to be empty, was {:?}",
        res.values
    );
}

#[tokio::test]
async fn test_tls_client_mtls_invalid_client_cert() {
    let (server_id, res) = run_server(
        TlsServerConfig {
            cert: test_path("certs/memcached-server-cert.pem"),
            key: test_path("certs/memcached-server-key.pem"),
            ca: Some(test_path("certs/memcached-ca-cert.pem")),
        },
        TlsConfig {
            ca_path: Some(test_path("certs/memcached-ca-cert.pem")),
            cert_path: Some(test_path("certs/memcached-bad-client-cert.pem")),
            key_path: Some(test_path("certs/memcached-bad-client-key.pem")),
            server_name: None,
        },
    )
    .await;

    assert!(
        res.errors.contains_key(&server_id),
        "expected errors to contain server ID {}, was {:?}",
        server_id,
        res.errors
    );
    assert!(
        res.values.is_empty(),
        "expected values to be empty, was {:?}",
        res.values
    );
}

#[tokio::test]
async fn test_tls_client_mtls_missing_client_cert() {
    let (server_id, res) = run_server(
        TlsServerConfig {
            cert: test_path("certs/memcached-server-cert.pem"),
            key: test_path("certs/memcached-server-key.pem"),
            ca: Some(test_path("certs/memcached-ca-cert.pem")),
        },
        TlsConfig {
            ca_path: Some(test_path("certs/memcached-ca-cert.pem")),
            cert_path: None,
            key_path: None,
            server_name: None,
        },
    )
    .await;

    assert!(
        res.errors.contains_key(&server_id),
        "expected errors to contain server ID {}, was {:?}",
        server_id,
        res.errors
    );
    assert!(
        res.values.is_empty(),
        "expected values to be empty, was {:?}",
        res.values
    );
}

#[tokio::test]
async fn test_tls_client_mtls_success() {
    let (server_id, res) = run_server(
        TlsServerConfig {
            cert: test_path("certs/memcached-server-cert.pem"),
            key: test_path("certs/memcached-server-key.pem"),
            ca: Some(test_path("certs/memcached-ca-cert.pem")),
        },
        TlsConfig {
            ca_path: Some(test_path("certs/memcached-ca-cert.pem")),
            cert_path: Some(test_path("certs/memcached-client-cert.pem")),
            key_path: Some(test_path("certs/memcached-client-key.pem")),
            server_name: None,
        },
    )
    .await;

    assert!(
        res.errors.is_empty(),
        "expected errors to be empty, was {:?}",
        res.errors
    );
    assert!(
        res.values.contains_key(&server_id),
        "expected values to contain server ID {}, was {:?}",
        server_id,
        res.values
    );
}
