#![allow(clippy::uninlined_format_args)]
#![cfg(unix)]

mod tls_common;

use crate::tls_common::{TlsServerConfig, handle_client_connection, test_path, tls_server_config};
use mtop_client::{
    MemcachedClient, MemcachedClientConfig, RendezvousSelector, Server, ServerID, ServersResponse, TlsConfig,
    TlsUnixClientFactory,
};
use rustls_pki_types::ServerName;
use std::path::{Path, PathBuf};
use std::{env, fs};
use tokio::net::UnixListener;
use tokio::runtime::Handle;
use tokio_rustls::TlsAcceptor;

/// Start a UNIX TLS stub server that responds to the Memcached `version` command for
/// integration testing. If the server cannot be started, the method will panic.
/// Any errors accepting or dealing with connections after the server has started
/// will be written to `stderr`. A future that must be `tokio::spawn`'d is returned.
async fn tls_unix_server<P>(config: TlsServerConfig, handle: Handle, path: P) -> impl Future<Output = ()>
where
    P: AsRef<Path>,
{
    eprintln!("PATH: {:?}, LEN: {}", path.as_ref(), path.as_ref().as_os_str().len());

    let unix_listener = UnixListener::bind(path).expect("error binding path");
    let server_config = tls_server_config(config, handle.clone()).await;
    let tls_acceptor = TlsAcceptor::from(server_config.clone());

    async move {
        loop {
            let (stream, remote_addr) = match unix_listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("error accepting TCP stream from client: {}", e);
                    continue;
                }
            };

            let tls_acceptor = tls_acceptor.clone();
            handle.spawn(async move {
                let tls_stream = match tls_acceptor.accept(stream).await {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("error accepting TLS connection from {:?}: {}", remote_addr, e);
                        return;
                    }
                };

                handle_client_connection(tls_stream).await;
            });
        }
    }
}

#[derive(Debug)]
struct TempUnixSocket {
    base: PathBuf,
    sock: PathBuf,
}

impl TempUnixSocket {
    pub fn new<P1, P2>(prefix: P1, name: P2) -> Self
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
    {
        let base = env::temp_dir().join(prefix);
        let sock = base.join(name);

        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).unwrap();
        TempUnixSocket { base, sock }
    }

    pub fn path(&self) -> PathBuf {
        self.sock.clone()
    }
}

impl Drop for TempUnixSocket {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.base);
    }
}

/// Start a stub server, connect to it using TLS, and run the `ping` method from the client against it.
async fn run_server<P>(
    test: P,
    server_config: TlsServerConfig,
    client_config: TlsConfig,
) -> (ServerID, ServersResponse<()>)
where
    P: AsRef<Path>,
{
    let sock = TempUnixSocket::new(test, "memcached.sock");
    let id = ServerID::Path(sock.path().to_path_buf());
    let server = tls_unix_server(server_config, Handle::current(), sock.path()).await;
    let handle = tokio::spawn(server);

    let factory = TlsUnixClientFactory::new(client_config).await.unwrap();
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

// NOTE: The test names are used as part of the path of the UNIX socket created
// for each test. The names are purposefully short to avoid hitting the max path
// length limit on macOS (104 chars). See https://unix.stackexchange.com/q/367008

#[tokio::test]
async fn test_tls_unix_client_mtls_default_roots_server() {
    let (server_id, res) = run_server(
        "mtls_default_roots_server",
        TlsServerConfig {
            cert: test_path("certs/memcached-server-cert.pem"),
            key: test_path("certs/memcached-server-key.pem"),
            ca: None,
        },
        TlsConfig {
            ca_path: Some(test_path("certs/memcached-ca-cert.pem")),
            cert_path: Some(test_path("certs/memcached-client-cert.pem")),
            key_path: Some(test_path("certs/memcached-client-key.pem")),
            server_name: Some(ServerName::try_from("localhost").unwrap()),
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
async fn test_tls_unix_client_mtls_invalid_client_cert() {
    let (server_id, res) = run_server(
        "mtls_invalid_client_cert",
        TlsServerConfig {
            cert: test_path("certs/memcached-server-cert.pem"),
            key: test_path("certs/memcached-server-key.pem"),
            ca: Some(test_path("certs/memcached-ca-cert.pem")),
        },
        TlsConfig {
            ca_path: Some(test_path("certs/memcached-ca-cert.pem")),
            cert_path: Some(test_path("certs/memcached-bad-client-cert.pem")),
            key_path: Some(test_path("certs/memcached-bad-client-key.pem")),
            server_name: Some(ServerName::try_from("localhost").unwrap()),
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
async fn test_tls_unix_client_mtls_missing_client_cert() {
    let (server_id, res) = run_server(
        "mtls_missing_client_cert",
        TlsServerConfig {
            cert: test_path("certs/memcached-server-cert.pem"),
            key: test_path("certs/memcached-server-key.pem"),
            ca: Some(test_path("certs/memcached-ca-cert.pem")),
        },
        TlsConfig {
            ca_path: Some(test_path("certs/memcached-ca-cert.pem")),
            cert_path: None,
            key_path: None,
            server_name: Some(ServerName::try_from("localhost").unwrap()),
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
async fn test_tls_unix_client_mtls_success() {
    let (server_id, res) = run_server(
        "mtls_success",
        TlsServerConfig {
            cert: test_path("certs/memcached-server-cert.pem"),
            key: test_path("certs/memcached-server-key.pem"),
            ca: Some(test_path("certs/memcached-ca-cert.pem")),
        },
        TlsConfig {
            ca_path: Some(test_path("certs/memcached-ca-cert.pem")),
            cert_path: Some(test_path("certs/memcached-client-cert.pem")),
            key_path: Some(test_path("certs/memcached-client-key.pem")),
            server_name: Some(ServerName::try_from("localhost").unwrap()),
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
