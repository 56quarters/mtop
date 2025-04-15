use mtop_client::test::{tls_server, TlsServerConfig};
use mtop_client::{
    MemcachedClient, MemcachedClientConfig, RendezvousSelector, Server, ServerID, ServersResponse, TcpClientFactory,
    TlsConfig,
};
use rustls_pki_types::ServerName;
use std::path::PathBuf;
use tokio::runtime::Handle;

fn test_path(p: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests").join(p)
}

/// Start a stub server, connect to it using TLS, and run the `ping` method from the client against it.
async fn run_server(server_config: TlsServerConfig, client_config: TlsConfig) -> (ServerID, ServersResponse<()>) {
    let (addr, server) = tls_server(server_config, Handle::current(), "localhost:0").await;
    let id = ServerID::from(addr);
    let handle = tokio::spawn(server);

    let factory = TcpClientFactory::new(client_config, Handle::current()).await.unwrap();
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
            enabled: true,
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
            enabled: true,
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
            enabled: true,
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
            enabled: true,
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
