use crate::core::MtopError;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::runtime::Handle;
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

const RESPONSE_VERSION: &str = "VERSION 1.6.22\r\n";
const RESPONSE_ERROR: &str = "ERROR\r\n";

#[derive(Debug, Clone)]
pub struct TlsServerConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: Option<PathBuf>,
}

/// Start a TLS stub server that responds to the Memcached `version` command for
/// integration testing. If the server cannot be started, the method will panic.
/// Any errors accepting or dealing with connections after the server has started
/// will be written to `stderr`. The address of the server and a future that must
/// be `tokio::spawn`'d are returned.
pub async fn tls_server<A>(
    config: TlsServerConfig,
    handle: Handle,
    address: A,
) -> (SocketAddr, impl Future<Output = ()>)
where
    A: ToSocketAddrs,
{
    let tcp_listener = TcpListener::bind(address).await.expect("error binding address");
    let local_address = tcp_listener.local_addr().expect("error getting local address");

    let server_config = tls_server_config(config, handle.clone())
        .await
        .expect("error building TLS configuration");
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

async fn tls_server_config(config: TlsServerConfig, handle: Handle) -> Result<Arc<ServerConfig>, MtopError> {
    fn server_config(tls: TlsServerConfig) -> Result<Arc<ServerConfig>, MtopError> {
        let cert = crate::net::load_cert(&tls.cert)?;
        let key = crate::net::load_key(&tls.key)?;
        let root = Arc::new(if let Some(p) = &tls.ca {
            crate::net::custom_root_store(crate::net::load_cert(p)?)?
        } else {
            crate::net::default_root_store()
        });

        let client_verifier = WebPkiClientVerifier::builder(root.clone()).build().unwrap();
        Ok(Arc::new(
            ServerConfig::builder()
                .with_client_cert_verifier(client_verifier.clone())
                .with_single_cert(cert, key)
                .unwrap(),
        ))
    }

    handle.spawn_blocking(move || server_config(config)).await.unwrap()
}
