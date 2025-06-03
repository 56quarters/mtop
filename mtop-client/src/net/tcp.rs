use crate::core::MtopError;
use rustls_pki_types::ServerName;
use std::fmt;
use std::sync::Arc;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::ClientConfig;

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
