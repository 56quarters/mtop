use crate::MtopError;
use rustls_pki_types::ServerName;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::UnixStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::ClientConfig;

pub(crate) async fn unix_connect<P>(path: P) -> Result<(ReadHalf<UnixStream>, WriteHalf<UnixStream>), MtopError>
where
    P: AsRef<Path>,
{
    let unix_stream = UnixStream::connect(path).await.map_err(MtopError::from)?;
    Ok(tokio::io::split(unix_stream))
}

pub(crate) async fn unix_tls_connect<P>(
    path: P,
    server: ServerName<'static>,
    config: Arc<ClientConfig>,
) -> Result<(ReadHalf<TlsStream<UnixStream>>, WriteHalf<TlsStream<UnixStream>>), MtopError>
where
    P: AsRef<Path>,
{
    let unix_stream = UnixStream::connect(path).await.map_err(MtopError::from)?;
    let connector = TlsConnector::from(config);
    let unix_tls_stream = connector.connect(server, unix_stream).await?;
    Ok(tokio::io::split(unix_tls_stream))
}
