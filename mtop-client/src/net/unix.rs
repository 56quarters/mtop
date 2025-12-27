use crate::MtopError;
use std::path::Path;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::UnixStream;

pub(crate) async fn unix_connect<P>(path: P) -> Result<(ReadHalf<UnixStream>, WriteHalf<UnixStream>), MtopError>
where
    P: AsRef<Path>,
{
    let unix_stream = UnixStream::connect(path).await.map_err(MtopError::from)?;
    Ok(tokio::io::split(unix_stream))
}
