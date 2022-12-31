use std::fmt;
use std::fmt::Formatter;
use std::io::{self, Error as IOError, ErrorKind};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter,
    Lines,
};
use tokio::net::TcpStream;

#[derive(Debug)]
struct RawStat {
    pub key: String,
    pub val: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("connecting...");
    let c = TcpStream::connect(("localhost", 11211)).await.unwrap();
    let (r, mut w) = c.into_split();

    let mut rw = StatReader::new(r, w);

    for i in 0..4 {
        let stats = rw.read_stats(StatsCommand::Items).await?;
        for kv in stats {
            println!("{:?}", kv);
        }
    }

    Ok(())
}

enum StatsCommand {
    Default,
    Items,
    Slabs,
    Sizes,
}

impl fmt::Display for StatsCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => write!(f, "stats"),
            Self::Items => write!(f, "stats items"),
            Self::Slabs => write!(f, "stats slabs"),
            Self::Sizes => write!(f, "stats sizes"),
        }
    }
}

struct StatReader<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    lines: Lines<BufReader<R>>,
    write: W,
}

impl<R, W> StatReader<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    fn new(read: R, write: W) -> Self {
        StatReader {
            lines: BufReader::new(read).lines(),
            write,
        }
    }

    async fn read_stats(&mut self, cmd: StatsCommand) -> io::Result<Vec<RawStat>> {
        println!("writing...");
        self.write
            .write_all(format!("{}\r\n", cmd).as_bytes())
            .await?;

        println!("reading...");
        self.parse_stats().await
    }

    async fn parse_stats(&mut self) -> io::Result<Vec<RawStat>> {
        let mut out = Vec::new();

        loop {
            let line = self.lines.next_line().await?;
            match line.as_deref() {
                Some("END") | None => break,
                Some(v) => {
                    let mut parts = v.split(' ');
                    match (parts.next(), parts.next(), parts.next()) {
                        (Some("STAT"), Some(key), Some(val)) => out.push(RawStat {
                            key: key.to_string(),
                            val: val.to_string(),
                        }),
                        _ => {
                            // TODO: Create our own error type with useful context
                            return Err(IOError::from(ErrorKind::InvalidData));
                        }
                    }
                }
            }
        }

        Ok(out)
    }
}
