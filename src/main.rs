use clap::Parser;
use std::fmt;
use std::fmt::Formatter;
use std::io::{self, Error as IOError, ErrorKind};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, Lines};
use tokio::net::TcpStream;
use tracing::{Instrument, Level};

const DEFAULT_LOG_LEVEL: Level = Level::INFO;

/// mtop: top for memcached
#[derive(Debug, Parser)]
#[clap(name = "mtop", version = clap::crate_version!())]
struct MtopApplication {
    /// Logging verbosity. Allowed values are 'trace', 'debug', 'info', 'warn', and 'error'
    /// (case insensitive)
    #[clap(long, default_value_t = DEFAULT_LOG_LEVEL)]
    log_level: Level,

    /// Memcached hosts to connect to in the form 'hostname:port'. Must be specified at least
    /// once and may be used multiple times (separated by spaces).
    #[clap(required = true)]
    hosts: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let opts = MtopApplication::parse();

    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(opts.log_level)
            .finish(),
    )
    .expect("failed to set tracing subscriber");

    for addr in opts.hosts.iter() {
        tracing::info!(message = "connecting", address = ?addr);
        let c = TcpStream::connect(addr).await?;
        let (r, w) = c.into_split();
        let mut rw = StatReader::new(r, w);

        let stats = rw.read_stats(StatsCommand::Default).await?;
        for kv in stats {
            println!("{:?}", kv);
        }
    }

    Ok(())
}

#[derive(Debug)]
#[allow(dead_code)]
struct RawStat {
    pub key: String,
    pub val: String,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
#[allow(dead_code)]
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
        self.write
            .write_all(format!("{}\r\n", cmd).as_bytes())
            .instrument(tracing::span!(Level::DEBUG, "send_command"))
            .await?;

        self.parse_stats()
            .instrument(tracing::span!(Level::DEBUG, "parse_response"))
            .await
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
