use clap::{Parser, ValueHint};
use mtop::client::{MemcachedPool, Meta, Value};
use std::error;
use std::io::{self, BufWriter, Write};
use std::{env, process};
use tracing::Level;

const DEFAULT_LOG_LEVEL: Level = Level::INFO;
const DEFAULT_HOST: &str = "localhost:11211";

/// mc: memcached command line utility
#[derive(Debug, Parser)]
#[command(name = "mc", version = clap::crate_version!())]
struct McConfig {
    /// Logging verbosity. Allowed values are 'trace', 'debug', 'info', 'warn', and 'error'
    /// (case insensitive).
    #[arg(long, default_value_t = DEFAULT_LOG_LEVEL)]
    log_level: Level,

    /// Memcached host to connect to in the form 'hostname:port'.
    #[arg(long, default_value_t = DEFAULT_HOST.to_owned(), value_hint = ValueHint::Hostname)]
    host: String,

    #[command(subcommand)]
    mode: SubCommand,
}

#[derive(Debug, Parser)]
enum SubCommand {
    Delete(DeleteCommand),
    Get(GetCommand),
    Keys(KeysCommand),
    Touch(TouchCommand),
}

/// Delete an item associated with a key.
#[derive(Debug, Parser)]
struct DeleteCommand {
    /// Key of the item to delete. If the item  does not exist the command will exit with an
    /// error status.
    #[arg(required = true)]
    key: String,
}

/// Get the value of an item associated with a key.
#[derive(Debug, Parser)]
struct GetCommand {
    /// Key of the item to get. The raw contents of this item will be written to standard out.
    /// This has the potential to mess up your terminal. Consider piping the output to a file
    /// or another tool to examine it.
    #[arg(required = true)]
    key: String,
}

/// Show keys for all items in a server.
#[derive(Debug, Parser)]
struct KeysCommand;

/// Update the TTL of an item associated with a key.
#[derive(Debug, Parser)]
struct TouchCommand {
    /// Key of the item to update the TTL of. If the item does not exist the command will exit
    /// with an error status.
    #[arg(required = true)]
    key: String,

    /// TTL to set for the item, in seconds. If the TTL is longer than the number of seconds
    /// in 30 days, it will be treated as a UNIX timestamp, setting the item to expire at a
    /// particular date/time.
    #[arg(required = true)]
    ttl: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error + Send + Sync>> {
    let opts = McConfig::parse();

    let console_subscriber = mtop::tracing::console_subscriber(opts.log_level)?;
    tracing::subscriber::set_global_default(console_subscriber).expect("failed to initialize console logging");

    let pool = MemcachedPool::new();
    let mut client = pool.get(&opts.host).await.unwrap_or_else(|e| {
        tracing::error!(message = "unable to connect", host = opts.host, error = %e);
        process::exit(1);
    });

    match opts.mode {
        SubCommand::Delete(c) => {
            if let Err(e) = client.delete(c.key.clone()).await {
                tracing::error!(message = "unable to delete item", key = c.key, host = opts.host, error = %e);
                process::exit(1);
            }
        }
        SubCommand::Get(c) => {
            let results = client.get(&[c.key.clone()]).await.unwrap_or_else(|e| {
                tracing::error!(message = "unable to get item", key = c.key, host = opts.host, error = %e);
                process::exit(1);
            });

            if let Some(v) = results.get(&c.key) {
                if let Err(e) = print_data(v) {
                    tracing::warn!(message = "error writing output", error = %e);
                }
            }
        }
        SubCommand::Keys(_) => {
            let mut metas = client.metas().await.unwrap_or_else(|e| {
                tracing::error!(message = "unable to list keys", host = opts.host, error = %e);
                process::exit(1);
            });

            metas.sort();
            if let Err(e) = print_keys(&metas) {
                tracing::warn!(message = "error writing output", error = %e);
            }
        }
        SubCommand::Touch(c) => {
            if let Err(e) = client.touch(c.key.clone(), c.ttl).await {
                tracing::error!(message = "unable to touch item", key = c.key, host = opts.host, error = %e);
                process::exit(1);
            }
        }
    }

    pool.put(client).await;
    Ok(())
}

fn print_data(val: &Value) -> io::Result<()> {
    let out = io::stdout().lock();
    let mut buf = BufWriter::new(out);

    buf.write_all(&val.data)?;
    buf.flush()
}

fn print_keys(metas: &[Meta]) -> io::Result<()> {
    let out = io::stdout().lock();
    let mut buf = BufWriter::new(out);

    for meta in metas {
        let _ = writeln!(buf, "{}", meta.key);
    }

    buf.flush()
}
