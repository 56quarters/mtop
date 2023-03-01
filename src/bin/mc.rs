use clap::{Parser, ValueHint};
use mtop::client::{Item, MemcachedPool};
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
                tracing::error!(message = "unable to delete key", key = c.key, host = opts.host, error = %e);
                process::exit(1);
            }
        }
        SubCommand::Get(c) => {
            let results = client.get(vec![c.key.clone()]).await.unwrap_or_else(|e| {
                tracing::error!(message = "unable to get key", key = c.key, host = opts.host, error = %e);
                process::exit(1);
            });

            pool.put(client).await;
            if let Some(v) = results.get(&c.key) {
                let _ = io::stdout().write_all(&v.data);
            }
        }
        SubCommand::Keys(_) => {
            let mut keys = client.keys().await.unwrap_or_else(|e| {
                tracing::error!(message = "unable to list keys", host = opts.host, error = %e);
                process::exit(1);
            });

            pool.put(client).await;
            keys.sort();

            if let Err(e) = print_keys(&keys) {
                tracing::warn!(message = "error flushing output", error = %e);
            }
        }
        SubCommand::Touch(c) => {
            if let Err(e) = client.touch(c.key.clone(), c.ttl).await {
                tracing::error!(message = "unable to touch key", key = c.key, host = opts.host, error = %e);
                process::exit(1);
            }
        }
    }

    Ok(())
}

fn print_keys(items: &[Item]) -> io::Result<()> {
    let out = io::stdout().lock();
    let mut buf = BufWriter::new(out);

    for item in items {
        let _ = writeln!(buf, "{}", item.key);
    }

    buf.flush()
}
