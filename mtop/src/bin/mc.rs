use clap::{Args, Parser, Subcommand, ValueHint};
use mtop_client::{MemcachedPool, Meta, TLSConfig, Value};
use std::error;
use std::io;
use std::path::PathBuf;
use std::{env, process};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::runtime::Handle;
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

    /// Enable TLS connections to the Memcached server.
    #[arg(long)]
    tls_enabled: bool,

    /// Optional certificate authority to use for validating the server certificate instead of
    /// the default root certificates.
    #[arg(long, value_hint = ValueHint::FilePath)]
    tls_ca: Option<PathBuf>,

    /// Optional server name to use for validating the server certificate. If not set, the
    /// hostname of the server is used for checking that the certificate matches the server.
    #[arg(long)]
    tls_server_name: Option<String>,

    /// Optional client certificate to use to authenticate with the Memcached server. Note that
    /// this may or may not be required based on how the Memcached server is configured.
    #[arg(long, requires = "tls_key", value_hint = ValueHint::FilePath)]
    tls_cert: Option<PathBuf>,

    /// Optional client key to use to authenticate with the Memcached server. Note that this may
    /// or may not be required based on how the Memcached server is configured.
    #[arg(long, requires = "tls_cert", value_hint = ValueHint::FilePath)]
    tls_key: Option<PathBuf>,

    #[command(subcommand)]
    mode: Action,
}

#[derive(Debug, Subcommand)]
enum Action {
    Delete(DeleteCommand),
    Get(GetCommand),
    Keys(KeysCommand),
    Set(SetCommand),
    Touch(TouchCommand),
}

/// Delete an item in the cache.
#[derive(Debug, Args)]
struct DeleteCommand {
    /// Key of the item to delete. If the item  does not exist the command will exit with an
    /// error status.
    #[arg(required = true)]
    key: String,
}

/// Get the value of an item in the cache.
#[derive(Debug, Args)]
struct GetCommand {
    /// Key of the item to get. The raw contents of this item will be written to standard out.
    /// This has the potential to mess up your terminal. Consider piping the output to a file
    /// or another tool to examine it.
    #[arg(required = true)]
    key: String,
}

/// Show keys for all items in the cache.
#[derive(Debug, Args)]
struct KeysCommand {
    /// Print key name, expiration as a UNIX timestamp, and value size in bytes as tab separated
    /// values instead of only the key name.
    #[arg(long)]
    details: bool,
}

/// Set a value in the cache.
///
/// The value will be read from standard input. You can use shell pipes or redirects to set
/// the contents of files as values.
#[derive(Debug, Args)]
struct SetCommand {
    /// Key of the item to set the value for.
    #[arg(required = true)]
    key: String,

    /// TTL to set for the item, in seconds. If the TTL is longer than the number of seconds
    /// in 30 days, it will be treated as a UNIX timestamp, setting the item to expire at a
    /// particular date/time.
    #[arg(required = true)]
    ttl: u32,
}

/// Update the TTL of an item in the cache.
#[derive(Debug, Args)]
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

    let pool = MemcachedPool::new(
        Handle::current(),
        TLSConfig {
            enabled: opts.tls_enabled,
            ca_path: opts.tls_ca,
            cert_path: opts.tls_cert,
            key_path: opts.tls_key,
            server_name: opts.tls_server_name,
        },
    )
    .await
    .unwrap_or_else(|e| {
        tracing::error!(message = "unable to initialize memcached client", host = opts.host, error = %e);
        process::exit(1);
    });

    let mut client = pool.get(&opts.host).await.unwrap_or_else(|e| {
        tracing::error!(message = "unable to connect", host = opts.host, error = %e);
        process::exit(1);
    });

    match opts.mode {
        Action::Delete(c) => {
            if let Err(e) = client.delete(c.key.clone()).await {
                tracing::error!(message = "unable to delete item", key = c.key, host = opts.host, error = %e);
                process::exit(1);
            }
        }
        Action::Get(c) => {
            let results = client.get(&[c.key.clone()]).await.unwrap_or_else(|e| {
                tracing::error!(message = "unable to get item", key = c.key, host = opts.host, error = %e);
                process::exit(1);
            });

            if let Some(v) = results.get(&c.key) {
                if let Err(e) = print_data(v).await {
                    tracing::warn!(message = "error writing output", error = %e);
                }
            }
        }
        Action::Keys(c) => {
            let mut metas = client.metas().await.unwrap_or_else(|e| {
                tracing::error!(message = "unable to list keys", host = opts.host, error = %e);
                process::exit(1);
            });

            metas.sort();
            if let Err(e) = print_keys(&metas, c.details).await {
                tracing::warn!(message = "error writing output", error = %e);
            }
        }
        Action::Set(c) => {
            let buf = read_input().await.unwrap_or_else(|e| {
                tracing::error!(message = "unable to read item data from stdin", error = %e);
                process::exit(1);
            });

            if let Err(e) = client.set(c.key.clone(), 0, c.ttl, buf).await {
                tracing::error!(message = "unable to set item", key = c.key, host = opts.host, error = %e);
                process::exit(1);
            }
        }
        Action::Touch(c) => {
            if let Err(e) = client.touch(c.key.clone(), c.ttl).await {
                tracing::error!(message = "unable to touch item", key = c.key, host = opts.host, error = %e);
                process::exit(1);
            }
        }
    }

    pool.put(client).await;
    Ok(())
}

async fn read_input() -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut input = BufReader::new(tokio::io::stdin());
    input.read_to_end(&mut buf).await?;
    Ok(buf)
}

async fn print_data(val: &Value) -> io::Result<()> {
    let mut output = BufWriter::new(tokio::io::stdout());
    output.write_all(&val.data).await?;
    output.flush().await
}

async fn print_keys(metas: &[Meta], show_details: bool) -> io::Result<()> {
    let mut output = BufWriter::new(tokio::io::stdout());
    for meta in metas {
        let line = if show_details {
            format!("{}\t{}\t{}\n", meta.key, meta.expires, meta.size)
        } else {
            format!("{}\n", meta.key)
        };

        output.write_all(line.as_bytes()).await?;
    }

    output.flush().await
}