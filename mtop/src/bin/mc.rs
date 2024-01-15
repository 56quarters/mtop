use clap::{Args, Parser, Subcommand, ValueHint};
use mtop::check::{Checker, MeasurementBundle};
use mtop_client::{
    MemcachedClient, MemcachedPool, Meta, MtopError, PoolConfig, SelectorRendezvous, Server, TLSConfig, Timeout, Value,
};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;
use std::{env, io};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::runtime::Handle;
use tracing::{Instrument, Level};

const DEFAULT_LOG_LEVEL: Level = Level::INFO;
const DEFAULT_HOST: &str = "localhost:11211";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

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
    Add(AddCommand),
    Check(CheckCommand),
    Decr(DecrCommand),
    Delete(DeleteCommand),
    Get(GetCommand),
    Incr(IncrCommand),
    Keys(KeysCommand),
    Replace(ReplaceCommand),
    Set(SetCommand),
    Touch(TouchCommand),
}

/// Add a value to the cache only if it does not already exist.
///
/// The value will be read from standard input. You can use shell pipes or redirects to set
/// the contents of files as values.
#[derive(Debug, Args)]
struct AddCommand {
    /// Key of the item to add the value for.
    #[arg(required = true)]
    key: String,

    /// TTL to set for the item, in seconds. If the TTL is longer than the number of seconds
    /// in 30 days, it will be treated as a UNIX timestamp, setting the item to expire at a
    /// particular date/time.
    #[arg(required = true)]
    ttl: u32,
}

/// Run health checks against the cache.
#[derive(Debug, Args)]
struct CheckCommand {
    /// How long to run the checks for in seconds.
    #[arg(long, default_value_t = 60)]
    time_secs: u64,

    /// Timeout for each portion of the check (DNS, connection, set, get) in seconds.
    #[arg(long, default_value_t = 5)]
    timeout_secs: u64,

    /// How long to wait between each health check in milliseconds.
    #[arg(long, default_value_t = 100)]
    delay_millis: u64,
}

/// Decrement the value of an item in the cache.
#[derive(Debug, Args)]
struct DecrCommand {
    /// Key of the value to decrement. If the value does not exist the command will exit with
    /// an error status.
    #[arg(required = true)]
    key: String,

    /// Amount to decrement the value by.
    #[arg(required = true)]
    delta: u64,
}

/// Delete an item in the cache.
#[derive(Debug, Args)]
struct DeleteCommand {
    /// Key of the item to delete. If the item does not exist the command will exit with an
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

/// Increment the value of an item in the cach
#[derive(Debug, Args)]
struct IncrCommand {
    /// Key of the value to increment. If the value does not exist the command will exit with
    /// an error status.
    #[arg(required = true)]
    key: String,

    /// Amount to increment the value by.
    #[arg(required = true)]
    delta: u64,
}

/// Show keys for all items in the cache.
#[derive(Debug, Args)]
struct KeysCommand {
    /// Print key name, expiration as a UNIX timestamp, and value size in bytes as tab separated
    /// values instead of only the key name.
    #[arg(long)]
    details: bool,
}

/// Replace a value in the cache only if it already exists.
///
/// The value will be read from standard input. You can use shell pipes or redirects to set
/// the contents of files as values.
#[derive(Debug, Args)]
struct ReplaceCommand {
    /// Key of the item to replace the value for.
    #[arg(required = true)]
    key: String,

    /// TTL to set for the item, in seconds. If the TTL is longer than the number of seconds
    /// in 30 days, it will be treated as a UNIX timestamp, setting the item to expire at a
    /// particular date/time.
    #[arg(required = true)]
    ttl: u32,
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
async fn main() -> ExitCode {
    let opts = McConfig::parse();

    let console_subscriber =
        mtop::tracing::console_subscriber(opts.log_level).expect("failed to setup console logging");
    tracing::subscriber::set_global_default(console_subscriber).expect("failed to initialize console logging");

    let client = match new_client(&opts).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(message = "unable to initialize memcached client", host = opts.host, err = %e);
            return ExitCode::FAILURE;
        }
    };

    // Hardcoded timeout so that we can ensure the host is actually up.
    if let Err(e) = connect(&client, CONNECT_TIMEOUT).await {
        tracing::error!(message = "unable to connect", host = opts.host, err = %e);
        return ExitCode::FAILURE;
    };

    match &opts.mode {
        Action::Add(cmd) => run_add(&opts, cmd, &client).await,
        Action::Check(cmd) => run_check(&opts, cmd, &client).await,
        Action::Decr(cmd) => run_decr(&opts, cmd, &client).await,
        Action::Delete(cmd) => run_delete(&opts, cmd, &client).await,
        Action::Get(cmd) => run_get(&opts, cmd, &client).await,
        Action::Incr(cmd) => run_incr(&opts, cmd, &client).await,
        Action::Keys(cmd) => run_keys(&opts, cmd, &client).await,
        Action::Replace(cmd) => run_replace(&opts, cmd, &client).await,
        Action::Set(cmd) => run_set(&opts, cmd, &client).await,
        Action::Touch(cmd) => run_touch(&opts, cmd, &client).await,
    }
}

async fn new_client(opts: &McConfig) -> Result<MemcachedClient, MtopError> {
    let tls = TLSConfig {
        enabled: opts.tls_enabled,
        ca_path: opts.tls_ca.clone(),
        cert_path: opts.tls_cert.clone(),
        key_path: opts.tls_key.clone(),
        server_name: opts.tls_server_name.clone(),
    };

    MemcachedPool::new(
        Handle::current(),
        PoolConfig {
            tls,
            ..Default::default()
        },
    )
    .await
    .map(|pool| {
        let selector = SelectorRendezvous::new(vec![Server::new(&opts.host)]);
        MemcachedClient::new(Handle::current(), selector, pool)
    })
}

async fn connect(client: &MemcachedClient, timeout: Duration) -> Result<(), MtopError> {
    let pings = client
        .ping()
        .timeout(timeout, "client.ping")
        .instrument(tracing::span!(Level::INFO, "client.ping"))
        .await?;

    if let Some((_server, err)) = pings.errors.into_iter().next() {
        return Err(err);
    }

    Ok(())
}

async fn run_add(opts: &McConfig, cmd: &AddCommand, client: &MemcachedClient) -> ExitCode {
    let buf = match read_input().await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(message = "unable to read item data from stdin", err = %e);
            return ExitCode::FAILURE;
        }
    };

    if let Err(e) = client.add(&cmd.key, 0, cmd.ttl, &buf).await {
        tracing::error!(message = "unable to add item", key = cmd.key, host = opts.host, err = %e);
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_check(opts: &McConfig, cmd: &CheckCommand, client: &MemcachedClient) -> ExitCode {
    let checker = Checker::new(
        client,
        Duration::from_millis(cmd.delay_millis),
        Duration::from_secs(cmd.timeout_secs),
    );
    let results = checker.run(&opts.host, Duration::from_secs(cmd.time_secs)).await;
    if let Err(e) = print_check_results(&results).await {
        tracing::warn!(message = "error writing output", err = %e);
    }

    if results.failures.total > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_decr(opts: &McConfig, cmd: &DecrCommand, client: &MemcachedClient) -> ExitCode {
    if let Err(e) = client.decr(&cmd.key, cmd.delta).await {
        tracing::error!(message = "unable to decrement value", key = cmd.key, host = opts.host, err = %e);
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_delete(opts: &McConfig, cmd: &DeleteCommand, client: &MemcachedClient) -> ExitCode {
    if let Err(e) = client.delete(&cmd.key).await {
        tracing::error!(message = "unable to delete item", key = cmd.key, host = opts.host, err = %e);
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_get(opts: &McConfig, cmd: &GetCommand, client: &MemcachedClient) -> ExitCode {
    let response = match client.get(&[cmd.key.clone()]).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(message = "unable to get item", key = cmd.key, host = opts.host, err = %e);
            return ExitCode::FAILURE;
        }
    };

    if let Some(v) = response.values.get(&cmd.key) {
        if let Err(e) = print_data(v).await {
            tracing::warn!(message = "error writing output", err = %e);
        }
    }

    for (server, e) in response.errors.iter() {
        tracing::error!(message = "error fetching value", host = server.name, err = %e);
    }

    if response.has_errors() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_incr(opts: &McConfig, cmd: &IncrCommand, client: &MemcachedClient) -> ExitCode {
    if let Err(e) = client.incr(&cmd.key, cmd.delta).await {
        tracing::error!(message = "unable to increment value", key = cmd.key, host = opts.host, err = %e);
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_keys(opts: &McConfig, cmd: &KeysCommand, client: &MemcachedClient) -> ExitCode {
    let mut response = match client.metas().await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(message = "unable to list keys", host = opts.host, err = %e);
            return ExitCode::FAILURE;
        }
    };

    let mut metas = response.values.remove(&Server::new(&opts.host)).unwrap_or_default();
    metas.sort();

    if let Err(e) = print_keys(&metas, cmd.details).await {
        tracing::warn!(message = "error writing output", err = %e);
    }

    for (server, e) in response.errors.iter() {
        tracing::error!(message = "error fetching metas", host = server.name, err = %e);
    }

    if response.has_errors() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_replace(opts: &McConfig, cmd: &ReplaceCommand, client: &MemcachedClient) -> ExitCode {
    let buf = match read_input().await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(message = "unable to read item data from stdin", err = %e);
            return ExitCode::FAILURE;
        }
    };

    if let Err(e) = client.replace(&cmd.key, 0, cmd.ttl, &buf).await {
        tracing::error!(message = "unable to replace item", key = cmd.key, host = opts.host, err = %e);
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_set(opts: &McConfig, cmd: &SetCommand, client: &MemcachedClient) -> ExitCode {
    let buf = match read_input().await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(message = "unable to read item data from stdin", err = %e);
            return ExitCode::FAILURE;
        }
    };

    if let Err(e) = client.set(&cmd.key, 0, cmd.ttl, &buf).await {
        tracing::error!(message = "unable to set item", key = cmd.key, host = opts.host, err = %e);
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_touch(opts: &McConfig, cmd: &TouchCommand, client: &MemcachedClient) -> ExitCode {
    if let Err(e) = client.touch(&cmd.key, cmd.ttl).await {
        tracing::error!(message = "unable to touch item", key = cmd.key, host = opts.host, err = %e);
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
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

    if show_details {
        for meta in metas {
            output
                .write_all(format!("{}\t{}\t{}\n", meta.key, meta.expires, meta.size).as_bytes())
                .await?;
        }
    } else {
        for meta in metas {
            output.write_all(format!("{}\n", meta.key).as_bytes()).await?;
        }
    }

    output.flush().await
}

async fn print_check_results(results: &MeasurementBundle) -> io::Result<()> {
    let mut output = BufWriter::new(tokio::io::stdout());

    output
        .write_all(
            format!(
                "type=min total={:.9} dns={:.9} connection={:.9} set={:.9} get={:.9}\n",
                results.total.min.as_secs_f64(),
                results.dns.min.as_secs_f64(),
                results.connections.min.as_secs_f64(),
                results.sets.min.as_secs_f64(),
                results.gets.min.as_secs_f64()
            )
            .as_bytes(),
        )
        .await?;

    output
        .write_all(
            format!(
                "type=max total={:.9} dns={:.9} connection={:.9} set={:.9} get={:.9}\n",
                results.total.max.as_secs_f64(),
                results.dns.max.as_secs_f64(),
                results.connections.max.as_secs_f64(),
                results.sets.max.as_secs_f64(),
                results.gets.max.as_secs_f64(),
            )
            .as_bytes(),
        )
        .await?;

    output
        .write_all(
            format!(
                "type=avg total={:.9} dns={:.9} connection={:.9} set={:.9} get={:.9}\n",
                results.total.avg.as_secs_f64(),
                results.dns.avg.as_secs_f64(),
                results.connections.avg.as_secs_f64(),
                results.sets.avg.as_secs_f64(),
                results.gets.avg.as_secs_f64()
            )
            .as_bytes(),
        )
        .await?;

    output
        .write_all(
            format!(
                "type=stddev total={:.9} dns={:.9} connection={:.9} set={:.9} get={:.9}\n",
                results.total.std_dev.as_secs_f64(),
                results.dns.std_dev.as_secs_f64(),
                results.connections.std_dev.as_secs_f64(),
                results.sets.std_dev.as_secs_f64(),
                results.gets.std_dev.as_secs_f64()
            )
            .as_bytes(),
        )
        .await?;

    output
        .write_all(
            format!(
                "type=failures total={} dns={} connection={} set={} get={}\n",
                results.failures.total,
                results.failures.dns,
                results.failures.connections,
                results.failures.sets,
                results.failures.gets,
            )
            .as_bytes(),
        )
        .await?;

    output.flush().await
}
