use clap::{Args, Parser, Subcommand, ValueHint};
use mtop::bench::{Bencher, Percent, Summary};
use mtop::check::{Checker, TimingBundle};
use mtop_client::{
    DiscoveryDefault, MemcachedClient, MemcachedPool, Meta, MtopError, PoolConfig, SelectorRendezvous, Server,
    TLSConfig, Timeout, Value,
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
const DEFAULT_TIMEOUT_SECS: u64 = 30;
const DEFAULT_CONNECTIONS_PER_HOST: u64 = 4;

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

    /// Timeout for network operations, in seconds.
    #[arg(long, default_value_t = DEFAULT_TIMEOUT_SECS)]
    timeout_secs: u64,

    /// Maximum number of idle connections to maintain per host.
    #[arg(long, default_value_t = DEFAULT_CONNECTIONS_PER_HOST)]
    connections: u64,

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
    Bench(BenchCommand),
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

/// Run a benchmark against the cache.
///
/// One or more worker threads will be spawned to run gets and sets against the cache
/// in a loop, with `delay-millis` in between each iteration of the loop. The benchmark
/// will run for `time-secs` and print per-worker gets and sets per second as key-value
/// pairs.
///
/// The default configuration runs for 60 seconds, uses a single worker, performs 10,000
/// gets per second, performs 500 sets per second, and writes payloads between a few
/// hundred bytes and 128KB.
#[derive(Debug, Args)]
struct BenchCommand {
    /// How long to run the benchmark for in seconds.
    #[arg(long, default_value_t = 60)]
    time_secs: u64,

    /// How many writes to the cache as a percentage of reads from the cache, 0 to 1.
    ///
    /// A value of `1.0` means that for 100 gets, there will be 100 sets. A value of `0.5`
    /// means that for 100 gets, there will be 50 sets. Default is to perform many more gets
    /// than sets since cache workloads tend to have more reads than writes.
    #[arg(long, default_value_t = Percent::unchecked(0.05))]
    write_percent: Percent,

    /// How many workers to run at once, performing gets and sets against the cache.
    ///
    /// Each worker does 10,000 gets and 500 sets per second in the default configuration.
    #[arg(long, default_value_t = 1)]
    concurrency: usize,

    /// How long to wait between each batch of gets and sets performed against the cache.
    ///
    /// Each batch is 1000 gets and 50 sets by default. With a delay of 100ms this means
    /// there will be 10,000 gets and 500 sets per second. To increase the number of gets
    /// and sets performed by a worker, reduce this number. To decrease the number of gets
    /// and sets performed by a worker, increase this number.
    #[arg(long, default_value_t = 100)]
    delay_millis: u64,

    /// TTL to use for test values stored in the cache in seconds.
    #[arg(long, default_value_t = 1800)]
    ttl_secs: u32,
}

/// Run health checks against the cache.
///
/// Checks will be run repeatedly with `delay-millis` in between each iteration until
/// `time-secs` has elapsed. Time taken to do DNS resolution, connect, set a value in
/// the cache, and get a value from the cache will be recorded and emitted as key-value
/// pairs at the end of the test.
#[derive(Debug, Args)]
struct CheckCommand {
    /// How long to run the checks for in seconds.
    #[arg(long, default_value_t = 60)]
    time_secs: u64,

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

/// Increment the value of an item in the cache.
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

    let timeout = Duration::from_secs(opts.timeout_secs);
    let resolver = DiscoveryDefault;
    let servers = match resolver
        .resolve_by_proto(&opts.host)
        .timeout(timeout, "resolver.resolve_by_proto")
        .instrument(tracing::span!(Level::INFO, "resolver.resolve_by_proto"))
        .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(message = "unable to resolve host names", hosts = ?opts.host, err = %e);
            return ExitCode::FAILURE;
        }
    };

    let client = match new_client(&opts, &servers).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(message = "unable to initialize memcached client", host = opts.host, err = %e);
            return ExitCode::FAILURE;
        }
    };

    if let Err(e) = connect(&client, timeout).await {
        tracing::error!(message = "unable to connect", host = opts.host, err = %e);
        return ExitCode::FAILURE;
    };

    match &opts.mode {
        Action::Add(cmd) => run_add(&opts, cmd, &client).await,
        Action::Bench(cmd) => run_bench(&opts, cmd, client).await,
        Action::Check(cmd) => run_check(&opts, cmd, &client, &resolver).await,
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

async fn new_client(opts: &McConfig, servers: &[Server]) -> Result<MemcachedClient, MtopError> {
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
            max_idle_per_host: opts.connections,
            ..Default::default()
        },
    )
    .await
    .map(|pool| {
        let selector = SelectorRendezvous::new(servers.to_vec());
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

    if let Err(e) = client
        .add(&cmd.key, 0, cmd.ttl, &buf)
        .timeout(Duration::from_secs(opts.timeout_secs), "client.add")
        .instrument(tracing::span!(Level::INFO, "client.add"))
        .await
    {
        tracing::error!(message = "unable to add item", key = cmd.key, host = opts.host, err = %e);
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_bench(opts: &McConfig, cmd: &BenchCommand, client: MemcachedClient) -> ExitCode {
    let bencher = Bencher::new(
        client,
        Handle::current(),
        Duration::from_millis(cmd.delay_millis),
        Duration::from_secs(opts.timeout_secs),
        Duration::from_secs(cmd.ttl_secs as u64),
        cmd.write_percent,
        cmd.concurrency,
    );

    let measurements = bencher.run(Duration::from_secs(cmd.time_secs)).await;
    print_bench_results(&measurements);

    ExitCode::SUCCESS
}

async fn run_check(
    opts: &McConfig,
    cmd: &CheckCommand,
    client: &MemcachedClient,
    resolver: &DiscoveryDefault,
) -> ExitCode {
    let checker = Checker::new(
        client,
        resolver,
        Duration::from_millis(cmd.delay_millis),
        Duration::from_secs(opts.timeout_secs),
    );
    let results = checker.run(&opts.host, Duration::from_secs(cmd.time_secs)).await;
    print_check_results(&results);

    if results.failures.total > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_decr(opts: &McConfig, cmd: &DecrCommand, client: &MemcachedClient) -> ExitCode {
    if let Err(e) = client
        .decr(&cmd.key, cmd.delta)
        .timeout(Duration::from_secs(opts.timeout_secs), "client.decr")
        .instrument(tracing::span!(Level::INFO, "client.decr"))
        .await
    {
        tracing::error!(message = "unable to decrement value", key = cmd.key, host = opts.host, err = %e);
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_delete(opts: &McConfig, cmd: &DeleteCommand, client: &MemcachedClient) -> ExitCode {
    if let Err(e) = client
        .delete(&cmd.key)
        .timeout(Duration::from_secs(opts.timeout_secs), "client.delete")
        .instrument(tracing::span!(Level::INFO, "client.delete"))
        .await
    {
        tracing::error!(message = "unable to delete item", key = cmd.key, host = opts.host, err = %e);
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_get(opts: &McConfig, cmd: &GetCommand, client: &MemcachedClient) -> ExitCode {
    let response = match client
        .get(&[cmd.key.clone()])
        .timeout(Duration::from_secs(opts.timeout_secs), "client.get")
        .instrument(tracing::span!(Level::INFO, "client.get"))
        .await
    {
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

    for (id, e) in response.errors.iter() {
        tracing::error!(message = "error fetching value", server = %id, err = %e);
    }

    if response.has_errors() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_incr(opts: &McConfig, cmd: &IncrCommand, client: &MemcachedClient) -> ExitCode {
    if let Err(e) = client
        .incr(&cmd.key, cmd.delta)
        .timeout(Duration::from_secs(opts.timeout_secs), "client.incr")
        .instrument(tracing::span!(Level::INFO, "client.incr"))
        .await
    {
        tracing::error!(message = "unable to increment value", key = cmd.key, host = opts.host, err = %e);
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_keys(opts: &McConfig, cmd: &KeysCommand, client: &MemcachedClient) -> ExitCode {
    let response = match client
        .metas()
        .timeout(Duration::from_secs(opts.timeout_secs), "client.metas")
        .instrument(tracing::span!(Level::INFO, "client.metas"))
        .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(message = "unable to list keys", host = opts.host, err = %e);
            return ExitCode::FAILURE;
        }
    };

    let has_errors = response.has_errors();
    let mut metas: Vec<Meta> = response.values.into_values().flatten().collect();
    metas.sort();

    if let Err(e) = print_keys(&metas, cmd.details).await {
        tracing::warn!(message = "error writing output", err = %e);
    }

    for (id, e) in response.errors.iter() {
        tracing::error!(message = "error fetching metas", server = %id, err = %e);
    }

    if has_errors {
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

    if let Err(e) = client
        .replace(&cmd.key, 0, cmd.ttl, &buf)
        .timeout(Duration::from_secs(opts.timeout_secs), "client.replace")
        .instrument(tracing::span!(Level::INFO, "client.replace"))
        .await
    {
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

    if let Err(e) = client
        .set(&cmd.key, 0, cmd.ttl, &buf)
        .timeout(Duration::from_secs(opts.timeout_secs), "client.set")
        .instrument(tracing::span!(Level::INFO, "client.set"))
        .await
    {
        tracing::error!(message = "unable to set item", key = cmd.key, host = opts.host, err = %e);
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn run_touch(opts: &McConfig, cmd: &TouchCommand, client: &MemcachedClient) -> ExitCode {
    if let Err(e) = client
        .touch(&cmd.key, cmd.ttl)
        .timeout(Duration::from_secs(opts.timeout_secs), "client.touch")
        .instrument(tracing::span!(Level::INFO, "client.touch"))
        .await
    {
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
    // Write to stdout via buffered output since we need to write raw bytes.
    let mut output = BufWriter::new(tokio::io::stdout());
    output.write_all(&val.data).await?;
    output.flush().await
}

async fn print_keys(metas: &[Meta], show_details: bool) -> io::Result<()> {
    // Write to stdout via buffered output since keys results are usually
    // quite big and we don't want to overhead of locking and flushing on
    // every println!().
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

fn print_bench_results(results: &[Summary]) {
    for m in results {
        println!(
            "worker={} gets={} gets_time={:.3} gets_per_second={:.0} sets={} sets_time={:.3} sets_per_second={:.0}",
            m.worker,
            m.gets,
            m.gets_time.as_secs_f64(),
            m.gets_per_sec(),
            m.sets,
            m.sets_time.as_secs_f64(),
            m.sets_per_sec(),
        );
    }
}

fn print_check_results(results: &TimingBundle) {
    println!(
        "type=min total={:.9} dns={:.9} connection={:.9} set={:.9} get={:.9}",
        results.total.min.as_secs_f64(),
        results.dns.min.as_secs_f64(),
        results.connections.min.as_secs_f64(),
        results.sets.min.as_secs_f64(),
        results.gets.min.as_secs_f64()
    );

    println!(
        "type=max total={:.9} dns={:.9} connection={:.9} set={:.9} get={:.9}",
        results.total.max.as_secs_f64(),
        results.dns.max.as_secs_f64(),
        results.connections.max.as_secs_f64(),
        results.sets.max.as_secs_f64(),
        results.gets.max.as_secs_f64(),
    );

    println!(
        "type=avg total={:.9} dns={:.9} connection={:.9} set={:.9} get={:.9}",
        results.total.avg.as_secs_f64(),
        results.dns.avg.as_secs_f64(),
        results.connections.avg.as_secs_f64(),
        results.sets.avg.as_secs_f64(),
        results.gets.avg.as_secs_f64()
    );

    println!(
        "type=stddev total={:.9} dns={:.9} connection={:.9} set={:.9} get={:.9}",
        results.total.std_dev.as_secs_f64(),
        results.dns.std_dev.as_secs_f64(),
        results.connections.std_dev.as_secs_f64(),
        results.sets.std_dev.as_secs_f64(),
        results.gets.std_dev.as_secs_f64()
    );

    println!(
        "type=failures total={} dns={} connection={} set={} get={}",
        results.failures.total,
        results.failures.dns,
        results.failures.connections,
        results.failures.sets,
        results.failures.gets,
    );
}
