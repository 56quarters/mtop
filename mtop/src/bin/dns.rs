use clap::{Args, Parser, Subcommand, ValueHint};
use mtop_client::dns::{Flags, Message, MessageId, Name, Question, Record, RecordClass, RecordType};
use std::fmt::Write;
use std::io::Cursor;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{Instrument, Level};

const DEFAULT_LOG_LEVEL: Level = Level::INFO;
const DEFAULT_RECORD_TYPE: RecordType = RecordType::A;
const DEFAULT_RECORD_CLASS: RecordClass = RecordClass::INET;

/// dns: Make DNS queries or read/write binary format DNS messages
#[derive(Debug, Parser)]
#[command(name = "dns", version = clap::crate_version!())]
struct DnsConfig {
    /// Logging verbosity. Allowed values are 'trace', 'debug', 'info', 'warn', and 'error'
    /// (case-insensitive).
    #[arg(long, default_value_t = DEFAULT_LOG_LEVEL)]
    log_level: Level,

    #[command(subcommand)]
    mode: Action,
}

#[derive(Debug, Subcommand)]
enum Action {
    Query(QueryCommand),
    Read(ReadCommand),
    Write(WriteCommand),
}

/// Perform a DNS query and display the result as dig-like text output.
#[derive(Debug, Args)]
struct QueryCommand {
    /// Path to resolv.conf file for loading DNS configuration information. If this file
    /// can't be loaded, default values for DNS configuration are used instead.
    #[arg(long, default_value = default_resolv_conf().into_os_string(), value_hint = ValueHint::FilePath)]
    resolv_conf: PathBuf,

    /// Output query results in raw binary format instead of human-readable
    /// text. NOTE, this may break your terminal and so should probably be piped
    /// to a file.
    #[arg(long, default_value_t = false)]
    raw: bool,

    /// Type of record to request. Supported: A, AAAA, CNAME, NS, SOA, SRV, TXT.
    #[arg(long, default_value_t = DEFAULT_RECORD_TYPE)]
    rtype: RecordType,

    /// Class of record to request. Supported: INET, CHAOS, HESIOD, NONE, ANY.
    #[arg(long, default_value_t = DEFAULT_RECORD_CLASS)]
    rclass: RecordClass,

    /// Domain name to lookup.
    #[arg(required = true)]
    name: String,
}

fn default_resolv_conf() -> PathBuf {
    PathBuf::from("/etc/resolv.conf")
}

/// Read a binary format DNS message from standard input and display it as dig-like text output.
#[derive(Debug, Args)]
struct ReadCommand {}

/// Write a binary format DNS message to standard output.
#[derive(Debug, Args)]
struct WriteCommand {
    /// Type of record to request. Supported: A, AAAA, CNAME, NS, SOA, SRV, TXT.
    #[arg(long, default_value_t = DEFAULT_RECORD_TYPE)]
    rtype: RecordType,

    /// Class of record to request. Supported: INET, CHAOS, HESIOD, NONE, ANY.
    #[arg(long, default_value_t = DEFAULT_RECORD_CLASS)]
    rclass: RecordClass,

    /// Domain name to lookup.
    #[arg(required = true)]
    name: String,
}

#[tokio::main]
async fn main() -> ExitCode {
    let opts = DnsConfig::parse();

    let console_subscriber =
        mtop::tracing::console_subscriber(opts.log_level).expect("failed to setup console logging");
    tracing::subscriber::set_global_default(console_subscriber).expect("failed to initialize console logging");

    match &opts.mode {
        Action::Query(cmd) => run_query(cmd).await,
        Action::Read(cmd) => run_read(cmd).await,
        Action::Write(cmd) => run_write(cmd).await,
    }
}

async fn run_query(cmd: &QueryCommand) -> ExitCode {
    let client = mtop::dns::new_client(&cmd.resolv_conf)
        .instrument(tracing::span!(Level::INFO, "dns.new_client"))
        .await;
    let name = match Name::from_str(&cmd.name) {
        Ok(n) => n,
        Err(e) => {
            tracing::error!(message = "invalid name supplied", name = cmd.name, err = %e);
            return ExitCode::FAILURE;
        }
    };

    let response = match client
        .resolve(name, cmd.rtype, cmd.rclass)
        .instrument(tracing::span!(Level::INFO, "client.resolve"))
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(message = "unable to perform DNS query", err = %e);
            return ExitCode::FAILURE;
        }
    };

    if cmd.raw {
        write_binary_message(&response).await
    } else {
        write_text_message(&response).await
    }
}

async fn run_read(_: &ReadCommand) -> ExitCode {
    let mut buf = Vec::new();
    let mut input = tokio::io::stdin();

    let n = match input.read_to_end(&mut buf).await {
        Ok(n) => n,
        Err(e) => {
            tracing::error!(message = "unable to read message from stdin", err = %e);
            return ExitCode::FAILURE;
        }
    };

    let cur = Cursor::new(&buf[0..n]);
    let msg = match Message::read_network_bytes(cur) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(message = "malformed message", err = %e);
            return ExitCode::FAILURE;
        }
    };

    write_text_message(&msg).await;
    ExitCode::SUCCESS
}

async fn run_write(cmd: &WriteCommand) -> ExitCode {
    let name = match Name::from_str(&cmd.name) {
        Ok(n) => n,
        Err(e) => {
            tracing::error!(message = "invalid name supplied", name = cmd.name, err = %e);
            return ExitCode::FAILURE;
        }
    };

    let id = MessageId::random();
    let msg = Message::new(id, Flags::default().set_query().set_recursion_desired())
        .add_question(Question::new(name, cmd.rtype).set_qclass(cmd.rclass));

    write_binary_message(&msg).await
}

async fn write_binary_message(msg: &Message) -> ExitCode {
    let mut buf = Vec::new();
    if let Err(e) = msg.write_network_bytes(&mut buf) {
        tracing::error!(message = "unable to encode message to wire format", err = %e);
        return ExitCode::FAILURE;
    }

    let mut out = tokio::io::stdout();
    if let Err(e) = out.write_all(&buf).await {
        tracing::error!(message = "unable to write wire encoded message to stdout", err = %e);
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

async fn write_text_message(msg: &Message) -> ExitCode {
    let mut buf = String::new();
    format_header(&mut buf, msg);
    format_question(&mut buf, msg.questions());
    format_answer(&mut buf, msg.answers());
    format_authority(&mut buf, msg.authority());
    format_extra(&mut buf, msg.extra());

    let mut out = tokio::io::stdout();
    if let Err(e) = out.write_all(buf.as_bytes()).await {
        tracing::error!(message = "unable to write human readable message to stdout", err = %e);
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

fn format_header(buf: &mut String, msg: &Message) {
    let _ = writeln!(
        buf,
        ";; >>HEADER<< opcode: {:?}, status: {:?}, id: {}",
        msg.flags().get_op_code(),
        msg.flags().get_response_code(),
        msg.id()
    );
    let _ = writeln!(buf, ";; flags: {:?}", msg.flags());
}

fn format_question(buf: &mut String, questions: &[Question]) {
    let _ = writeln!(buf, ";; QUESTION SECTION:");
    for q in questions {
        let _ = writeln!(buf, "; {}\t\t\t{}\t{}", q.name(), q.qclass(), q.qtype());
    }
}

fn format_authority(buf: &mut String, records: &[Record]) {
    let _ = writeln!(buf, ";; AUTHORITY SECTION:");
    format_records(buf, records);
}

fn format_answer(buf: &mut String, records: &[Record]) {
    let _ = writeln!(buf, ";; ANSWER SECTION:");
    format_records(buf, records);
}

fn format_extra(buf: &mut String, records: &[Record]) {
    let _ = writeln!(buf, ";; ADDITIONAL SECTION:");
    format_records(buf, records);
}

fn format_records(buf: &mut String, records: &[Record]) {
    for r in records {
        let _ = writeln!(
            buf,
            "{}\t\t{}\t{}\t{}\t{}",
            r.name(),
            r.ttl(),
            r.rclass(),
            r.rtype(),
            r.rdata()
        );
    }
}
