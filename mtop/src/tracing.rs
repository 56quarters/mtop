use std::fs;
use std::fs::File;
use std::io::{self, Stderr};
use std::path::PathBuf;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::format::{DefaultFields, Format};
use tracing_subscriber::FmtSubscriber;

#[allow(clippy::type_complexity)]
pub fn console_subscriber(
    level: tracing::Level,
) -> Result<FmtSubscriber<DefaultFields, Format, LevelFilter, fn() -> Stderr>, io::Error> {
    Ok(FmtSubscriber::builder()
        .with_max_level(level)
        .with_writer(io::stderr as fn() -> Stderr)
        .finish())
}

#[allow(clippy::type_complexity)]
pub fn file_subscriber(
    level: tracing::Level,
    path: &PathBuf,
) -> Result<FmtSubscriber<DefaultFields, Format, LevelFilter, File>, io::Error> {
    if let Some(d) = path.parent() {
        fs::create_dir_all(d)?;
    }

    let file = File::options().create(true).write(true).truncate(true).open(path)?;

    Ok(FmtSubscriber::builder()
        .with_max_level(level)
        .with_writer(file)
        .with_ansi(false)
        .finish())
}
