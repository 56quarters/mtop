use crate::profile::Profiler;
use mtop_client::MtopError;
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Default)]
pub struct Writer {
    profiler: Profiler,
}

impl Writer {
    pub fn finish<P>(&self, path: P)
    where
        P: AsRef<Path>,
    {
        if let Err(e) = self.profiler.proto().and_then(|b| Self::write_file(&path, &b)) {
            tracing::warn!(message = "unable to collect and write pprof data", path = ?path.as_ref(), err = %e);
        }
    }

    fn write_file<P>(path: P, contents: &[u8]) -> Result<(), MtopError>
    where
        P: AsRef<Path>,
    {
        if contents.is_empty() {
            return Err(MtopError::configuration("mtop built without profiling support"));
        }

        let mut file = File::options().create(true).truncate(true).write(true).open(path)?;
        file.write_all(contents)?;
        file.flush()?;
        Ok(file.sync_all()?)
    }
}
