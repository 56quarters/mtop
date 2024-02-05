use mtop_client::MtopError;
use pprof::protos::Message;
use pprof::{ProfilerGuard, ProfilerGuardBuilder};
use std::ffi::c_int;
use std::fmt;

const PROFILE_FREQUENCY_HZ: c_int = 999;
const PROFILE_BLOCK_LIST: &[&str] = &["libc", "libgcc", "pthread", "vdso"];

pub struct Profiler {
    guard: ProfilerGuard<'static>,
}

impl Profiler {
    pub fn new() -> Self {
        let guard = ProfilerGuardBuilder::default()
            .frequency(PROFILE_FREQUENCY_HZ)
            .blocklist(PROFILE_BLOCK_LIST)
            .build()
            .unwrap();

        Self { guard }
    }

    pub fn proto(&self) -> Result<Vec<u8>, MtopError> {
        self.guard
            .report()
            .build()
            .and_then(|report| report.pprof())
            .map_err(|e| MtopError::runtime_cause("cannot build profile report", e))
            .and_then(|profile| {
                profile
                    .write_to_bytes()
                    .map_err(|e| MtopError::runtime_cause("cannot encode as protobuf", e))
            })
    }
}

impl Default for Profiler {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Profiler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Profiler {{ guard: <...> }}")
    }
}
