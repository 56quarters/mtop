#[cfg(not(feature = "profile"))]
pub use nop::Profiler;
#[cfg(feature = "profile")]
pub use pprof::Profiler;

#[cfg(not(feature = "profile"))]
mod nop;
#[cfg(feature = "profile")]
mod pprof;
