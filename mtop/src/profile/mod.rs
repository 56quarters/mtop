#[cfg(not(feature = "profile"))]
mod nop;
#[cfg(not(feature = "profile"))]
pub use nop::Profiler;

#[cfg(feature = "profile")]
mod proto;
#[cfg(feature = "profile")]
pub use proto::Profiler;

mod writer;
pub use writer::Writer;
