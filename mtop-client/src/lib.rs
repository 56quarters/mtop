#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::single_match_else,
    clippy::uninlined_format_args
)]
#![warn(missing_debug_implementations, unreachable_pub)]
#![deny(unused_must_use)]

mod client;
mod codec;
mod core;
mod discovery;
pub mod dns;
mod net;
mod pool;
mod timeout;

#[cfg(unix)]
pub use crate::client::UnixClientFactory;
pub use crate::client::{
    MemcachedClient, MemcachedClientConfig, RendezvousSelector, Selector, ServersResponse, TcpClientFactory,
    TlsTcpClientFactory, ValuesResponse,
};
pub use crate::codec::url_decode;
pub use crate::core::{
    ErrorKind, Key, Memcached, Meta, MtopError, ProtocolError, ProtocolErrorKind, Slab, SlabItem, SlabItems, Slabs,
    Stats, Value,
};
pub use crate::discovery::{Discovery, Server, ServerID};
pub use crate::net::TlsConfig;
pub use crate::pool::{ClientFactory, PooledClient};
pub use crate::timeout::{Timed, Timeout};
