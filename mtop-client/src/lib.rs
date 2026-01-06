#![allow(clippy::uninlined_format_args)]
#![warn(missing_debug_implementations, unreachable_pub)]
#![deny(unused_must_use)]

mod client;
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
pub use crate::core::{
    ErrorKind, Key, Memcached, Meta, MtopError, ProtocolError, ProtocolErrorKind, Slab, SlabItem, SlabItems, Slabs,
    Stats, Value,
};
pub use crate::discovery::{Discovery, Server, ServerID};
pub use crate::net::TlsConfig;
pub use crate::pool::{ClientFactory, PooledClient};
pub use crate::timeout::{Timed, Timeout};
