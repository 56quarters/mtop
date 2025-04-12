mod client;
mod core;
mod discovery;
mod net;
mod pool;
mod timeout;

pub mod dns;
#[doc(hidden)]
pub mod test;

pub use crate::client::{
    MemcachedClient, MemcachedClientConfig, RendezvousSelector, Selector, ServersResponse, TcpClientFactory,
    ValuesResponse,
};
pub use crate::core::{
    ErrorKind, Key, Memcached, Meta, MtopError, ProtocolError, ProtocolErrorKind, Slab, SlabItem, SlabItems, Slabs,
    Stats, Value,
};
pub use crate::discovery::{Discovery, Server, ServerID};
pub use crate::net::TlsConfig;
pub use crate::pool::{ClientFactory, PooledClient};
pub use crate::timeout::{Timed, Timeout};
