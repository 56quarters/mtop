mod client;
mod core;
mod discovery;
pub mod dns;
mod net;
mod pool;
mod timeout;

pub use crate::client::{
    MemcachedClient, MemcachedPool, MemcachedPoolConfig, SelectorRendezvous, ServersResponse, ValuesResponse,
};
pub use crate::core::{
    ErrorKind, Key, Memcached, Meta, MtopError, ProtocolError, ProtocolErrorKind, Slab, SlabItem, SlabItems, Slabs,
    Stats, Value,
};
pub use crate::discovery::{DiscoveryDefault, Server, ServerID};
pub use crate::net::TlsConfig;
pub use crate::pool::PooledClient;
pub use crate::timeout::{Timed, Timeout};
