mod client;
mod core;
mod discovery;
pub mod dns;
mod pool;
mod timeout;

pub use crate::client::{MemcachedClient, SelectorRendezvous, ServersResponse, ValuesResponse};
pub use crate::core::{
    ErrorKind, Key, Memcached, Meta, MtopError, ProtocolError, ProtocolErrorKind, Slab, SlabItem, SlabItems, Slabs,
    Stats, Value,
};
pub use crate::discovery::{DiscoveryDefault, Server, ServerID};
pub use crate::pool::{MemcachedPool, PoolConfig, PooledMemcached, TLSConfig};
pub use crate::timeout::{Timed, Timeout};
