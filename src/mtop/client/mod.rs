mod core;
mod pool;

pub use crate::client::core::{
    ErrorKind, Memcached, Meta, MtopError, ProtocolError, ProtocolErrorKind, Slab, SlabItem, SlabItems, Slabs, Stats,
    Value,
};
pub use crate::client::pool::{MemcachedPool, PooledMemcached, TLSConfig};
