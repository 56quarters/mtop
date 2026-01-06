mod tcp;
mod tls;
#[cfg(unix)]
mod unix;

pub(crate) use crate::net::tcp::{tcp_connect, tcp_tls_connect};
pub use crate::net::tls::TlsConfig;
pub(crate) use crate::net::tls::tls_client_config;
#[cfg(unix)]
pub(crate) use crate::net::unix::unix_connect;
