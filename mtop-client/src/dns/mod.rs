mod client;
mod core;
mod message;
mod name;
mod rdata;
mod resolv;
mod test;

pub use crate::dns::client::{
    DefaultDnsClient, DnsClient, DnsClientConfig, TcpConnection, TcpConnectionFactory, UdpConnection,
    UdpConnectionFactory,
};
pub use crate::dns::core::{RecordClass, RecordType};
pub use crate::dns::message::{Flags, Message, MessageId, Operation, Question, Record, ResponseCode};
pub use crate::dns::name::Name;
pub use crate::dns::rdata::{
    RecordData, RecordDataA, RecordDataAAAA, RecordDataCNAME, RecordDataNS, RecordDataOpt, RecordDataSOA,
    RecordDataSRV, RecordDataTXT, RecordDataUnknown,
};
pub use crate::dns::resolv::{ResolvConf, ResolvConfOptions, config};
