mod client;
mod core;
mod message;
mod name;
mod rdata;
mod resolv;

pub use crate::dns::client::DnsClient;
pub use crate::dns::core::{RecordClass, RecordType};
pub use crate::dns::message::{Flags, Message, MessageId, Operation, Question, Record, ResponseCode};
pub use crate::dns::name::Name;
pub use crate::dns::rdata::{
    RecordData, RecordDataA, RecordDataAAAA, RecordDataCNAME, RecordDataNS, RecordDataOpt, RecordDataSOA,
    RecordDataSRV, RecordDataTXT, RecordDataUnknown,
};
pub use crate::dns::resolv::{config, ResolvConf, ResolvConfOptions};
