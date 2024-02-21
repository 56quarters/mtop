mod core;
mod message;
mod name;
mod rdata;

pub use crate::dns::core::{RecordClass, RecordType};
pub use crate::dns::message::{Flags, Message, Operation, Question, Record, ResponseCode};
pub use crate::dns::name::Name;
pub use crate::dns::rdata::{
    RecordData, RecordDataA, RecordDataAAAA, RecordDataCNAME, RecordDataNS, RecordDataSOA, RecordDataSRV,
    RecordDataTXT, RecordDataUnknown,
};

use crate::core::MtopError;
use std::io::Cursor;
use tokio::net::UdpSocket;

const DEFAULT_RECV_BUF: usize = 512;

pub fn id() -> u16 {
    rand::random()
}

pub async fn send(sock: &UdpSocket, msg: &Message) -> Result<(), MtopError> {
    let mut buf = Vec::new();
    msg.write_network_bytes(&mut buf)?;
    Ok(sock.send(&buf).await.map(|_| ())?)
}

pub async fn recv(sock: &UdpSocket, id: u16) -> Result<Message, MtopError> {
    let mut buf = vec![0_u8; DEFAULT_RECV_BUF];
    loop {
        let n = sock.recv(&mut buf).await?;
        let cur = Cursor::new(&buf[0..n]);
        let msg = Message::read_network_bytes(cur)?;
        if msg.id() == id {
            return Ok(msg);
        }
    }
}
