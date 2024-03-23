use crate::core::MtopError;
use crate::dns::core::RecordType;
use crate::dns::message::{Flags, Message, MessageId, Question};
use crate::dns::name::Name;
use std::io::Cursor;
use std::net::SocketAddr;
use std::str::FromStr;
use tokio::net::UdpSocket;

const DEFAULT_RECV_BUF: usize = 512;

#[derive(Debug, Clone)]
pub struct DnsClient {
    local: SocketAddr,
    server: SocketAddr,
}

impl DnsClient {
    pub fn new(local: SocketAddr, server: SocketAddr) -> Self {
        Self { local, server }
    }

    pub async fn exchange(&self, msg: &Message) -> Result<Message, MtopError> {
        let id = msg.id();
        let sock = self.connect_udp().await?;
        self.send_udp(&sock, msg).await?;
        self.recv_udp(&sock, id).await
    }

    pub async fn resolve_srv(&self, name: &str) -> Result<Message, MtopError> {
        let n = Name::from_str(name)?;
        let id = MessageId::random();
        let flags = Flags::default().set_recursion_desired();
        let msg = Message::new(id, flags).add_question(Question::new(n, RecordType::SRV));

        self.exchange(&msg).await
    }

    async fn connect_udp(&self) -> Result<UdpSocket, MtopError> {
        let sock = UdpSocket::bind(&self.local).await?;
        sock.connect(&self.server).await?;
        Ok(sock)
    }

    async fn send_udp(&self, socket: &UdpSocket, msg: &Message) -> Result<(), MtopError> {
        let mut buf = Vec::new();
        msg.write_network_bytes(&mut buf)?;
        Ok(socket.send(&buf).await.map(|_| ())?)
    }

    async fn recv_udp(&self, socket: &UdpSocket, id: MessageId) -> Result<Message, MtopError> {
        let mut buf = vec![0_u8; DEFAULT_RECV_BUF];
        loop {
            let n = socket.recv(&mut buf).await?;
            let cur = Cursor::new(&buf[0..n]);
            let msg = Message::read_network_bytes(cur)?;
            if msg.id() == id {
                return Ok(msg);
            }
        }
    }
}
