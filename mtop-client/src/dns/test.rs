use crate::core::MtopError;
use crate::dns::client::{TcpConnection, UdpConnection};
use crate::dns::message::Message;
use crate::pool::ClientFactory;
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use std::collections::HashMap;
use std::io::{Cursor, Error};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Test implementation of `AsyncRead` and `AsyncWrite` that reads and writes
/// UDP format DNS bytes based on provided `Message` objects. The expected size
/// of each message is asserted when read or written.
#[derive(Debug)]
pub(crate) struct TestUdpSocket {
    messages: Vec<Message>,
}

impl TestUdpSocket {
    /// Create a new test socket that will emit the provided messages when read.
    #[allow(dead_code)]
    pub(crate) fn new(messages: Vec<Message>) -> Self {
        Self { messages }
    }
}

impl AsyncRead for TestUdpSocket {
    fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let msg = self.get_mut().messages.pop().unwrap();
        let size = msg.size();

        let mut bytes = Vec::new();
        msg.write_network_bytes(&mut bytes).unwrap();
        assert_eq!(bytes.len(), size);

        buf.put_slice(&bytes);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for TestUdpSocket {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Error>> {
        let mut cur = Cursor::new(buf);
        let start = cur.position();
        let msg = Message::read_network_bytes(&mut cur).unwrap();
        let read = (cur.position() - start) as usize;
        assert_eq!(read, msg.size());

        Poll::Ready(Ok(read))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}

/// Test implementation of `AsyncRead` and `AsyncWrite` that reads and writes
/// TCP format DNS bytes based on provided `Message` objects. The expected size
/// of each message is asserted when read or written.
#[derive(Debug)]
pub(crate) struct TestTcpSocket {
    messages: Vec<Message>,
}

impl TestTcpSocket {
    /// Create a new test socket that will emit the provided messages when read.
    #[allow(dead_code)]
    pub(crate) fn new(messages: Vec<Message>) -> Self {
        Self { messages }
    }
}

impl AsyncRead for TestTcpSocket {
    fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let msg = self.get_mut().messages.pop().unwrap();
        let size = msg.size();

        let mut bytes = Vec::new();
        bytes.write_u16::<NetworkEndian>(size as u16).unwrap();
        msg.write_network_bytes(&mut bytes).unwrap();
        assert_eq!(bytes.len(), size + 2);

        buf.put_slice(&bytes);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for TestTcpSocket {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Error>> {
        let mut cur = Cursor::new(buf);
        let size = usize::from(cur.read_u16::<NetworkEndian>().unwrap());
        let msg = Message::read_network_bytes(cur).unwrap();
        assert_eq!(size, msg.size());

        Poll::Ready(Ok(2 + size))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}

/// Test implementation of `ClientFactory` for creating `UdpConnection` instances that
/// read provided `Message` objects.
#[derive(Debug)]
pub(crate) struct TestUdpClientFactory {
    messages: HashMap<SocketAddr, Vec<Message>>,
}

impl TestUdpClientFactory {
    #[allow(dead_code)]
    pub(crate) fn new(messages: HashMap<SocketAddr, Vec<Message>>) -> Self {
        Self { messages }
    }
}

impl ClientFactory<SocketAddr, UdpConnection> for TestUdpClientFactory {
    async fn make(&self, key: &SocketAddr) -> Result<UdpConnection, MtopError> {
        let messages = self
            .messages
            .get(key)
            .ok_or_else(|| MtopError::runtime(format!("no messages configured for {}", key)))?;

        let sock = TestUdpSocket::new(messages.to_vec());
        let (read, write) = tokio::io::split(sock);
        Ok(UdpConnection::new(read, write))
    }
}

/// Test implementation of `ClientFactory` for creating `TcpConnection` instances that
/// read provided `Message` objects.
#[derive(Debug)]
pub(crate) struct TestTcpClientFactory {
    messages: HashMap<SocketAddr, Vec<Message>>,
}

impl TestTcpClientFactory {
    #[allow(dead_code)]
    pub(crate) fn new(messages: HashMap<SocketAddr, Vec<Message>>) -> Self {
        Self { messages }
    }
}

impl ClientFactory<SocketAddr, TcpConnection> for TestTcpClientFactory {
    async fn make(&self, key: &SocketAddr) -> Result<TcpConnection, MtopError> {
        let messages = self
            .messages
            .get(key)
            .ok_or_else(|| MtopError::runtime(format!("no messages configured for {}", key)))?;

        let sock = TestTcpSocket::new(messages.to_vec());
        let (read, write) = tokio::io::split(sock);
        Ok(TcpConnection::new(read, write))
    }
}
