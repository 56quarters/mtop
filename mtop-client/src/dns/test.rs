use crate::core::MtopError;
use crate::dns::client::{TcpConnection, UdpConnection};
use crate::dns::message::Message;
use crate::pool::ClientFactory;
use async_trait::async_trait;
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use std::collections::HashMap;
use std::io::{Cursor, Error};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::Mutex;

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
        let end = cur.position();

        assert!(end > start);
        let read = usize::try_from(end - start).unwrap();
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
        bytes.write_u16::<NetworkEndian>(u16::try_from(size).unwrap()).unwrap();
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
/// read multiple provided `Message` objects.
#[derive(Debug)]
pub(crate) struct TestPooledUdpClientFactory {
    messages: Mutex<HashMap<SocketAddr, Vec<Message>>>,
}

impl TestPooledUdpClientFactory {
    #[allow(dead_code)]
    pub(crate) fn new(messages: HashMap<SocketAddr, Vec<Message>>) -> Self {
        Self {
            messages: Mutex::new(messages),
        }
    }
}

#[async_trait]
impl ClientFactory<SocketAddr, UdpConnection> for TestPooledUdpClientFactory {
    async fn make(&self, key: &SocketAddr) -> Result<UdpConnection, MtopError> {
        let mut messages = self.messages.lock().await;
        let sock_messages = messages.remove(key).unwrap();

        let sock = TestUdpSocket::new(sock_messages);
        let (read, write) = tokio::io::split(sock);
        Ok(UdpConnection::new(read, write))
    }
}

/// Test implementation of `ClientFactory` for creating `UdpConnection` instances that
/// read a single provided `Message` object at time.
#[derive(Debug)]
pub(crate) struct TestUnpooledUdpClientFactory {
    messages: Mutex<HashMap<SocketAddr, Vec<Message>>>,
}

impl TestUnpooledUdpClientFactory {
    #[allow(dead_code)]
    pub(crate) fn new(messages: HashMap<SocketAddr, Vec<Message>>) -> Self {
        Self {
            messages: Mutex::new(messages),
        }
    }
}

#[async_trait]
impl ClientFactory<SocketAddr, UdpConnection> for TestUnpooledUdpClientFactory {
    async fn make(&self, key: &SocketAddr) -> Result<UdpConnection, MtopError> {
        let mut messages = self.messages.lock().await;
        let sock_message = messages
            .get_mut(key)
            .ok_or_else(|| MtopError::runtime(format!("no messages configured for {}", key)))?
            .pop()
            .unwrap();

        let sock = TestUdpSocket::new(vec![sock_message]);
        let (read, write) = tokio::io::split(sock);
        Ok(UdpConnection::new(read, write))
    }
}

/// Test implementation of `ClientFactory` for creating `TcpConnection` instances that
/// read provided `Message` objects.
#[derive(Debug)]
pub(crate) struct TestPooledTcpClientFactory {
    messages: Mutex<HashMap<SocketAddr, Vec<Message>>>,
}

impl TestPooledTcpClientFactory {
    #[allow(dead_code)]
    pub(crate) fn new(messages: HashMap<SocketAddr, Vec<Message>>) -> Self {
        Self {
            messages: Mutex::new(messages),
        }
    }
}

#[async_trait]
impl ClientFactory<SocketAddr, TcpConnection> for TestPooledTcpClientFactory {
    async fn make(&self, key: &SocketAddr) -> Result<TcpConnection, MtopError> {
        let mut messages = self.messages.lock().await;
        let sock_messages = messages.remove(key).unwrap();

        let sock = TestTcpSocket::new(sock_messages);
        let (read, write) = tokio::io::split(sock);
        Ok(TcpConnection::new(read, write))
    }
}

/// Test implementation of `ClientFactory` for creating `TcpConnection` instances that
/// read a single provided `Message` object at a time.
#[derive(Debug)]
pub(crate) struct TestUnpooledTcpClientFactory {
    messages: Mutex<HashMap<SocketAddr, Vec<Message>>>,
}

impl TestUnpooledTcpClientFactory {
    #[allow(dead_code)]
    pub(crate) fn new(messages: HashMap<SocketAddr, Vec<Message>>) -> Self {
        Self {
            messages: Mutex::new(messages),
        }
    }
}

#[async_trait]
impl ClientFactory<SocketAddr, TcpConnection> for TestUnpooledTcpClientFactory {
    async fn make(&self, key: &SocketAddr) -> Result<TcpConnection, MtopError> {
        let mut messages = self.messages.lock().await;
        let sock_message = messages
            .get_mut(key)
            .ok_or_else(|| MtopError::runtime(format!("no messages configured for {}", key)))?
            .pop()
            .unwrap();

        let sock = TestTcpSocket::new(vec![sock_message]);
        let (read, write) = tokio::io::split(sock);
        Ok(TcpConnection::new(read, write))
    }
}
