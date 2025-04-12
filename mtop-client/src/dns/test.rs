use crate::dns::message::Message;
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Error};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Test implementation of `AsyncRead` and `AsyncWrite` that reads and writes
/// TCP format DNS bytes based on provided message objects. The expected size
/// of each message is asserted when read or written.
pub(crate) struct TcpTestSocket {
    messages: Vec<Message>,
    written: Arc<Mutex<Vec<Message>>>,
}

impl TcpTestSocket {
    /// Create a new test socket that will emit the provided messages when read
    /// and return a handle to any messages written to this test socket.
    #[allow(dead_code)]
    pub(crate) fn new(messages: Vec<Message>) -> (Self, Arc<Mutex<Vec<Message>>>) {
        let written = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                messages,
                written: written.clone(),
            },
            written.clone(),
        )
    }
}

impl AsyncRead for TcpTestSocket {
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

impl AsyncWrite for TcpTestSocket {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Error>> {
        let mut cur = Cursor::new(buf);
        let size = usize::from(cur.read_u16::<NetworkEndian>().unwrap());
        let msg = Message::read_network_bytes(cur).unwrap();
        assert_eq!(size, msg.size());

        let mut written = self.written.lock().unwrap();
        written.push(msg);
        Poll::Ready(Ok(2 + size))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}

/// Test implementation of `AsyncRead` and `AsyncWrite` that reads and writes
/// UDP format DNS bytes based on provided message objects. The expected size
/// of each message is asserted when read or written.
pub(crate) struct UdpTestSocket {
    messages: Vec<Message>,
    written: Arc<Mutex<Vec<Message>>>,
}

impl UdpTestSocket {
    /// Create a new test socket that will emit the provided messages when read
    /// and return a handle to any messages written to this test socket.
    #[allow(dead_code)]
    pub(crate) fn new(messages: Vec<Message>) -> (Self, Arc<Mutex<Vec<Message>>>) {
        let written = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                messages,
                written: written.clone(),
            },
            written.clone(),
        )
    }
}

impl AsyncRead for UdpTestSocket {
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

impl AsyncWrite for UdpTestSocket {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Error>> {
        let mut cur = Cursor::new(buf);
        let start = cur.position();
        let msg = Message::read_network_bytes(&mut cur).unwrap();
        let read = (cur.position() - start) as usize;
        assert_eq!(read, msg.size());

        let mut written = self.written.lock().unwrap();
        written.push(msg);
        Poll::Ready(Ok(read))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}
