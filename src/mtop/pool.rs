use crate::client::{Memcached, MtopError};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

// TODO: Just move this to the client module? Make it a directory?

pub struct Client {
    inner: Memcached<OwnedReadHalf, OwnedWriteHalf>,
    host: String,
}

impl Deref for Client {
    type Target = Memcached<OwnedReadHalf, OwnedWriteHalf>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Client {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[derive(Default)]
pub struct ClientPool {
    clients: Mutex<HashMap<String, Memcached<OwnedReadHalf, OwnedWriteHalf>>>,
}

impl ClientPool {
    pub fn new() -> Self {
        ClientPool {
            clients: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get(&self, host: &str) -> Result<Client, MtopError> {
        let mut map = self.clients.lock().await;
        let inner = match map.remove(host) {
            Some(c) => c,
            None => connect(host).await?,
        };

        Ok(Client {
            inner,
            host: host.to_owned(),
        })
    }

    pub async fn put(&self, client: Client) {
        let mut map = self.clients.lock().await;
        map.insert(client.host, client.inner);
    }
}

async fn connect(host: &str) -> Result<Memcached<OwnedReadHalf, OwnedWriteHalf>, MtopError> {
    let c = TcpStream::connect(host).await?;
    let (r, w) = c.into_split();
    Ok(Memcached::new(r, w))
}
