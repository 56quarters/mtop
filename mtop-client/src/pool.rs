use crate::core::MtopError;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use tokio::sync::Mutex;

/// Trait used by a client pool for creating new client instances when needed.
///
/// Implementations are expected to retain any required configuration for client
/// instances beyond the identifier for an instance (usually a server address).
pub trait ClientFactory<K, V> {
    /// Create a new client instance based on its ID.
    fn make(&self, key: &K) -> impl Future<Output = Result<V, MtopError>> + Send + Sync;
}

#[derive(Debug, Clone)]
pub(crate) struct ClientPoolConfig {
    pub name: String,
    pub max_idle: u64,
}

#[derive(Debug)]
pub(crate) struct ClientPool<K, V, F>
where
    K: Eq + Hash + Clone + fmt::Display,
    F: ClientFactory<K, V> + Send + Sync,
{
    clients: Mutex<HashMap<K, Vec<PooledClient<K, V>>>>,
    config: ClientPoolConfig,
    factory: F,
}

impl<K, V, F> ClientPool<K, V, F>
where
    K: Eq + Hash + Clone + fmt::Display,
    F: ClientFactory<K, V> + Send + Sync,
{
    pub(crate) fn new(config: ClientPoolConfig, factory: F) -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            config,
            factory,
        }
    }

    pub(crate) async fn get(&self, key: &K) -> Result<PooledClient<K, V>, MtopError> {
        // Lock the clients HashMap and try to get an existing client in a limited scope
        // so that we don't hold the lock while trying to connect if there are no exising
        // clients.
        let client = {
            let mut clients = self.clients.lock().await;
            clients.get_mut(key).and_then(|v| v.pop())
        };

        match client {
            Some(c) => {
                tracing::trace!(message = "using existing client", pool = self.config.name, server = %key);
                Ok(c)
            }
            None => {
                tracing::trace!(message = "creating new client", pool = self.config.name, server = %key);
                let inner = self.factory.make(key).await?;
                Ok(PooledClient {
                    key: key.clone(),
                    inner,
                })
            }
        }
    }

    pub(crate) async fn put(&self, client: PooledClient<K, V>) {
        let mut clients = self.clients.lock().await;
        let entries = clients.entry(client.key.clone()).or_default();
        if (entries.len() as u64) < self.config.max_idle {
            entries.push(client);
        }
    }
}

/// Wrapper for a client that belongs to a pool and must be returned
/// to the pool when complete.
#[derive(Debug)]
pub struct PooledClient<K, V> {
    key: K,
    inner: V,
}

impl<K, V> Deref for PooledClient<K, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V> DerefMut for PooledClient<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(test)]
mod test {
    use super::{ClientFactory, ClientPool, ClientPoolConfig};
    use crate::core::MtopError;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    struct CountingClient {
        dropped: Arc<AtomicU64>,
    }

    impl Drop for CountingClient {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::Release);
        }
    }

    struct CountingClientFactory {
        created: Arc<AtomicU64>,
        dropped: Arc<AtomicU64>,
    }

    impl ClientFactory<String, CountingClient> for CountingClientFactory {
        async fn make(&self, _key: &String) -> Result<CountingClient, MtopError> {
            self.created.fetch_add(1, Ordering::Release);

            Ok(CountingClient {
                dropped: self.dropped.clone(),
            })
        }
    }

    fn new_pool(
        created: Arc<AtomicU64>,
        dropped: Arc<AtomicU64>,
    ) -> ClientPool<String, CountingClient, CountingClientFactory> {
        let factory = CountingClientFactory {
            created: created.clone(),
            dropped: dropped.clone(),
        };
        let pool_cfg = ClientPoolConfig {
            name: "test".to_owned(),
            max_idle: 1,
        };

        ClientPool::new(pool_cfg, factory)
    }

    #[tokio::test]
    async fn test_client_pool_get_empty_pool() {
        let created = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let pool = new_pool(created.clone(), dropped.clone());

        let _client = pool.get(&"whatever".to_owned()).await.unwrap();

        assert_eq!(1, created.load(Ordering::Acquire));
        assert_eq!(0, dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_client_pool_get_existing_client() {
        let created = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let pool = new_pool(created.clone(), dropped.clone());

        let client1 = pool.get(&"whatever".to_owned()).await.unwrap();
        pool.put(client1).await;
        let _client2 = pool.get(&"whatever".to_owned()).await.unwrap();

        assert_eq!(1, created.load(Ordering::Acquire));
        assert_eq!(0, dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_client_pool_put_at_max_idle() {
        let created = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let pool = new_pool(created.clone(), dropped.clone());

        let client1 = pool.get(&"whatever".to_owned()).await.unwrap();
        let client2 = pool.get(&"whatever".to_owned()).await.unwrap();
        pool.put(client1).await;
        pool.put(client2).await;

        assert_eq!(2, created.load(Ordering::Acquire));
        assert_eq!(1, dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_client_pool_put_zero_max_idle() {
        let created = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let mut pool = new_pool(created.clone(), dropped.clone());
        pool.config.max_idle = 0;

        let client = pool.get(&"whatever".to_owned()).await.unwrap();
        pool.put(client).await;

        assert_eq!(1, created.load(Ordering::Acquire));
        assert_eq!(1, dropped.load(Ordering::Acquire));
    }
}
