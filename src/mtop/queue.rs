use crate::client::Measurement;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct MeasurementQueue {
    queues: Mutex<HashMap<String, VecDeque<Measurement>>>,
    max_size: usize,
}

impl MeasurementQueue {
    pub fn new(max_size: usize) -> Self {
        MeasurementQueue {
            max_size,
            queues: Mutex::new(HashMap::new()),
        }
    }

    pub async fn insert(&self, host: String, m: Measurement) {
        let mut map = self.queues.lock().await;
        let q = map.entry(host).or_insert_with(|| VecDeque::new());

        q.push_back(m);
        if q.len() > self.max_size {
            q.pop_front();
        }
    }

    pub async fn read(&self, host: &str) -> Option<Measurement> {
        let map = self.queues.lock().await;
        map.get(host).and_then(|q| q.front()).map(|o| o.clone())
    }
}

#[derive(Debug)]
pub struct BlockingMeasurementQueue {
    queue: Arc<MeasurementQueue>,
    handle: Handle,
}

impl BlockingMeasurementQueue {
    pub fn new(queue: Arc<MeasurementQueue>, handle: Handle) -> Self {
        BlockingMeasurementQueue { queue, handle }
    }

    pub fn insert(&self, host: String, m: Measurement) {
        self.handle.block_on(self.queue.insert(host, m))
    }

    pub fn read(&self, host: &str) -> Option<Measurement> {
        self.handle.block_on(self.queue.read(host))
    }
}
