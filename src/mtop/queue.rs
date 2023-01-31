use crate::client::Measurement;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct MeasurementDelta {
    pub previous: Measurement,
    pub current: Measurement,
    pub seconds: u64,
}

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
        let q = map.entry(host).or_insert_with(VecDeque::new);

        // If the last entry in the queue (most recent) has a higher uptime than the
        // measurement we're inserting now, there was a memcached restart and counters
        // are all reset to 0. Clear the existing queue to avoid underflow (since
        // we assume counters can only increase).
        if let Some(prev) = q.back() {
            if m.uptime < prev.uptime {
                q.clear()
            }
        }

        q.push_back(m);
        if q.len() > self.max_size {
            q.pop_front();
        }
    }

    pub async fn read_delta(&self, host: &str) -> Option<MeasurementDelta> {
        let map = self.queues.lock().await;
        map.get(host).and_then(|q| match (q.front(), q.back()) {
            // The delta is only valid if there are more than two entries in the queue. This
            // avoids division by zero errors (since the time for the entries would be the same).
            (Some(previous), Some(current)) if q.len() >= 2 => {
                let seconds = (current.time - previous.time) as u64;
                Some(MeasurementDelta {
                    previous: previous.clone(),
                    current: current.clone(),
                    seconds,
                })
            }
            _ => None,
        })
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

    pub fn read_delta(&self, host: &str) -> Option<MeasurementDelta> {
        self.handle.block_on(self.queue.read_delta(host))
    }
}
