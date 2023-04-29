use crate::client::Stats;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct StatsDelta {
    pub previous: Stats,
    pub current: Stats,
    pub seconds: u64,
}

#[derive(Debug)]
pub struct StatsQueue {
    queues: Mutex<HashMap<String, VecDeque<Stats>>>,
    max_size: usize,
}

impl StatsQueue {
    pub fn new(max_size: usize) -> Self {
        StatsQueue {
            max_size,
            queues: Mutex::new(HashMap::new()),
        }
    }

    pub async fn insert(&self, host: String, m: Stats) {
        let mut map = self.queues.lock().await;
        let q = map.entry(host).or_insert_with(VecDeque::new);

        if let Some(prev) = q.back() {
            if m.uptime == prev.uptime {
                // The most recent entry in the queue has the same uptime as the measurement we're
                // inserting now. This can happen when the server is under heavy load and it isn't
                // able to update the variable it uses to keep track of uptime. Instead of clearing
                // the entire measurement queue, skip this insert instead (less disruptive to users
                // to miss a measurement than cause the UI to reset).
                tracing::debug!(
                    message = "server uptime did not advance, dropping measurement",
                    old_uptime = prev.uptime,
                    current_uptime = m.uptime,
                );
                return;
            } else if m.uptime < prev.uptime {
                // The most recent entry in the queue has a higher uptime than the measurement
                // we're inserting now. This means there was a restart and counters are all reset
                // to 0. Clear the existing queue to avoid underflow (since we assume counters can
                // only increase).
                tracing::debug!(
                    message = "server uptime reset detected, clearing measurement queue",
                    old_uptime = prev.uptime,
                    current_uptime = m.uptime
                );
                q.clear();
            } else if m.server_time <= prev.server_time {
                // Make sure that any calculations that depend on the number of seconds elapsed
                // since the last measurement don't divide by zero. Realistically this shouldn't
                // happen unless we get really unlucky with the frequency of updates or the
                // memcached server is being purposefully malicious but it doesn't hurt to be
                // defensive.
                tracing::debug!(
                    message = "server timestamp did not advance, dropping measurement",
                    old_timestamp = prev.server_time,
                    current_timestamp = m.server_time
                );
                return;
            }
        }

        q.push_back(m);
        if q.len() > self.max_size {
            q.pop_front();
        }
    }

    pub async fn read_delta(&self, host: &str) -> Option<StatsDelta> {
        let map = self.queues.lock().await;
        map.get(host).and_then(|q| match (q.front(), q.back()) {
            // The delta is only valid if there are more than two entries in the queue. This
            // avoids division by zero errors (since the time for the entries would be the same).
            (Some(previous), Some(current)) if q.len() >= 2 => {
                let seconds = current.server_time - previous.server_time;
                Some(StatsDelta {
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
pub struct BlockingStatsQueue {
    queue: Arc<StatsQueue>,
    handle: Handle,
}

impl BlockingStatsQueue {
    pub fn new(queue: Arc<StatsQueue>, handle: Handle) -> Self {
        BlockingStatsQueue { queue, handle }
    }

    pub fn insert(&self, host: String, m: Stats) {
        self.handle.block_on(self.queue.insert(host, m))
    }

    pub fn read_delta(&self, host: &str) -> Option<StatsDelta> {
        self.handle.block_on(self.queue.read_delta(host))
    }
}
