use mtop_client::{MemcachedClient, MtopError, Timeout};
use rand::Rng;
use rand_distr::Exp;
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::time;
use tokio::time::Instant;

const MAX_ITEM_SIZE: usize = 64 * 1024;
const NUM_KEYS: usize = 1000;
const GET_BATCH_SIZE: usize = 100;

#[derive(Debug, Clone, Copy, PartialOrd, PartialEq)]
#[repr(transparent)]
pub struct Percent(f64);

impl Percent {
    pub fn unchecked(v: f64) -> Self {
        assert!(v >= 0.0, "percent must be >= 0.0");
        assert!(v <= 1.0, "percent must be <= 1.0");
        Self(v)
    }

    pub fn as_f64(&self) -> f64 {
        self.0
    }
}

impl FromStr for Percent {
    type Err = MtopError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v = s
            .parse()
            .map_err(|e| MtopError::configuration_cause(format!("invalid percent {}", s), e))?;

        if !(0.0..=1.0).contains(&v) {
            Err(MtopError::configuration(format!("invalid percent {}", v)))
        } else {
            Ok(Self(v))
        }
    }
}

impl fmt::Display for Percent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
pub struct Bencher {
    client: Arc<MemcachedClient>,
    handle: Handle,
    delay: Duration,
    timeout: Duration,
    ttl: Duration,
    write_percent: Percent,
    concurrency: usize,
}

impl Bencher {
    pub fn new(
        client: MemcachedClient,
        handle: Handle,
        delay: Duration,
        timeout: Duration,
        ttl: Duration,
        write_percent: Percent,
        concurrency: usize,
    ) -> Self {
        Self {
            client: Arc::new(client),
            handle,
            delay,
            timeout,
            ttl,
            write_percent,
            concurrency,
        }
    }
    pub async fn run(&self, time: Duration) -> Vec<Summary> {
        let run = Arc::new(AtomicBool::new(true));
        let mut tasks = Vec::with_capacity(self.concurrency);

        for worker in 0..self.concurrency {
            // Copy everything needed for the future so that we don't capture "self".
            let mut interval = time::interval(self.delay);
            let run = run.clone();
            let client = self.client.clone();
            let write_percent = self.write_percent;
            let ttl = self.ttl.as_secs() as u32;
            let timeout = self.timeout;

            tasks.push((worker, self.handle.spawn(async move {
                let fixture = fixture_data(worker, NUM_KEYS);
                let mut stats = Summary {  worker, ..Default::default() };

                while run.load(Ordering::Acquire) {
                    let set_start = interval.tick().await;

                    for kv in fixture.kvs.iter() {
                        // Write a small percentage of fixture data because cache workloads skew read heavy.
                        if rand::thread_rng().gen_bool(write_percent.as_f64()) {
                            if let Err(e) = client.set(&kv.key, 0, ttl, fixture.payload(kv)).timeout(timeout, "client.set").await {
                                tracing::debug!(message = "unable to set item", key = kv.key, payload_size = kv.len, err = %e);
                            } else {
                                stats.sets += 1;
                            }
                        }
                    }

                    stats.sets_time += set_start.elapsed();
                    let get_start = Instant::now();

                    for batch in fixture.kvs.chunks(GET_BATCH_SIZE) {
                        let keys: Vec<&String> = batch.iter().map(|kv| &kv.key).collect();
                        match client.get(keys).timeout(timeout, "client.get").await {
                            Ok(v) => {
                                stats.gets += GET_BATCH_SIZE as u64;
                                for (id, e) in v.errors {
                                    tracing::debug!(message = "error getting items", first_key = batch.first().map(|kv| &kv.key), server = %id, err = %e);
                                }
                            }
                            Err(e) => {
                                tracing::debug!(message = "unable to get items", first_key = batch.first().map(|kv| &kv.key), err = %e);
                            }
                        }
                    }

                    stats.gets_time += get_start.elapsed();
                }

                stats
            })));
        }

        // Let each of the benchmark tasks run for the given time and then set the "stop"
        // flag which they check each test iteration.
        tokio::time::sleep(time).await;
        run.store(false, Ordering::Release);
        let mut out = Vec::with_capacity(self.concurrency);

        for (worker, t) in tasks {
            match t.await {
                Ok(m) => out.push(m),
                Err(e) => {
                    tracing::error!(message = "unable to run benchmark task", worker = worker, err = %e);
                }
            }
        }

        out
    }
}

fn fixture_data(worker: usize, num: usize) -> FixtureData {
    let mut kvs = Vec::with_capacity(num);
    let mut rng = rand::thread_rng();
    let payload = b"x".repeat(MAX_ITEM_SIZE);
    // Using a "lambda" value of 10 means that most numbers end up in the 0 to 1.0 range
    // with the occasional value over 1.0. We're generating sizes of test data, so it doesn't
    // really matter if we occasionally have values over 1.0.
    let dist = Exp::new(10.0).unwrap();

    for i in 0..num {
        let key = format!("mc-bench-{}-{}", worker, i);
        let unit = rng.sample(dist);
        // Each KV pair is actually a key and length of the payload. We don't need to
        // store a copy of the payload since the actual contents don't matter, we'll just
        // grab a subslice of it when we need to write to the cache.
        let len = (MAX_ITEM_SIZE as f64 * unit) as usize;
        kvs.push(KVPair { key, len })
    }

    FixtureData { payload, kvs }
}

struct FixtureData {
    payload: Vec<u8>,
    kvs: Vec<KVPair>,
}

impl FixtureData {
    fn payload(&self, kv: &KVPair) -> &[u8] {
        &self.payload[0..kv.len.min(self.payload.len())]
    }
}

struct KVPair {
    key: String,
    len: usize,
}

#[derive(Debug, Default)]
pub struct Summary {
    pub gets_time: Duration,
    pub gets: u64,
    pub sets_time: Duration,
    pub sets: u64,
    pub worker: usize,
}

impl Summary {
    pub fn gets_per_sec(&self) -> f64 {
        self.gets as f64 / self.gets_time.as_secs_f64()
    }

    pub fn sets_per_sec(&self) -> f64 {
        self.sets as f64 / self.sets_time.as_secs_f64()
    }
}
