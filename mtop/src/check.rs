use mtop_client::{DiscoveryDefault, Key, MemcachedClient, Timeout};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;

const KEY: &str = "mc-check-test";
const VALUE: &[u8] = "test".as_bytes();

/// Repeatedly make connections to a Memcached server to verify connectivity.
#[derive(Debug)]
pub struct Checker {
    client: MemcachedClient,
    resolver: DiscoveryDefault,
    delay: Duration,
    timeout: Duration,
    stop: Arc<AtomicBool>,
}

impl Checker {
    /// Create a new `Checker` that uses connections created from the provided `client`. `delay`
    /// is the amount of time to wait between each test. `timeout` is how long each individual
    /// part of the test may take (DNS resolution, connecting, setting a value, and fetching
    /// a value).
    pub fn new(
        client: MemcachedClient,
        resolver: DiscoveryDefault,
        delay: Duration,
        timeout: Duration,
        stop: Arc<AtomicBool>,
    ) -> Self {
        Self {
            client,
            resolver,
            delay,
            timeout,
            stop,
        }
    }

    /// Perform connection tests for a particular host in a loop and return information
    /// about the time taken for each step of the test (DNS resolution, connecting, setting
    /// a value, and fetching a value) and counts of failures or timeouts during each step.
    ///
    /// Note that each test run performs a DNS lookup and creates a brand-new connection.
    pub async fn run(&self, host: &str, time: Duration) -> Bundle {
        let mut dns_builder = TimingBuilder::default();
        let mut conn_builder = TimingBuilder::default();
        let mut set_builder = TimingBuilder::default();
        let mut get_builder = TimingBuilder::default();
        let mut overall_builder = TimingBuilder::default();
        let mut failures = Failures::default();
        let mut interval = time::interval(self.delay);
        let start = Instant::now();

        // Note that we don't return the connection to the client each iteration. This ensures
        // we're creating a new connection each time and thus actually testing the network
        // when doing the check.
        while !self.stop.load(Ordering::Acquire) && start.elapsed() < time {
            let _ = interval.tick().await;
            let key = Key::one(KEY).unwrap();
            let val = VALUE.to_vec();

            let dns_start = Instant::now();
            let server = match self
                .resolver
                .resolve(host)
                .timeout(self.timeout, "resolver.resolve_by_proto")
                .await
                .map(|mut v| v.pop())
            {
                Ok(Some(s)) => s,
                Ok(None) => {
                    tracing::warn!(message = "no addresses for host", host = host);
                    failures.total += 1;
                    failures.dns += 1;
                    continue;
                }
                Err(e) => {
                    tracing::warn!(message = "failed to resolve host", host = host, err = %e);
                    failures.total += 1;
                    failures.dns += 1;
                    continue;
                }
            };

            let dns_time = dns_start.elapsed();
            let conn_start = Instant::now();
            let mut conn = match self.client.raw_open(&server).timeout(self.timeout, "client.raw_open").await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(message = "failed to connect to host", host = host, addr = %server.address(), err = %e);
                    failures.total += 1;
                    failures.connections += 1;
                    continue;
                }
            };

            let conn_time = conn_start.elapsed();
            let set_start = Instant::now();
            match conn.set(&key, 0, 60, &val).timeout(self.timeout, "connection.set").await {
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(message = "failed to set key", host = host, addr = %server.address(), err = %e);
                    failures.total += 1;
                    failures.sets += 1;
                    continue;
                }
            }

            let set_time = set_start.elapsed();
            let get_start = Instant::now();
            match conn.get(&[key]).timeout(self.timeout, "connection.get").await {
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(message = "failed to get key", host = host, addr = %server.address(), err = %e);
                    failures.total += 1;
                    failures.gets += 1;
                    continue;
                }
            }

            let get_time = get_start.elapsed();
            let overall_time = dns_start.elapsed();

            tracing::info!(
                timeout = ?self.timeout,
                total = ?overall_time,
                dns = ?dns_time,
                connection = ?conn_time,
                set = ?set_time,
                get = ?get_time,
            );

            dns_builder.add(dns_time);
            conn_builder.add(conn_time);
            set_builder.add(set_time);
            get_builder.add(get_time);
            overall_builder.add(overall_time);
        }

        let dns = dns_builder.build();
        let connections = conn_builder.build();
        let gets = get_builder.build();
        let sets = set_builder.build();
        let overall = overall_builder.build();

        Bundle {
            overall,
            dns,
            connections,
            sets,
            gets,
            failures,
        }
    }
}

/// Accumulate measurements of how long a particular operation takes and compute
/// the min, max, average, and standard deviation of them.
#[derive(Debug)]
pub struct TimingBuilder {
    min: Duration,
    max: Duration,
    sum: f64,
    sum2: f64,
    count: u32,
}

impl Default for TimingBuilder {
    fn default() -> Self {
        Self {
            min: Duration::MAX,
            max: Duration::ZERO,
            sum: 0.0,
            sum2: 0.0,
            count: 0,
        }
    }
}

impl TimingBuilder {
    pub fn add(&mut self, d: Duration) {
        self.min = self.min.min(d);
        self.max = self.max.max(d);
        self.sum += d.as_secs_f64();
        self.sum2 += d.as_secs_f64() * d.as_secs_f64();
        self.count += 1;
    }

    pub fn build(&self) -> Timing {
        // Alternate method of calculating standard deviation that works in constant
        // memory and doesn't require keeping all samples. This is the same method that
        // the `man 8 ping` command uses.
        // See https://serverfault.com/a/333203
        let (avg, std_dev) = if self.count != 0 {
            let mean = self.sum / self.count as f64;
            let mean2 = self.sum2 / self.count as f64;
            (
                Duration::from_secs_f64(mean),
                Duration::from_secs_f64((mean2 - (mean * mean)).sqrt()),
            )
        } else {
            (Duration::ZERO, Duration::ZERO)
        };

        Timing {
            min: self.min,
            max: self.max,
            avg,
            std_dev,
        }
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct Timing {
    pub min: Duration,
    pub max: Duration,
    pub avg: Duration,
    pub std_dev: Duration,
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct Bundle {
    pub overall: Timing,
    pub dns: Timing,
    pub connections: Timing,
    pub sets: Timing,
    pub gets: Timing,
    pub failures: Failures,
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct Failures {
    pub total: u64,
    pub dns: u64,
    pub connections: u64,
    pub sets: u64,
    pub gets: u64,
}
