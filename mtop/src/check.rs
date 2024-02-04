use mtop_client::{DiscoveryDefault, Key, MemcachedClient};
use std::cmp;
use std::time::{Duration, Instant};
use tokio::time;

const KEY: &str = "mc-check-test";
const VALUE: &[u8] = "test".as_bytes();

/// Repeatedly make connections to a Memcached server to verify connectivity.
#[derive(Debug)]
pub struct Checker<'a> {
    client: &'a MemcachedClient,
    resolver: &'a DiscoveryDefault,
    delay: Duration,
    timeout: Duration,
}

impl<'a> Checker<'a> {
    /// Create a new `Checker` that uses connections created from the provided `pool`. `delay`
    /// is the amount of time to wait between each test. `timeout` is how long each individual
    /// part of the test may take (DNS resolution, connecting, setting a value, and fetching
    /// a value).
    pub fn new(
        client: &'a MemcachedClient,
        resolver: &'a DiscoveryDefault,
        delay: Duration,
        timeout: Duration,
    ) -> Self {
        Self {
            client,
            resolver,
            delay,
            timeout,
        }
    }

    /// Perform connection tests for a particular hosts in a loop and return information
    /// about the time taken for each step of the test (DNS resolution, connecting, setting
    /// a value, and fetching a value) and counts of failures or timeouts during each step.
    ///
    /// Note that each test run performs a DNS lookup and creates a brand new connection.
    pub async fn run(&self, host: &str, time: Duration) -> TimingBundle {
        let mut dns_builder = TimingBuilder::default();
        let mut conn_builder = TimingBuilder::default();
        let mut set_builder = TimingBuilder::default();
        let mut get_builder = TimingBuilder::default();
        let mut total_builder = TimingBuilder::default();
        let mut failures = Failures::default();
        let mut interval = time::interval(self.delay);
        let start = Instant::now();

        // Note that we don't return the connection to the client each iteration. This ensures
        // we're creating a new connection each time and thus actually testing the network
        // when doing the check.
        while start.elapsed() < time {
            let _ = interval.tick().await;
            let key = Key::one(KEY).unwrap();
            let val = VALUE.to_vec();

            let dns_start = Instant::now();
            let server = match time::timeout(self.timeout, self.resolver.resolve_by_proto(host))
                .await
                .map(|r| r.map(|mut v| v.pop()))
            {
                Ok(Ok(Some(s))) => s,
                Ok(Ok(None)) => {
                    tracing::warn!(message = "no addresses for host", host = host);
                    failures.total += 1;
                    failures.dns += 1;
                    continue;
                }
                Ok(Err(e)) => {
                    tracing::warn!(message = "failed to resolve host", host = host, err = %e);
                    failures.total += 1;
                    failures.dns += 1;
                    continue;
                }
                Err(_) => {
                    tracing::warn!(message = "timeout resolving host", host = host);
                    failures.total += 1;
                    failures.dns += 1;
                    continue;
                }
            };

            let dns_time = dns_start.elapsed();
            let conn_start = Instant::now();
            let mut conn = match time::timeout(self.timeout, self.client.raw_open(&server)).await {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => {
                    tracing::warn!(message = "failed to connect to host", host = host, addr = %server.addr(), err = %e);
                    failures.total += 1;
                    failures.connections += 1;
                    continue;
                }
                Err(_) => {
                    tracing::warn!(message = "timeout connecting to host", host = host, addr = %server.addr());
                    failures.total += 1;
                    failures.connections += 1;
                    continue;
                }
            };

            let conn_time = conn_start.elapsed();
            let set_start = Instant::now();
            match time::timeout(self.timeout, conn.set(&key, 0, 60, &val)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    tracing::warn!(message = "failed to set key", host = host, addr = %server.addr(), err = %e);
                    failures.total += 1;
                    failures.sets += 1;
                    continue;
                }
                Err(_) => {
                    tracing::warn!(message = "timeout setting key", host = host, addr = %server.addr());
                    failures.total += 1;
                    failures.sets += 1;
                    continue;
                }
            }

            let set_time = set_start.elapsed();
            let get_start = Instant::now();
            match time::timeout(self.timeout, conn.get(&[key])).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    tracing::warn!(message = "failed to get key", host = host, addr = %server.addr(), err = %e);
                    failures.total += 1;
                    failures.gets += 1;
                    continue;
                }
                Err(_) => {
                    tracing::warn!(message = "timeout getting key", host = host, addr = %server.addr());
                    failures.total += 1;
                    failures.gets += 1;
                    continue;
                }
            }

            let get_time = get_start.elapsed();
            let total_time = dns_start.elapsed();

            tracing::info!(
                timeout = ?self.timeout,
                total = ?total_time,
                dns = ?dns_time,
                connection = ?conn_time,
                set = ?set_time,
                get = ?get_time,
            );

            dns_builder.add(dns_time);
            conn_builder.add(conn_time);
            set_builder.add(set_time);
            get_builder.add(get_time);
            total_builder.add(total_time);
        }

        let dns = dns_builder.build();
        let connections = conn_builder.build();
        let gets = get_builder.build();
        let sets = set_builder.build();
        let total = total_builder.build();

        TimingBundle {
            total,
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
#[derive(Debug, Default)]
pub struct TimingBuilder {
    times: Vec<Duration>,
}

impl TimingBuilder {
    pub fn add(&mut self, d: Duration) {
        self.times.push(d);
    }

    pub fn build(self) -> Timing {
        let mut min = Duration::MAX;
        let mut max = Duration::ZERO;
        let mut sum = Duration::ZERO;
        let count = self.times.len();

        for d in self.times.iter().cloned() {
            min = cmp::min(min, d);
            max = cmp::max(max, d);
            sum += d;
        }

        let avg = sum.checked_div(count as u32).unwrap_or(Duration::ZERO);
        let avg_f64 = avg.as_secs_f64();
        let mut deviance = 0f64;

        for d in self.times.iter() {
            deviance += (d.as_secs_f64() - avg_f64).powi(2)
        }

        let std_dev = if count != 0 {
            Duration::from_secs_f64((deviance / count as f64).sqrt())
        } else {
            Duration::ZERO
        };

        Timing { min, max, avg, std_dev }
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
pub struct TimingBundle {
    pub total: Timing,
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
