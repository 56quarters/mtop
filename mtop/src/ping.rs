use crate::check::{Timing, TimingBuilder};
use mtop_client::dns::{DefaultDnsClient, DnsClient, MessageId, Name, RecordClass, RecordType, ResponseCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::time;
use tracing::{Instrument, Level};

/// Repeatedly make DNS requests to resolve a domain name.
#[derive(Debug)]
pub struct DnsPinger {
    client: DefaultDnsClient,
    interval: Duration,
    stop: Arc<AtomicBool>,
}

impl DnsPinger {
    /// Create a new `DnsPinger` that uses the provided client to repeatedly resolve a domain
    /// name. `interval` is the amount of time to wait between each DNS query.
    pub fn new(client: DefaultDnsClient, interval: Duration, stop: Arc<AtomicBool>) -> Self {
        Self { client, interval, stop }
    }

    /// Perform DNS queries for a particular domain name up to `count` times or until stopped
    /// via the `stop` flag if `count` is `0` (indicating "infinite" queries).
    pub async fn run(&self, name: Name, rtype: RecordType, rclass: RecordClass, count: u64) -> Bundle {
        let mut timing_builder = TimingBuilder::default();
        let mut total = 0;
        let mut protocol_errors = 0;
        let mut fatal_errors = 0;

        let mut interval = time::interval(self.interval);

        while !self.stop.load(Ordering::Acquire) && (count == 0 || total < count) {
            let _ = interval.tick().await;
            // Create our own Instant to measure the time taken to perform the query since
            // the one emitted by the interval isn't _immediately_ when the future resolves
            // and so skews the measurement of queries.
            let start = Instant::now();

            match self
                .client
                .resolve(MessageId::random(), name.clone(), rtype, rclass)
                .instrument(tracing::span!(Level::INFO, "client.resolve"))
                .await
            {
                Ok(r) => {
                    let min_ttl = r.answers().iter().map(|a| a.ttl()).min().unwrap_or(0);
                    let elapsed = start.elapsed();
                    timing_builder.add(elapsed);

                    // Warn if the name couldn't be resolved correctly. This is different from
                    // an error like a timeout since we did actually get a response for the query
                    if r.flags().get_response_code() != ResponseCode::NoError {
                        tracing::warn!(
                            id = %r.id(),
                            name = %name,
                            response_code = ?r.flags().get_response_code(),
                            num_questions = r.questions().len(),
                            num_answers = r.answers().len(),
                            num_authority = r.authority().len(),
                            num_extra = r.extra().len(),
                            min_ttl = min_ttl,
                            elapsed = ?elapsed,
                        );
                        protocol_errors += 1;
                    } else {
                        tracing::info!(
                            id = %r.id(),
                            name = %name,
                            response_code = ?r.flags().get_response_code(),
                            num_questions = r.questions().len(),
                            num_answers = r.answers().len(),
                            num_authority = r.authority().len(),
                            num_extra = r.extra().len(),
                            min_ttl = min_ttl,
                            elapsed = ?elapsed,
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(message = "failed to resolve", name = %name, err = %e);
                    fatal_errors += 1;
                }
            }

            total += 1;
        }

        let timing = timing_builder.build();
        Bundle {
            timing,
            total,
            protocol_errors,
            fatal_errors,
        }
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct Bundle {
    pub timing: Timing,
    pub total: u64,
    pub protocol_errors: u64,
    pub fatal_errors: u64,
}
