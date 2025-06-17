#![no_main]

use libfuzzer_sys::{Corpus, arbitrary, fuzz_target};
use mtop_client::{Memcached, Timeout};
use std::io::{Cursor, Write};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::runtime::Runtime;

const TIMEOUT: Duration = Duration::from_millis(10);

static RT: OnceLock<Runtime> = OnceLock::new();

#[derive(Clone, Debug, arbitrary::Arbitrary)]
struct StatsResponse {
    lines: Vec<(String, u64)>,
}

impl From<StatsResponse> for Vec<u8> {
    fn from(value: StatsResponse) -> Self {
        let mut out = Vec::new();

        for line in value.lines {
            write!(&mut out, "STAT {} {}\r\n", line.0, line.1).unwrap();
        }

        out.extend_from_slice(b"END\r\n");
        out
    }
}

fuzz_target!(|data: StatsResponse| -> Corpus {
    let read: Cursor<Vec<u8>> = Cursor::new(data.into());
    let write = Vec::new();
    let mut conn = Memcached::new(read, write);
    let runtime = RT.get_or_init(|| Runtime::new().unwrap());

    match runtime.block_on(async { conn.stats().timeout(TIMEOUT, "fuzz").await }) {
        Ok(_v) => Corpus::Keep,
        Err(_e) => Corpus::Reject,
    }
});
