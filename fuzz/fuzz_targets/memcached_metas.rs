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
struct MetasResponse {
    lines: Vec<MetaLine>,
}

#[derive(Clone, Debug, arbitrary::Arbitrary)]
struct MetaLine {
    pairs: Vec<(String, String)>,
}

impl From<MetasResponse> for Vec<u8> {
    fn from(value: MetasResponse) -> Self {
        let mut out = Vec::new();

        for line in value.lines {
            for pair in line.pairs {
                write!(&mut out, "{}={} ", pair.0, urlencoding::encode(&pair.1)).unwrap();
            }

            out.extend_from_slice(b"\r\n");
        }

        out.extend_from_slice(b"END\r\n");
        out
    }
}

fuzz_target!(|data: MetasResponse| -> Corpus {
    let read: Cursor<Vec<u8>> = Cursor::new(data.into());
    let write = Vec::new();
    let mut conn = Memcached::new(read, write);
    let runtime = RT.get_or_init(|| Runtime::new().unwrap());

    match runtime.block_on(async { conn.metas().timeout(TIMEOUT, "fuzz").await }) {
        Ok(_v) => Corpus::Keep,
        Err(_e) => Corpus::Reject,
    }
});
