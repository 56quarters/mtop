#![no_main]

use libfuzzer_sys::{Corpus, arbitrary, fuzz_target};
use mtop_client::{Key, Memcached, Timeout};
use std::io::{Cursor, Write};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::runtime::Runtime;

const TIMEOUT: Duration = Duration::from_millis(10);

static RT: OnceLock<Runtime> = OnceLock::new();

#[derive(Debug, Clone, arbitrary::Arbitrary)]
struct ValuesResponse {
    lines: Vec<ValueLine>,
}

#[derive(Debug, Clone, arbitrary::Arbitrary)]
struct ValueLine {
    key: String,
    flags: u64,
    len: u64,
    cas: u64,
    data: Vec<u8>,
}

impl From<ValuesResponse> for Vec<u8> {
    fn from(v: ValuesResponse) -> Self {
        let mut out = Vec::new();

        for val in v.lines {
            write!(&mut out, "VALUE {} {} {} {}\r\n", val.key, val.flags, val.len, val.cas).unwrap();
            out.extend_from_slice(&val.data);
        }

        out.extend_from_slice(b"END\r\n");
        out
    }
}

fuzz_target!(|data: ValuesResponse| -> Corpus {
    // Get all the keys from the generated data, ignoring invalid ones. This doesn't
    // actually affect the data returned and the parsing logic short-circuits and returns
    // an error after the first value it can't parse _but_ this allows us to make sure
    // writing keys to the server works well enough.
    let keys: Vec<Key> = data.lines.iter().flat_map(|l| Key::one(&l.key)).collect();

    let read: Cursor<Vec<u8>> = Cursor::new(data.into());
    let write = Vec::new();
    let mut conn = Memcached::new(read, write);
    let runtime = RT.get_or_init(|| Runtime::new().unwrap());

    match runtime.block_on(async { conn.get(&keys).timeout(TIMEOUT, "fuzz").await }) {
        Ok(_v) => Corpus::Keep,
        Err(_e) => Corpus::Reject,
    }
});
