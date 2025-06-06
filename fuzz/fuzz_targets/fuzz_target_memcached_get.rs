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
struct ValueResponse {
    key: String,
    flags: u64,
    len: u64,
    cas: u64,
    data: Vec<u8>,
}

impl From<ValueResponse> for Vec<u8> {
    fn from(v: ValueResponse) -> Self {
        let mut out = Vec::with_capacity(v.data.len());
        write!(&mut out, "VALUE {} {} {} {}\r\n", v.key, v.flags, v.len, v.cas).unwrap();
        out.extend_from_slice(&v.data);
        out
    }
}

fuzz_target!(|data: ValueResponse| -> Corpus {
    let read: Cursor<Vec<u8>> = Cursor::new(data.into());
    let write = Vec::new();
    let mut conn = Memcached::new(read, write);
    let runtime = RT.get_or_init(|| Runtime::new().unwrap());

    match runtime.block_on(async { conn.get(&[Key::one("foo").unwrap()]).timeout(TIMEOUT, "fuzz").await }) {
        Ok(_v) => Corpus::Keep,
        Err(_e) => Corpus::Reject,
    }
});
