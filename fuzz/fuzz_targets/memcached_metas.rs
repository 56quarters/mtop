#![no_main]

use libfuzzer_sys::{Corpus, arbitrary, fuzz_target};
use mtop_client::Memcached;
use std::io::{Cursor, Write};
use std::sync::OnceLock;
use tokio::runtime::Runtime;

static RT: OnceLock<Runtime> = OnceLock::new();

#[derive(Clone, Debug, arbitrary::Arbitrary)]
struct MetasResponse {
    ok: bool,
    lines: Vec<MetaLine>,
}

#[derive(Clone, Debug, arbitrary::Arbitrary)]
struct MetaLine {
    key: String,
    exp: u64,
    la: u64,
    cas: u64,
    fetch: bool,
    cls: usize,
    size: usize,
    flags: u64,
}

impl From<MetasResponse> for Vec<u8> {
    fn from(value: MetasResponse) -> Self {
        let mut out = Vec::new();
        if value.ok {
            out.extend_from_slice(b"OK\r\n");
        }

        for line in value.lines {
            write!(
                &mut out,
                "key={} exp={} la={} cas={} fetch={} cls={} size={} flags={}\r\n",
                mtop_client::url_encode(&line.key),
                line.exp,
                line.la,
                line.cas,
                if line.fetch { "yes" } else { "no" },
                line.cls,
                line.size,
                line.flags,
            )
            .unwrap();
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

    match runtime.block_on(async { conn.metas().await }) {
        Ok(_v) => Corpus::Keep,
        Err(_e) => Corpus::Reject,
    }
});
