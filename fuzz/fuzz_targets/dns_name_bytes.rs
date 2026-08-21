#![no_main]

use libfuzzer_sys::{Corpus, fuzz_target};
use mtop_client::dns::Name;
use std::io::Cursor;

fuzz_target!(|data: &[u8]| -> Corpus {
    let mut cur = Cursor::new(data);
    match Name::read_network_bytes(&mut cur) {
        Ok(n) => {
            let n = n.to_fqdn();
            let _ = n.to_string();
            let mut buf = Vec::new();
            n.write_network_bytes(&mut buf).unwrap();
            Corpus::Keep
        }
        Err(_e) => Corpus::Reject,
    }
});
