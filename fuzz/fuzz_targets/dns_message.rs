#![no_main]

use libfuzzer_sys::{Corpus, fuzz_target};
use mtop_client::dns::Message;
use std::io::Cursor;

fuzz_target!(|data: &[u8]| -> Corpus {
    let mut cur = Cursor::new(data);
    if let Ok(m) = Message::read_network_bytes(&mut cur) {
        let mut buf = Vec::new();
        m.write_network_bytes(&mut buf).unwrap();
        Corpus::Keep
    } else {
        Corpus::Reject
    }
});
