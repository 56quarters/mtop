#![no_main]

use libfuzzer_sys::{Corpus, fuzz_target};
use mtop_client::MtopError;
use mtop_client::dns::Name;
use std::io::Cursor;
use std::str::FromStr;

fuzz_target!(|data: &[u8]| -> Corpus {
    // Try parsing the bytes as a binary DNS name
    let mut cur = Cursor::new(data);
    if let Ok(n) = Name::read_network_bytes(&mut cur) {
        let n = n.to_fqdn();
        let _ = n.to_string();
        let mut buf = Vec::new();
        n.write_network_bytes(&mut buf).unwrap();
        return Corpus::Keep;
    }

    // Try parsing the bytes as a text representation of the name
    if let Ok(n) = str::from_utf8(data)
        .map_err(|e| MtopError::runtime_cause("utf-8", e))
        .and_then(Name::from_str)
    {
        let n = n.to_fqdn();
        let _ = n.to_string();
        let mut buf = Vec::new();
        n.write_network_bytes(&mut buf).unwrap();
        return Corpus::Keep;
    }

    Corpus::Reject
});
