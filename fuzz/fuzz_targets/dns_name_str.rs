#![no_main]

use libfuzzer_sys::{Corpus, fuzz_target};
use mtop_client::dns::Name;
use std::str::FromStr;

fuzz_target!(|data: String| -> Corpus {
    match Name::from_str(&data) {
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
