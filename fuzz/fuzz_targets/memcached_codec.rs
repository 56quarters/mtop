#![no_main]

use libfuzzer_sys::{Corpus, fuzz_target};

fuzz_target!(|data: String| -> Corpus {
    match mtop_client::url_decode(&data) {
        Ok(_v) => Corpus::Keep,
        Err(_e) => Corpus::Reject,
    }
});
