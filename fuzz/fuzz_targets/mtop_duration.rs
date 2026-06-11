#![no_main]

use libfuzzer_sys::{Corpus, fuzz_target};
use mtop::duration::DurationString;

fuzz_target!(|data: String| -> Corpus {
    match data.parse::<DurationString>() {
        Ok(_v) => Corpus::Keep,
        Err(_e) => Corpus::Reject,
    }
});
