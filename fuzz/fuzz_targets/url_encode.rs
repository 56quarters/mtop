#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: String| {
    let _ = mtop_client::url_encode(&data);
});
