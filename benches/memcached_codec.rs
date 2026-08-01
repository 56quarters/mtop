use criterion::{Criterion, criterion_group, criterion_main};

const SHORT_TEXT: &str = "a%20short%20sentence%3B";
const LONG_TEXT: &str = "a%20long%20sentence%20that%20will%20require%20a%20fair%20bit%20of%20time%20to%20decode%2C%20on%20a%20relative%20scale%3B%20%26%20it%20should%20be%20a%20decent%20test%20of%20the%20%23%20of%20nano-seconds%20it%20takes%20for%20something%20like%20this%25";

fn memcached_url_decode(c: &mut Criterion) {
    c.bench_function("urlencoding::decode(short)", |b| {
        b.iter(|| {
            let _ = urlencoding::decode(SHORT_TEXT).unwrap();
        });
    });

    c.bench_function("urlencoding::decode(long)", |b| {
        b.iter(|| {
            let _ = urlencoding::decode(LONG_TEXT).unwrap();
        })
    });

    c.bench_function("mtop_client::url_decode(short)", |b| {
        b.iter(|| {
            let _ = mtop_client::url_decode(SHORT_TEXT).unwrap();
        });
    });

    c.bench_function("mtop_client::url_decode(long)", |b| {
        b.iter(|| {
            let _ = mtop_client::url_decode(LONG_TEXT).unwrap();
        })
    });
}

criterion_group!(memcached_codec, memcached_url_decode);
criterion_main!(memcached_codec);
