use criterion::{Criterion, criterion_group, criterion_main};

const SHORT_ENCODED: &str = "a%20short%20sentence%3B";
const LONG_ENCODED: &str = "a%20long%20sentence%20that%20will%20require%20a%20fair%20bit%20of%20time%20to%20decode%2C%20on%20a%20relative%20scale%3B%20%26%20it%20should%20be%20a%20decent%20test%20of%20the%20%23%20of%20nano-seconds%20it%20takes%20for%20something%20like%20this%25";
const SHORT_PLAIN: &str = "a short sentence;";
const LONG_PLAIN: &str = "a long sentence that will require a fair bit of time to decode, on a relative scale; & it should be a decent test of the # of nano-seconds it takes for something like this%";

fn bench_url_decode(c: &mut Criterion) {
    c.bench_function("mtop_client::url_decode(short)", |b| {
        b.iter(|| {
            let _ = mtop_client::url_decode(SHORT_ENCODED).unwrap();
        });
    });

    c.bench_function("mtop_client::url_decode(long)", |b| {
        b.iter(|| {
            let _ = mtop_client::url_decode(LONG_ENCODED).unwrap();
        });
    });
}

fn bench_url_encode(c: &mut Criterion) {
    c.bench_function("mtop_client::url_encode(short)", |b| {
        b.iter(|| {
            let _ = mtop_client::url_encode(SHORT_PLAIN);
        });
    });

    c.bench_function("mtop_client::url_encode(long)", |b| {
        b.iter(|| {
            let _ = mtop_client::url_encode(LONG_PLAIN);
        });
    });
}

criterion_group!(client_codec, bench_url_decode, bench_url_encode);
criterion_main!(client_codec);
