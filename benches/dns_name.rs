use criterion::{criterion_group, criterion_main, Criterion};
use mtop_client::dns::Name;
use std::io::Cursor;
use std::str::FromStr;

fn dns_name_constructors(c: &mut Criterion) {
    c.bench_function("Name::from_str", |b| {
        b.iter(|| {
            let _ = Name::from_str("dev.www.example.com.").unwrap();
        });
    });

    #[rustfmt::skip]
    c.bench_function("Name::read_network_bytes", |b| {
        let mut cur = Cursor::new(vec![
            7,                                // length
            101, 120, 97, 109, 112, 108, 101, // "example"
            3,                                // length
            99, 111, 109,                     // "com"
            0,                                // root
            3,                                // length
            119, 119, 119,                    // "www"
            192, 0,                           // pointer to offset 0
            3,                                // length
            100, 101, 118,                    // "dev"
            192, 13,                          // pointer to offset 13, "www"
        ]);

        b.iter(|| {
            cur.set_position(19);
            let _ = Name::read_network_bytes(&mut cur).unwrap();
        });
    });
}

fn dns_name_methods(c: &mut Criterion) {
    c.bench_function("Name::to_string", |b| {
        let name = Name::from_str("www.example.com.").unwrap();
        b.iter(|| {
            let _ = name.to_string();
        });
    });
}

criterion_group!(dns_name_group, dns_name_constructors, dns_name_methods);
criterion_main!(dns_name_group);
