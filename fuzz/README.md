# Fuzz Testing

Some parts of the `mtop` and `mtop-client` libraries are fuzz tested.
Instructions for running fuzz tests are below.

## Setup

Follow the [setup instructions](https://rust-fuzz.github.io/book/cargo-fuzz/setup.html) from the Rust fuzz testing book.

## Running

After you have installed `cargo-fuzz` and selected the `nightly` toolchain for `mtop`, execute the 
various `mtop` and `mtop-client` fuzz targets from the root of the `mtop` repository.

```
cargo fuzz run fuzz_target_dns_message
```

```
cargo fuzz run fuzz_target_dns_name
```

```
cargo fuzz run fuzz_target_memcached_get
```

```
cargo fuzz run fuzz_target_memcached_metas
```

```
cargo fuzz run fuzz_target_memcached_stats
```

Each of these commands will run until it encounters a panic or crash.
You may want to stop them early if they haven't found anything after a reasonable time (30 minutes or more). 