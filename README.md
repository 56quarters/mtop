# mtop

![build status](https://github.com/56quarters/mtop/actions/workflows/rust.yml/badge.svg)
[![docs.rs](https://docs.rs/mtop/badge.svg)](https://docs.rs/mtop/)
[![crates.io](https://img.shields.io/crates/v/mtop.svg)](https://crates.io/crates/mtop/)

mtop: `top` for Memcached.

![mtop](/images/mtop.png)

## Features

* Display real-time statistics about your `memcached` servers such as
  * Memory usage/limit
  * Current/max connections
  * Hit ratio
  * Gets/Sets/Evictions
  * Bytes transmitted and received
  * Server CPU usage
* Support for monitoring multiple servers: `mtop example1:11211 example2:11211 example3:11211`

## Install

There are multiple ways to install `mtop` listed below.

### Binaries

TBD

### Cargo

`mtop` along with its dependencies can be downloaded and built from source using the
Rust `cargo` tool. Note that this requires you have a Rust toolchain installed.

To install:

```
cargo install mtop
```

To install as a completely static binary (Linux only):

```
cargo install --target=x86_64-unknown-linux-musl mtop 
```

To uninstall:

```
cargo uninstall mtop
```

### Source

`mtop` along with its dependencies can be built from the latest sources on Github using
the Rust `cargo` tool. Note that this requires you have Git and a Rust toolchain installed.

Get the sources:

```
git clone https://github.com/56quarters/mtop.git && cd mtop
```

Build a binary:

```
cargo build --release
./target/release/mtop --help
```

Build a completely static binary (Linux only):

```
cargo build --target=x86_64-unknown-linux-musl --release
./target/x86_64-unknown-linux-musl/release/mtop --help
```

These binaries should then be copied on to your `$PATH`.

## Usage

`mtop` takes one or more Memcached `host:port` combinations as arguments. Statistics from
each of these  servers will be collected approximately once a second. A maximum of ten
measurements from each server will be kept in memory to use for computations. Some examples
of invoking `mtop` are given below.

### Connecting to a local server
```
mtop localhost:11211
```

### Connecting to multiple servers

```
mtop cache01.example.com:11211 cache02.example.com:11211 cache03.example.com:11211
```

### Connecting to a port-forwarded Kubernetes pod

```
kubectl port-forward --namespace=example memcached-0 11211:11211
mtop localhost:11211
```

## Limitations

### No TLS connections

`mtop` currently only supports plain-text (unencrypted) connections to servers. TLS support
*may* be added in a future version.

### Errors break the UI

Any errors connecting to Memcached servers are logged to `stderr` by default. This causes UI
glitches and may require restarting `mtop` to resolve. Any messages logged are also lost when
`mtop` exits. This will be fixed in a future version.

### No historical data

`mtop` displays instantaneous statistics or an average over the last 10 seconds (depending on
the particular statistic). It does not persist statistics anywhere for historical analysis. If
this is something you need, use the [memcached_exporter](https://github.com/prometheus/memcached_exporter)
for Prometheus.

## License

mtop is available under the terms of the [GPL, version 3](LICENSE).

### Contribution

Any contribution intentionally submitted  for inclusion in the work by you
shall be licensed as above, without any additional terms or conditions.
