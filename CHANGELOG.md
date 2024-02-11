# Changelog

## v0.9.0 - unreleased

- Change host selection in `mtop` to allow more host tabs than fit on the
  screen. #119
- Change default colors in `mtop` to dark theme based on tailwind color
  palette. #122

## v0.8.0 - 2024-02-04

- Add default 5 second timeout to network operations done by `mtop`. #90
- Add default 30 second timeout to network operaitons done by `mc`. #111
- Add `bench`, `incr`, `decr`, `add`, and `replace` commands to `mc`. #95 #98 #111
- TLS related dependency updates. #93
- Create high-level client for operating on multiple servers. #101

## v0.7.0 - 2023-11-28

- Build binaries for Linux Musl libc target. #83
- Move "Bytes tx" under "Gets" and "Bytes rx" under "Sets". #84
- Create a `check` subcommand for `mc` to test connections to a server. #86

## v0.6.9 - 2023-10-11

- Build Docker images for arm64 architecture. #78

## v0.6.8 - 2023-10-07

- Build Docker images for each release. #72

## v0.6.7 - 2023-09-09

- Perform health checks on Memcached connections in the connection pool. #63
- Improve performance of metadata parsing, e.g. `mc keys`. #64

## v0.6.3 - 2023-08-24

- Split Memcached client into a separate crate. #58
- Require `--tls-cert` flag if `--tls-key` is present and vice versa in `mtop` and `mc`. #58

## v0.6.2 - 2023-08-21

- Update dependencies. #49 #53
- Change slab `max age` display format to `HH:MM:SS`. #52
- Fix inconsistent byte formatting behavior. #55

## v0.6.1 - 2023-07-20

- Fetch stats from each server in `mtop` in parallel. #46

## v0.6.0 - 2023-07-10

- Add UI to `mtop` for per-slab metrics. #41
- Add support for `dns+` hostname prefix to resolve a DNS name to multiple hosts. #43
- Update dependencies. #44

## v0.5.1 - 2023-06-12

- Fix a bug where stats for some servers were not updated if another server returned an error. #34

## v0.5.0 - 2023-04-30

- Minor performance improvement when running `mc keys`. #26
- Build binary artifacts for tags using `cargo-dist`. #27

## v0.4.2 - 2023-04-18

- Exit the `mtop` UI on `CTRL-c` in addition to `q`. #19

## v0.4.1 - 2023-03-22

- Fixed an issue where `mc keys` would fail for items without a TTL. #16

## v0.4.0 - 2023-03-17

- Add the ability to use mTLS connections to Memcached for `mc` and `mtop`. #13
- Add `--details` flag to `mc keys` to show item expiration time and size. #14

## v0.3.0 - 2023-03-04

- Introduce `mc` binary for running Memcached operations from the command line. #7

## v0.2.0 - 2023-02-22

- Errors fetching stats are logged to a file instead of `stderr`. #6

## v0.1.1 - 2023-02-09

- Documentation improvements. #3 #4 #5
- Log errors to `stderr` instead of `stdout`. #2

## v0.1.0 - 2023-02-06

- Initial release.
