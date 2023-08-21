# Changelog

## unreleased

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
