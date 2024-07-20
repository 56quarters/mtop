# DNS in `mtop`

The `mtop` and `mc` binaries use DNS for both connecting to and discovering
Memcached servers. They use both the system resolver and an internal DNS
resolver based on the server hostnames provided. How that works and design
decisions made for the internal DNS resolver are detailed below.

## How DNS is used

If hostnames given to `mtop` or `mc` are prefixed with special strings, the
internal DNS resolver is used to perform an initial resolution depending on
the prefix. If the hostnames given to `mtop` are `mc` are _not_ prefixed with
the special strings described below, no initial resolution is performed.

If the hostname is prefixed with `dns+`, the hostname is resolved as `A`
and `AAAA` records. The IP address of each record will be treated as a
Memcached server. This resolution is only performed a single time at
application start.

If the hostname is prefixed with `dnssrv+`, the hostname is resolved as a
`SRV` record. The `target` field of each record will be treated as a Memcached
server. The `port` field of the `SRV` record is ignored. Instead, the port
portion of the server name from command line arguments is used. The `target`
used for the Memcached server is resolved at connection time using the system
resolver.

## Internal DNS resolver

`mtop` and `mc` use an internal DNS resolver when given hostnames that are
prefixed with special strings (`dns+` and `dnssrv+`). How resolution is
performed is described below.

* Configuration is loaded from [`/etc/resolv.conf`](https://en.wikipedia.org/wiki/Resolv.conf).
  Configuration is only loaded a single time at application start up. Only a
  subset of possible settings and options are supported. Notably, **search
  domains are not supported**. All supplied hostnames are assumed to be fully
  qualified. The supported settings and options are:
    * Top-level setting `nameserver` - An IP address of DNS server to use for
      resolution. If not set, `127.0.0.1` is used as a default.
    * Option `timeout:n` - Number of seconds after which a DNS request is
      considered timed out. If not set, `5` seconds is used as a default.
    * Option `attempts:n` - Total number of attempts to make to resolve a
      hostname. If not set, `2` attempts are made.
    * Option `rotate` - If set, use a different `nameserver` for each request
      instead of always starting with the first one. If not set, each request
      will always start with the first `nameserver`.
* If the hostname has a special prefix, either `A` and `AAAA` resolutions of
  the hostname are performed or a `SRV` resolution is performed.
    * A `dns+` prefix will cause `A` and `AAAA` resolutions to be performed.
      All results from these two DNS queries are used as Memcached servers with
      the port supplied as a command line argument. If using TLS, the hostname
      supplied on the command line is used to validate to the server
      certificate (unless overridden by a command line flag).
    * A `dnssrv+` prefix will cause a `SRV` resolution to be performed. The
      `target` of each result is used as a Memcached server with the port
      supplied as a command line argument. If using TLS, the hostname supplied
      on the command line is used to validate the server certificate (unless
      override by a command line flag). If the `target` is a hostname, it will
      be resolved at connection time using the system resolver.
* If the hostname does not have a special prefix, no resolution using the
  internal DNS resolver is done. Instead, the hostname or hostnames are
  each treated as individual servers. They will be resolved to `A` or `AAAA`
  records at connection time using the system resolver. If using TLS, the
  hostname supplied on the command line is used to validate the server
  certificate (unless overridden by a command line flag).

## Limitations

These limitations apply when the internal DNS resolver is used.

* [`resolv.conf`](https://en.wikipedia.org/wiki/Resolv.conf) is only loaded a
  single time at application start up. This usually won't be a problem in
  practice because the internal DNS resolver is only used for an initial
  lookup when using `dns+` or `dnssrv+` prefixes.
* [Search domains](https://en.wikipedia.org/wiki/Search_domain) are not
  supported. Every domain is assumed to already be fully qualified.
* [EDNS](https://en.wikipedia.org/wiki/Extension_Mechanisms_for_DNS) is not
  supported. One implication of this is that only the default UDP packet size
  of 512 bytes is used. DNS responses that do not fit into a 512 byte UDP
  packet will cause TCP fallback to be used.
* Results are not cached based on record TTLs or otherwise. This usually won't
  be a problem in practice because the internal DNS resolver is only used for
  an initial lookup when using `dns+` or `dnssrv+` prefixes.
