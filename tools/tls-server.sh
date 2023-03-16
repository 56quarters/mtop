#!/usr/bin/env bash

set -o xtrace
set -o errexit

exec memcached -v -m 64 -Z -o ssl_ca_cert=memcached-ca-cert.pem,ssl_chain_cert=memcached-server-cert.pem,ssl_key=memcached-server-key.pem,ssl_verify_mode=2
