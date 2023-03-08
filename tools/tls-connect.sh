#!/usr/bin/env bash

set -o errexit

exec openssl s_client -CAfile memcached-ca-cert.pem -cert memcached-client-cert.pem -key memcached-client-key.pem -connect localhost:11211
