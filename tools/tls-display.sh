#!/usr/bin/env bash

set -o xtrace
set -o errexit

openssl x509 -text -noout -in memcached-ca-cert.pem
openssl x509 -text -noout -in memcached-server-cert.pem
openssl x509 -text -noout -in memcached-client-cert.pem
