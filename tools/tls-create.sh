#!/usr/bin/env bash

set -o xtrace
set -o errexit

# From https://mariadb.com/docs/xpand/security/data-in-transit-encryption/create-self-signed-certificates-keys-openssl/

# From https://docs.joshuatz.com/cheatsheets/security/self-signed-ssl-certs/#openssl---generating-self-signed-cert-without-prompts

# CA private key and cert
openssl req \
        -newkey rsa:4096 -x509 -nodes -days 10000 -sha256 \
        -subj "/C=US/ST=MA/L=Boston/O=mtop/OU=test/CN=memcached-ca" \
        -keyout memcached-ca-key.pem -out memcached-ca-cert.pem

# server private key and cert
openssl req \
        -newkey rsa:4096 -x509 -nodes -days 1000 -sha256 \
        -subj "/C=US/ST=MA/L=Boston/O=mtop/OU=test/CN=memcached-server" \
        -addext "basicConstraints=critical, CA:false" \
        -addext "subjectAltName = DNS:localhost, DNS:memcached-server, IP:127.0.0.1"  \
        -keyout memcached-server-key.pem -out memcached-server-cert.pem \
        -CA memcached-ca-cert.pem -CAkey memcached-ca-key.pem

# client private key and cert
openssl req \
        -newkey rsa:4096 -x509 -nodes -days 10000 -sha256 \
        -subj "/C=US/ST=MA/L=Boston/O=mtop/OU=test/CN=memcached-client" \
        -addext "basicConstraints=critical, CA:false" \
        -addext "subjectAltName = DNS:localhost, DNS:memcached-client, IP:127.0.0.1" \
        -keyout memcached-client-key.pem -out memcached-client-cert.pem \
        -CA memcached-ca-cert.pem -CAkey memcached-ca-key.pem

# verify server
openssl verify -CAfile memcached-ca-cert.pem memcached-ca-cert.pem memcached-server-cert.pem

# verify client
openssl verify -CAfile memcached-ca-cert.pem memcached-ca-cert.pem memcached-client-cert.pem
