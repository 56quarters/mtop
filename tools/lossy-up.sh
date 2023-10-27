#!/usr/bin/env bash

set -o errexit

exec tc qdisc add dev lo root netem loss 5% 25%
