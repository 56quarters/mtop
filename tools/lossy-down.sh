#!/usr/bin/env bash

set -o errexit

exec tc qdisc delete dev lo root
