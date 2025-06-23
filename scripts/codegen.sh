#!/usr/bin/env bash
set -euo pipefail

capnp compile -orust:./src/generated/ --src-prefix=src/schema ./src/schema/poc.capnp
cargo fmt
