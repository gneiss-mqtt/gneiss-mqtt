#!/bin/bash

set -e

cargo clean
cargo clippy --features=tokio-rustls,tokio-native-tls,tokio-websockets,threaded-rustls,threaded-native-tls,threaded-websockets,strict
