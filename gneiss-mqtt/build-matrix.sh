#!/bin/bash

set -e

cargo clean
cargo build

cargo clean
cargo build --features=threaded

cargo clean
cargo build --features=threaded-rustls

cargo clean
cargo build --features=threaded-native-tls

cargo clean
cargo build --features=threaded-websockets

cargo clean
cargo build --features=threaded-websockets,threaded-rustls

cargo clean
cargo build --features=threaded-websockets,threaded-native-tls

cargo clean
cargo build --features=tokio

cargo clean
cargo build --features=tokio-rustls

cargo clean
cargo build --features=tokio-native-tls

cargo clean
cargo build --features=tokio-websockets

cargo clean
cargo build --features=tokio-websockets,tokio-rustls

cargo clean
cargo build --features=tokio-websockets,tokio-native-tls

cargo clean
cargo build --all-features

