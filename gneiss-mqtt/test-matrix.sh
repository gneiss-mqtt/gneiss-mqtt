#!/bin/bash

set -e

cargo clean
echo "Testing with no features"
cargo test

declare -a features=("threaded" "threaded-rustls" "threaded-native-tls" "threaded-websockets" "threaded-websockets,threaded-rustls" "threaded-websockets,threaded-native-tls" "tokio" "tokio-rustls" "tokio-native-tls" "tokio-websockets" "tokio-websockets,tokio-rustls" "tokio-websockets,tokio-native-tls")

for i in "${features[@]}"
do
   cargo clean
   echo "Testing with features: $i"
   cargo test --features=$i
done

cargo clean
echo "Testing with all features"
cargo test --all-features

