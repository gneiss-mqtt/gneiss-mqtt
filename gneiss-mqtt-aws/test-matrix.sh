#!/bin/bash

set -e

cargo clean

declare -a features=("threaded-rustls" "threaded-native-tls" "threaded-rustls,threaded-native-tls" "tokio-rustls" "tokio-native-tls" "tokio-rustls,tokio-native-tls" "tokio-websockets,tokio-rustls" "tokio-websockets,tokio-native-tls")

for i in "${features[@]}"
do
   echo "Testing with features: $i"
   cargo test --features=strict,$i
done

echo "Testing with all features"
cargo test --all-features

