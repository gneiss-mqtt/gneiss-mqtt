#!/bin/bash

set -e

cargo clean
echo "Building with no features"
cargo build

declare -a features=("threaded" "threaded-rustls" "threaded-native-tls" "threaded-websockets" "threaded-websockets,threaded-rustls" "threaded-websockets,threaded-native-tls" "tokio" "tokio-rustls" "tokio-native-tls" "tokio-websockets" "tokio-websockets,tokio-rustls" "tokio-websockets,tokio-native-tls")

for i in "${features[@]}"
do
   cargo clean
   echo "Building with features: $i"
   cargo build --features=$i
done

cargo clean
echo "Building with all features"
cargo build --all-features

