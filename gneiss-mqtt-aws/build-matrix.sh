#!/bin/bash

set -e

cargo clean

declare -a features=("tokio-rustls" "tokio-native-tls" "tokio-websockets,tokio-rustls" "tokio-websockets,tokio-native-tls", "threaded-rustls", "threaded-native-tls")

for i in "${features[@]}"
do
   echo "Building with features: $i"
   cargo build --features=strict,$i
done

echo "Building with all features"
cargo build --all-features

