#!/bin/bash

set -e

cargo clean
echo "Building with no features"
cargo build --features=strict

declare -a features=("threaded" "threaded-rustls" "threaded-native-tls" "threaded-websockets" "threaded-websockets,threaded-rustls" "threaded-websockets,threaded-native-tls" "tokio" "tokio-rustls" "tokio-native-tls" "tokio-websockets" "tokio-websockets,tokio-rustls" "tokio-websockets,tokio-native-tls")

for i in "${features[@]}"
do
   echo "Building with features: $i"
   cargo build --features=strict,$i
done

echo "Building with all features"
cargo build --all-features

echo "Building examples..."
declare -a gneiss_mqtt_examples=("connect-plaintext-tokio" "connect-plaintext-threaded" "connect-proxy-tokio" "connect-proxy-threaded" "connect-tls-tokio" "connect-tls-threaded" "connect-websockets-tokio" "connect-websockets-threaded" "pubsub-threaded" "pubsub-tokio")
for i in "${gneiss_mqtt_examples[@]}"
do
   echo "Building $i"
   cargo build --package=$i
done
