#!/bin/bash

set -e

cargo clean
echo "Building with no features"
cargo build --features=strict

declare -a features=("threaded" "tokio")

for i in "${features[@]}"
do
   echo "Building with features: $i"
   cargo build --features=strict,$i
done

echo "Building with all features"
cargo build --all-features

echo "Building examples..."
declare -a gneiss_mqtt_examples=("connect-plaintext-tokio" "connect-plaintext-threaded" "pubsub-threaded" "pubsub-tokio")
for i in "${gneiss_mqtt_examples[@]}"
do
   echo "Building $i"
   cargo build --package=$i
done
