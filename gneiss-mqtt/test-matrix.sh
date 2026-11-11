#!/bin/bash

set -e

cargo clean
echo "Testing with no features"
cargo test --features=strict

declare -a features=("threaded" "tokio")

for i in "${features[@]}"
do
   echo "Testing with features: $i"
   cargo test --features=strict,$i
done

echo "Testing with all features"
cargo test --all-features

