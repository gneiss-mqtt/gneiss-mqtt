#!/bin/bash

set -e

cargo clean

declare -a features=("tokio-rustls" "tokio-native-tls" "tokio-websockets,tokio-rustls" "tokio-websockets,tokio-native-tls" "threaded-rustls" "threaded-native-tls")

for i in "${features[@]}"
do
   echo "Building with features: $i"
   cargo build --features=strict,$i
done

echo "Building with all features"
cargo build --all-features

declare -a gneiss_mqtt_aws_examples=("aws-custom-auth-signed-threaded" "aws-custom-auth-signed-tokio" "aws-custom-auth-unsigned-threaded" "aws-custom-auth-unsigned-tokio" "aws-mtls-threaded" "aws-mtls-tokio" "aws-websockets-tokio")
for i in "${gneiss_mqtt_aws_examples[@]}"
do
   echo "Building $i"
   cargo build --package=$i
done