#!/bin/bash

set -e

cargo clean

echo "Building gneiss-mqtt..."
echo "Building gneiss-mqtt with no features"
cargo build --package=gneiss-mqtt --features=strict

declare -a gneiss_mqtt_features=("threaded" "threaded-rustls" "threaded-native-tls" "threaded-websockets" "threaded-websockets,threaded-rustls" "threaded-websockets,threaded-native-tls" "tokio" "tokio-rustls" "tokio-native-tls" "tokio-websockets" "tokio-websockets,tokio-rustls" "tokio-websockets,tokio-native-tls")

for i in "${gneiss_mqtt_features[@]}"
do
   echo "Building gneiss-mqtt with features: $i"
   cargo build --package=gneiss-mqtt --features=strict,$i
done

echo "Building gneiss-mqtt with all features"
cargo build --package=gneiss-mqtt --all-features

echo "Building gneiss-mqtt-aws..."
declare -a gneiss_mqtt_aws_features=("tokio-rustls" "tokio-native-tls" "tokio-websockets,tokio-rustls" "tokio-websockets,tokio-native-tls" "threaded-rustls" "threaded-native-tls")

for i in "${gneiss_mqtt_aws_features[@]}"
do
   echo "Building gneiss-mqtt-aws with features: $i"
   cargo build --package=gneiss-mqtt-aws --features=strict,$i
done

echo "Building gneiss-mqtt-aws with all features"
cargo build --package=gneiss-mqtt-aws --all-features

echo "Building elastigneiss variants..."
declare -a elastigneiss_projects=("elasti-gneiss-tokio" "elasti-gneiss-threaded" "elasti-gneiss-aws-tokio" "elasti-gneiss-aws-threaded")
for i in "${elastigneiss_projects[@]}"
do
   echo "Building $i"
   cargo build --package=$i
done

echo "Building gneiss-mqtt examples..."
declare -a gneiss_mqtt_examples=("connect-plaintext-tokio" "connect-plaintext-threaded")
for i in "${gneiss_mqtt_examples[@]}"
do
   echo "Building $i"
   cargo build --package=$i
done

echo "Building gneiss-mqtt-aws examples..."
declare -a gneiss_mqtt_aws_examples=("aws-custom-auth-signed-threaded" "aws-custom-auth-signed-tokio" "aws-custom-auth-unsigned-threaded" "aws-custom-auth-unsigned-tokio" "aws-mtls-threaded" "aws-mtls-tokio" "aws-websockets-tokio")
for i in "${gneiss_mqtt_aws_examples[@]}"
do
   echo "Building $i"
   cargo build --package=$i
done