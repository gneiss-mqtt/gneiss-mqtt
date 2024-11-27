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
declare -a gneiss_mqtt_aws_features=("tokio-rustls" "tokio-native-tls" "tokio-websockets,tokio-rustls" "tokio-websockets,tokio-native-tls", "threaded-rustls", "threaded-native-tls")

for i in "${gneiss_mqtt_aws_features[@]}"
do
   echo "Building gneiss-mqtt-aws with features: $i"
   cargo build --package=gneiss-mqtt-aws --features=strict,$i
done

echo "Building gneiss-mqtt-aws with all features"
cargo build --package=gneiss-mqtt-aws --all-features

echo "Building elastigneiss variants..."
echo "Building elasti-gneiss-tokio"
cargo build --package=elasti-gneiss-tokio
echo "Building elasti-gneiss-threaded"
cargo build --package=elasti-gneiss-threaded
echo "Building elasti-gneiss-aws-tokio"
cargo build --package=elasti-gneiss-aws-tokio
echo "Building elasti-gneiss-aws-threaded"
cargo build --package=elasti-gneiss-aws-threaded