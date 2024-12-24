#!/bin/bash

set -e

RUSTDOCFLAGS="--cfg docsrs" cargo +nightly doc --open --features="tokio-rustls,tokio-websockets,tokio-native-tls,threaded-rustls,threaded-websockets,threaded-native-tls"
