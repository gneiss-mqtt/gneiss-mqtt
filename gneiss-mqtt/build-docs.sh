#!/bin/bash

set -e

RUSTDOCFLAGS="--cfg docsrs" cargo +nightly doc --open --features="threaded,tokio"
