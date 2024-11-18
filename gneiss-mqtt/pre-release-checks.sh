#!/bin/bash

set -e

./build-matrix.sh
./clippy.sh
./test-matrix.sh

