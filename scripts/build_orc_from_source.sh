#!/bin/bash
# Build and install Apache ORC C++ from source using system dependencies.

set -e
set -x

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Install build dependencies
sudo apt-get update
sudo apt-get install -y \
    g++ libtool make cmake git wget \
    libprotobuf-dev protobuf-compiler \
    libsnappy-dev liblz4-dev libzstd-dev zlib1g-dev

# Clean up potential conflicting manual installations before building
# We remove vendored libs to ensure we don't accidentally link against them
sudo rm -f /usr/local/lib/liborc_vendored*.a

# Setup ORC submodule
ORC_DIR="${PROJECT_ROOT}/third_party/orc"
git -C "${PROJECT_ROOT}" submodule update --init --recursive third_party/orc

cd "${ORC_DIR}"
# Clean submodule to ensure no vendored artifacts remain from previous attempts
git clean -xfd

# Build and install
# We explicitly disable building tools/tests and force usage of system libraries
# to ensure ABI compatibility with Arrow.
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    -DBUILD_JAVA=OFF \
    -DBUILD_LIBHDFSPP=OFF \
    -DBUILD_CPP_TESTS=OFF \
    -DBUILD_TOOLS=OFF \
    -DORC_PREFER_STATIC_PROTOBUF=OFF \
    -DPROTOBUF_HOME=/usr \
    -DORC_PREFER_STATIC_ZLIB=OFF \
    -DZLIB_HOME=/usr \
    -DORC_PREFER_STATIC_ZSTD=OFF \
    -DZSTD_HOME=/usr \
    -DORC_PREFER_STATIC_LZ4=OFF \
    -DLZ4_HOME=/usr \
    -DORC_PREFER_STATIC_SNAPPY=OFF \
    -DSNAPPY_HOME=/usr

JOBS=$(( ($(nproc)/2) > 0 ? ($(nproc)/2) : 1 ))
cmake --build build -j"${JOBS}"

sudo cmake --install build

echo "Apache ORC C++ has been built and installed successfully."