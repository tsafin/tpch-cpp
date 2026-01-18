#!/bin/bash
# Build and install Apache Arrow C++ from source.

set -e
set -x

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ARROW_DIR="${PROJECT_ROOT}/third_party/arrow"
XSIMD_DIR="${PROJECT_ROOT}/third_party/xsimd"
XSIMD_INSTALL_DIR="${PROJECT_ROOT}/build/xsimd_install"

# Check if Arrow source directory exists
if [ ! -d "${ARROW_DIR}" ] || [ ! -f "${ARROW_DIR}/cpp/CMakeLists.txt" ]; then
    echo "Arrow source not found in ${ARROW_DIR}."
    echo "Please add the git submodule first:"
    echo "git submodule add --depth 1 https://github.com/apache/arrow.git third_party/arrow"
    exit 1
fi

# Check if xsimd source directory exists
if [ ! -d "${XSIMD_DIR}" ] || [ ! -f "${XSIMD_DIR}/CMakeLists.txt" ]; then
    echo "xsimd source not found in ${XSIMD_DIR}."
    echo "Please ensure the xsimd submodule is initialized:"
    echo "git submodule update --init --recursive third_party/xsimd"
    exit 1
fi

# Install build dependencies
sudo apt-get update
sudo apt-get install -y \
    g++ cmake git libboost-all-dev rapidjson-dev \
    libbrotli-dev libthrift-dev \
    libsnappy-dev liblz4-dev libzstd-dev zlib1g-dev

# 1. Build and install xsimd from the submodule to a local directory
echo "Building and installing xsimd locally..."
mkdir -p "${XSIMD_DIR}/build"
cd "${XSIMD_DIR}/build"
cmake .. -DCMAKE_INSTALL_PREFIX="${XSIMD_INSTALL_DIR}"
make install

# 2. Build and install Arrow C++, pointing to our local xsimd installation
echo "Building and installing Apache Arrow..."
mkdir -p "${ARROW_DIR}/cpp/build"
cd "${ARROW_DIR}/cpp/build"

cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    -DCMAKE_PREFIX_PATH="${XSIMD_INSTALL_DIR}" \
    -DARROW_DEPENDENCY_SOURCE=SYSTEM \
    -DARROW_ORC=OFF \
    -DARROW_WITH_PROTOBUF=OFF \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_COMPUTE=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_JEMALLOC=ON \
    -DARROW_PARQUET=ON \
    -DPARQUET_BUILD_PROTOBUF=OFF \
    -DPARQUET_WITH_PROTOBUF=OFF \
    -DPARQUET_MINIMAL_DEPENDENCY=ON \
    -DARROW_CSV=ON

make -j$(nproc)
sudo make install

echo "Apache Arrow C++ has been built and installed successfully."
