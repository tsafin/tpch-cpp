#!/bin/bash
# Build and install Apache Arrow C++ from source WITHOUT protobuf support.
#
# ORC includes protobuf message definitions (orc_proto.proto) that conflict
# with Arrow's protobuf definitions at runtime. By building Arrow without
# protobuf support, we eliminate the descriptor database collision error:
#   "File already exists in database: orc_proto.proto"
#
# Arrow doesn't need protobuf for CSV and Parquet support, so this is safe.
# Only advanced features like Flight and Substrait need protobuf.

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

# Install build dependencies (excluding protobuf to avoid conflicts with ORC)
sudo apt-get update
sudo apt-get install -y \
    g++ cmake git \
    libboost-all-dev rapidjson-dev \
    libbrotli-dev libthrift-dev \
    libsnappy-dev liblz4-dev libzstd-dev zlib1g-dev \
    pkg-config

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

export PKG_CONFIG_PATH="/usr/local/lib/pkgconfig:$PKG_CONFIG_PATH"

cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    -DCMAKE_MODULE_PATH="${ARROW_DIR}/cpp/cmake_modules" \
    -DCMAKE_PREFIX_PATH="${XSIMD_INSTALL_DIR};/usr/local" \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_ZSTD=ON \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_PARQUET=ON \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_COMPUTE=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_CSV=ON \
    -DARROW_WITH_PROTOBUF=OFF \
    -DARROW_ORC=OFF \
    -DARROW_FLIGHT=OFF \
    -DARROW_SUBSTRAIT=OFF

make -j$(nproc)
sudo make install

echo "Apache Arrow C++ has been built and installed successfully."
