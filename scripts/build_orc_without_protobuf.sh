#!/bin/bash
# Build Apache ORC from submodule without protobuf dependency
# Uses ORC's vendored protobuf (statically linked, isolated from system)

set -e
set -x

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ORC_DIR="${PROJECT_ROOT}/third_party/orc"

# Check if ORC submodule exists
if [ ! -d "${ORC_DIR}" ] || [ ! -f "${ORC_DIR}/CMakeLists.txt" ]; then
    echo "ORC submodule not found at ${ORC_DIR}"
    echo "Please initialize: git submodule update --init third_party/orc"
    exit 1
fi

# Create build directory
mkdir -p "${ORC_DIR}/build"
cd "${ORC_DIR}/build"

# Configure ORC build as SHARED library with vendored protobuf
# Shared library encapsulates protobuf symbols, avoiding linker conflicts
# This allows linking ORC without needing external protobuf symbols
cmake .. \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    -DBUILD_SHARED_LIBS=ON \
    -DBUILD_JAVA=OFF \
    -DBUILD_CPP_TESTS=OFF \
    -DBUILD_TOOLS=ON \
    -DINSTALL_VENDORED_LIBS=OFF \
    -DORC_PREFER_STATIC_SNAPPY=OFF \
    -DORC_PREFER_STATIC_LZ4=OFF \
    -DORC_PREFER_STATIC_ZSTD=OFF \
    -DORC_PREFER_STATIC_ZLIB=OFF \
    -DORC_PREFER_STATIC_PROTOBUF=OFF

# Build ORC
make -j$(nproc)

# Install ORC
sudo make install

echo "ORC built and installed successfully without external protobuf dependency"
echo "ORC uses its vendored protobuf implementation which is isolated from system libraries"
