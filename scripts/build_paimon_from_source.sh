#!/bin/bash
# Build Apache Paimon C++ from source with Arrow/ORC integration
# Usage: bash scripts/build_paimon_from_source.sh [install_prefix]

set -euo pipefail

INSTALL_PREFIX="${1:-/usr/local}"

echo "Building Apache Paimon C++ from source..."
echo "Install prefix: ${INSTALL_PREFIX}"

# Temporary build directory
PAIMON_BUILD_DIR="/tmp/paimon-cpp-build-$$"
mkdir -p "${PAIMON_BUILD_DIR}"

trap "rm -rf ${PAIMON_BUILD_DIR}" EXIT

# Clone paimon-cpp (stable branch)
echo "Cloning paimon-cpp repository..."
git clone --depth 1 --branch main https://github.com/alibaba/paimon-cpp.git "${PAIMON_BUILD_DIR}"

cd "${PAIMON_BUILD_DIR}"

# Create build directory
mkdir -p build
cd build

# Configure with Arrow support and static linking
echo "Configuring paimon-cpp with CMake..."
cmake .. \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
    -DCMAKE_PREFIX_PATH="${INSTALL_PREFIX}" \
    -DBUILD_SHARED_LIBS=OFF \
    -DARROW_FOUND=ON

# Calculate parallelism (use up to half available cores)
JOBS=$(( ($(nproc)/2) > 0 ? ($(nproc)/2 + 2) : 1 ))
echo "Building with ${JOBS} parallel jobs..."

cmake --build . -j"${JOBS}"

# Install
echo "Installing Paimon C++ library..."
cmake --install .

echo "Apache Paimon C++ installed successfully to ${INSTALL_PREFIX}"
