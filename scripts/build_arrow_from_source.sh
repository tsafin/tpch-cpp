#!/bin/bash
# Build Apache Arrow from source on systems where dev packages are unavailable

set -e

echo "=========================================="
echo "Building Apache Arrow from source"
echo "=========================================="
echo ""

# Configuration
ARROW_VERSION="13.0.0"
ARROW_URL="https://github.com/apache/arrow/releases/download/apache-arrow-${ARROW_VERSION}/apache-arrow-${ARROW_VERSION}.tar.gz"
INSTALL_PREFIX="${1:-.local}"  # Default to ~/.local, can be overridden
BUILD_DIR="/tmp/arrow-build-$$"

echo "Arrow Version: ${ARROW_VERSION}"
echo "Install Prefix: ${INSTALL_PREFIX}"
echo "Build Directory: ${BUILD_DIR}"
echo ""

# Step 1: Download Arrow source
echo "Step 1: Downloading Arrow source..."
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"
wget -q "${ARROW_URL}"
tar xzf "apache-arrow-${ARROW_VERSION}.tar.gz"
cd "apache-arrow-${ARROW_VERSION}/cpp"
echo "✓ Arrow source downloaded"
echo ""

# Step 2: Build Arrow
echo "Step 2: Building Arrow (this may take 5-10 minutes)..."
mkdir -p build
cd build
cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
    -DARROW_PARQUET=ON \
    -DARROW_CSV=ON \
    ..

cmake --build . --config Release -j$(nproc)
echo "✓ Arrow built successfully"
echo ""

# Step 3: Install Arrow
echo "Step 3: Installing Arrow to ${INSTALL_PREFIX}..."
cmake --install . --config Release
echo "✓ Arrow installed successfully"
echo ""

# Step 4: Update CMake to find Arrow
echo "Step 4: Updating environment..."
export CMAKE_PREFIX_PATH="${INSTALL_PREFIX}:${CMAKE_PREFIX_PATH}"
export PKG_CONFIG_PATH="${INSTALL_PREFIX}/lib/pkgconfig:${PKG_CONFIG_PATH}"
export LD_LIBRARY_PATH="${INSTALL_PREFIX}/lib:${LD_LIBRARY_PATH}"

# Save to ~/.bashrc for future sessions
echo ""
echo "To use Arrow in future sessions, add to ~/.bashrc:"
echo "  export CMAKE_PREFIX_PATH=\"${INSTALL_PREFIX}:\${CMAKE_PREFIX_PATH}\""
echo "  export PKG_CONFIG_PATH=\"${INSTALL_PREFIX}/lib/pkgconfig:\${PKG_CONFIG_PATH}\""
echo "  export LD_LIBRARY_PATH=\"${INSTALL_PREFIX}/lib:\${LD_LIBRARY_PATH}\""
echo ""

# Step 5: Test
echo "Step 5: Verifying installation..."
if pkg-config --cflags arrow > /dev/null 2>&1; then
    echo "✓ Arrow installation verified"
    echo ""
    echo "=========================================="
    echo "Arrow build complete!"
    echo "=========================================="
    echo ""
    echo "Build TPC-H project:"
    echo "  cd /home/tsafin/src/tpch-cpp"
    echo "  cmake -B build -DCMAKE_BUILD_TYPE=Release"
    echo "  cmake --build build"
else
    echo "⚠ Arrow installation verification failed"
    echo "Try setting CMAKE_PREFIX_PATH manually:"
    echo "  export CMAKE_PREFIX_PATH=\"${INSTALL_PREFIX}:\${CMAKE_PREFIX_PATH}\""
fi

# Cleanup
echo ""
echo "Cleaning up build directory..."
rm -rf "${BUILD_DIR}"
echo "✓ Cleanup complete"
