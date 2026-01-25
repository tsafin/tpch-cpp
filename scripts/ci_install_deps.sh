#!/bin/bash

###############################################################################
# CI Dependency Installation Script
#
# Installs all required dependencies for TPC-H C++ project on Ubuntu 22.04.
# Optimized for GitHub Actions CI environment with system packages.
#
# Usage: bash scripts/ci_install_deps.sh
###############################################################################

set -euo pipefail

echo "═══════════════════════════════════════════════════════════════"
echo "Installing TPC-H C++ Dependencies (CI Mode)"
echo "═══════════════════════════════════════════════════════════════"

# Detect OS version
if ! command -v lsb_release &> /dev/null; then
    echo "ERROR: lsb_release not found. This script requires Ubuntu/Debian."
    exit 1
fi

OS_NAME=$(lsb_release --id --short)
OS_VERSION=$(lsb_release --codename --short)

echo "[INFO] Detected: $OS_NAME $OS_VERSION"

# Update package lists
echo "[INFO] Updating package lists..."
sudo apt-get update -qq

# Core build tools
echo "[INFO] Installing build tools..."
sudo apt-get install -y -qq \
    cmake \
    build-essential \
    git \
    pkg-config \
    wget

# Add Apache Arrow APT repository (required for libarrow-dev and libparquet-dev)
echo "[INFO] Adding Apache Arrow APT repository..."
ARROW_DEB="apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb"
wget "https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/$ARROW_DEB"
sudo apt-get install -y -qq "./$ARROW_DEB"
rm "$ARROW_DEB"
sudo apt-get update -qq

# Arrow + Parquet development libraries
echo "[INFO] Installing Arrow and Parquet libraries..."
sudo apt-get install -y -qq \
    libarrow-dev \
    libparquet-dev

# Protobuf (required for Apache ORC)
echo "[INFO] Installing Protobuf..."
sudo apt-get install -y -qq \
    libprotobuf-dev \
    protobuf-compiler

# Async I/O support (io_uring)
echo "[INFO] Installing async I/O library (liburing)..."
sudo apt-get install -y -qq \
    liburing-dev

# Compression libraries (required by Arrow and ORC)
echo "[INFO] Installing compression libraries..."
sudo apt-get install -y -qq \
    libzstd-dev \
    liblz4-dev \
    libsnappy-dev

# Additional dependencies
echo "[INFO] Installing additional dependencies..."
sudo apt-get install -y -qq \
    libthrift-dev \
    libre2-dev \
    libtool

# Build Apache ORC from source
echo ""
echo "[INFO] Building Apache ORC from source..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

if [ -f "${SCRIPT_DIR}/build_orc_from_source.sh" ]; then
    bash "${SCRIPT_DIR}/build_orc_from_source.sh"
else
    echo "[WARN] build_orc_from_source.sh not found, skipping ORC build"
fi

# Python (for benchmark log parsing)
echo "[INFO] Installing Python..."
sudo apt-get install -y -qq \
    python3

# Verify critical packages
echo ""
echo "[INFO] Verifying installed packages..."

packages_to_verify=(
    "libarrow-dev"
    "libparquet-dev"
    "liburing-dev"
    "libzstd-dev"
)

all_verified=true
for pkg in "${packages_to_verify[@]}"; do
    if dpkg -l | grep -q "^ii.*$pkg"; then
        echo "  ✓ $pkg"
    else
        echo "  ✗ $pkg (WARNING: not installed)"
        all_verified=false
    fi
done

if [ "$all_verified" = false ]; then
    echo ""
    echo "[WARN] Some packages may not be installed. Continuing anyway."
fi

# Initialize git submodules for build requirements
echo ""
echo "[INFO] Initializing git submodules..."
git submodule update --init --recursive --depth 1

# Verify dists.dss exists (required by dbgen)
if [ -f "third_party/tpch/dbgen/dists.dss" ]; then
    echo "  ✓ dists.dss found"
else
    echo "  ✗ dists.dss not found (will be copied during CMake)"
fi

# Report installation summary
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "Dependency installation complete!"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "Next steps:"
echo "  cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo"
echo "  cmake --build build -j\$(nproc)"
echo ""
