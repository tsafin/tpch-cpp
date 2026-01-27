#!/bin/bash
###############################################################################
# CI Build Arrow and ORC Script
#
# Builds Apache Arrow (without protobuf) and optionally ORC (as static library)
# from source. Results are installed to specified directory and suitable for caching
# in GitHub Actions CI.
#
# Usage: bash scripts/ci_build_arrow_and_orc.sh [orc_enabled] [install_prefix]
#   orc_enabled: ON or OFF (default: OFF)
#   install_prefix: Installation directory (default: /usr/local)
#
# This script is called from CI build jobs. Outputs are cached and restored
# by benchmark jobs to avoid rebuilding on every test.
###############################################################################

set -euo pipefail

ORC_ENABLED="${1:-OFF}"
INSTALL_PREFIX="${2:-/usr/local}"

echo "═══════════════════════════════════════════════════════════════"
echo "Building Arrow and ORC from source (CI Mode)"
echo "═══════════════════════════════════════════════════════════════"
echo "ORC enabled: $ORC_ENABLED"
echo "Install prefix: $INSTALL_PREFIX"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Step 1: Build Arrow from source (without protobuf)
echo "[1/2] Building Apache Arrow from source..."
bash "${SCRIPT_DIR}/build_arrow_from_source.sh" "${INSTALL_PREFIX}"

# Step 2: Build ORC from source if enabled
if [ "$ORC_ENABLED" = "ON" ]; then
    echo ""
    echo "[2/2] Building Apache ORC from source..."
    # Install additional dependency needed for ORC
    sudo apt-get update -qq
    sudo apt-get install -y -qq libtool
    bash "${SCRIPT_DIR}/build_orc_from_source.sh" "${INSTALL_PREFIX}"
else
    echo ""
    echo "[2/2] ORC build skipped (ORC_ENABLED=OFF)"
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "Arrow and ORC build complete!"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "Built libraries are installed in ${INSTALL_PREFIX}:"
echo "  - Arrow/Parquet C++ libraries: ${INSTALL_PREFIX}/lib/libarrow.*, ${INSTALL_PREFIX}/lib/libparquet.*"
if [ "$ORC_ENABLED" = "ON" ]; then
    echo "  - ORC C++ library: ${INSTALL_PREFIX}/lib/liborc.a (static)"
fi
echo "  - Headers: ${INSTALL_PREFIX}/include/"
echo ""
echo "These can be cached in CI and restored for benchmark jobs."
echo ""
