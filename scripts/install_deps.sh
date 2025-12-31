#!/bin/bash
# Install TPC-H C++ dependencies on Ubuntu/Debian systems

set -e

echo "Installing TPC-H C++ dependencies..."

# Update package list
sudo apt-get update

# Install dependencies
sudo apt-get install -y \
    cmake \
    build-essential \
    libarrow-dev \
    libparquet-dev \
    liborc-dev \
    liburing-dev

echo "Dependencies installed successfully!"
echo ""
echo "To build the project, run:"
echo "  mkdir build && cd build"
echo "  cmake -DCMAKE_BUILD_TYPE=Release .."
echo "  make -j$(nproc)"
