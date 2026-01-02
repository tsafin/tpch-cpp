#!/bin/bash
# Install TPC-H C++ dependencies on Ubuntu/Debian systems

set -e

echo "Installing TPC-H C++ dependencies..."

# Update package list
sudo apt update
# Install dependencies
sudo apt install -y \
    ca-certificates lsb-release wget \
    cmake build-essential \
    liburing-dev

# Add Apache Arrow repository
wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
sudo apt install -y libarrow-dev libparquet-dev


echo "Dependencies installed successfully!"
echo ""
echo "To build the project, run:"
echo "  mkdir build && cd build"
echo "  cmake -DCMAKE_BUILD_TYPE=Release .."
echo "  make -j$(nproc)"
