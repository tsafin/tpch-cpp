# ORC Docker image extending base with ORC support for TPC-H benchmarks
ARG BASE_IMAGE=ghcr.io/tsafin/tpch-cpp-base:latest
FROM ${BASE_IMAGE}

LABEL org.opencontainers.image.source="https://github.com/tsafin/tpch-cpp"
LABEL org.opencontainers.image.description="TPC-H C++ ORC Build Environment with Arrow/Parquet/ORC"
LABEL maintainer="tsafin"

ENV INSTALL_PREFIX=/opt/dependencies

# Copy ORC build script and submodule
WORKDIR /tmp/build
COPY scripts/build_orc_from_source.sh scripts/build_orc_from_source.sh
COPY third_party/orc third_party/orc

# Build and install ORC as static library
RUN cd third_party/orc && \
    mkdir -p build && \
    cd build && \
    cmake .. \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} \
        -DBUILD_LIBHDFSPP=OFF \
        -DBUILD_JAVA=OFF \
        -DBUILD_TOOLS=OFF \
        -DBUILD_CPP_TESTS=OFF \
        -DINSTALL_VENDORED_LIBS=OFF \
        -DBUILD_SHARED_LIBS=OFF && \
    make -j$(nproc) && \
    make install && \
    cd /tmp && \
    rm -rf /tmp/build

# Verify installation
RUN ldconfig && \
    ls -lh ${INSTALL_PREFIX}/lib/liborc* && \
    echo "✓ ORC image with Arrow/Parquet/ORC compiled successfully"

WORKDIR /workspace
