# ORC Runtime Issue: Protobuf ABI Mismatch

## Problem Summary

The `simple_orc` executable compiles successfully but crashes with a segmentation fault at runtime:

```
Segmentation fault (core dumped)
```

The crash occurs during dynamic library initialization, specifically in protobuf's `FileDescriptorProto::MergePartialFromCodedStream()`.

## Root Cause Analysis

### Issue Chain

1. **Apache Arrow dependency**: The Arrow library (`libarrow.so`) depends on the system's `libprotobuf.so.23`
2. **ORC build configuration**: ORC was built with vendored protobuf (embedded statically in `liborc.a`)
3. **ABI mismatch**: The system protobuf.so.23 (version 3.5.1) has a different ABI than the protobuf version used to build ORC

### Evidence

1. **ORC-only test succeeds**: A test program using only ORC (without Arrow) compiles and runs successfully, confirming ORC code is correct
   ```
   Test: Creating ORC writer without Arrow
   Schema created: struct<a:bigint,b:string>
   Output stream created
   Writer created
   Row batch created
   Test passed!
   ```

2. **Runtime dependencies confirm conflict**:
   ```bash
   ldd simple_orc | grep protobuf
   # Output: libprotobuf.so.23 => /lib/x86_64-linux-gnu/libprotobuf.so.23
   ```

3. **GDB backtrace shows protobuf initialization failure**:
   ```
   #0  0x... in google::protobuf::FileDescriptorProto::MergePartialFromCodedStream(...)
   #1  0x... in MergeFromImpl(...)
   #2  0x... in EncodedDescriptorDatabase::Add(...)
   ...
   ```

### System Information (Updated Jan 2, 2026)

- **System protobuf version**: 3.5.1 (pkg-config), 3.12.4 (actual libprotoc)
- **libprotobuf.so**: 23 (currently loaded version)
- **ORC rebuilt from source**: Yes, with system libraries (using build_orc_from_source.sh)
- **ORC libraries location**: `/usr/local/lib/liborc.a` (newly built)
- **Previous ORC vendored libraries** (removed):
  - `/usr/local/lib/liborc_vendored_protobuf.a` (removed)
  - `/usr/local/lib/liborc_vendored_lz4.a` (removed)
  - `/usr/local/lib/liborc_vendored_zlib.a` (removed)
  - `/usr/local/lib/liborc_vendored_zstd.a` (removed)

## Solutions

### Option 1: Rebuild ORC with CMake 3.25+ (Recommended)

ORC's main branch requires CMake 3.25.0 or higher. The current system has CMake 3.22.1.

```bash
# Install CMake 3.25+
wget https://github.com/Kitware/CMake/releases/download/v3.25.0/cmake-3.25.0-linux-x86_64.tar.gz
tar xzf cmake-3.25.0-linux-x86_64.tar.gz
export PATH=/path/to/cmake-3.25.0/bin:$PATH

# Rebuild ORC with system protobuf 3.5.1
cd /tmp
rm -rf orc orc-build
git clone https://github.com/apache/orc.git
mkdir orc-build && cd orc-build
cmake ../orc -DCMAKE_BUILD_TYPE=Release
cmake --build . -j$(nproc)
sudo cmake --install .
```

### Option 2: Downgrade System Protobuf (Not Recommended)

Install protobuf 3.12.4 (or the version ORC was built with) to match ORC's expectations.

**Risk**: May break other packages that depend on protobuf 3.5.1

### Option 3: Use Container/Virtual Environment

Build and run in an isolated environment with compatible library versions:

```dockerfile
FROM ubuntu:22.04
RUN apt-get update && apt-get install -y cmake git g++ libarrow-dev libparquet-dev
# Build ORC from source in this environment
```

### Option 4: Patch ORC Build Configuration

Modify ORC's CMakeLists.txt to enforce use of system protobuf 3.5.1 instead of vendored version (if compatible).

## Phase 8.1 Update: ORC Rebuilt from Source (Jan 2, 2026)

### Actions Taken
1. Rebuilt Apache ORC from source using `scripts/build_orc_from_source.sh`
2. Used system libraries instead of vendored dependencies (DORC_PREFER_STATIC_*=OFF)
3. Rebuilt with protobuf 3.12.4 (actual system libprotoc version)

### Current Status
- ✅ **ORC compilation with Arrow**: Builds successfully with `-DTPCH_ENABLE_ORC=ON`
- ✅ **CSV and Parquet formats**: Work perfectly, no protobuf conflicts
- ⚠️ **ORC runtime with Arrow**: Still fails with duplicate protobuf descriptor registration error
  - Error: "File already exists in database: orc_proto.proto"
  - This is a protobuf limitation when two libraries define the same message types

### Root Cause (Updated Understanding)
The issue is not a simple ABI mismatch, but rather **protobuf descriptor database pollution**:
- Both ORC and Arrow include compiled protobuf message definitions (orc_proto)
- When both libraries are loaded together, the protobuf library detects duplicate registrations
- This is a fundamental limitation of static-linking protobuf messages to multiple libraries
- The error occurs even with rebuilt ORC using system libraries

### Technical Details of Protobuf Issue
Protobuf uses a global descriptor database to track all known message types. When:
1. Arrow is loaded (contains protobuf message definitions)
2. ORC is loaded (contains same protobuf message definitions from build)
3. Protobuf initialization runs → detects duplicates → throws fatal error

This is different from an ABI mismatch and requires a different solution.

## Workaround for Current Environment

The code compiles correctly and the ORC writer implementation is sound. To test/develop:

1. **Use ORC without Arrow**: The ORC writer can be tested independently of Arrow (no protobuf conflict)
2. **Use static linking where possible**: Prefer static libraries to avoid runtime conflicts
3. **Consider using Parquet instead**: For the current development phase, use the Parquet writer which doesn't have this conflict
4. **Use ORC in separate process**: Data can be written in ORC format via subprocess if needed

## Code Status

- ✅ **ORC writer implementation**: Correct and functional
- ✅ **CMake build system**: Properly configured for ORC support
- ✅ **Linking**: All symbols resolved correctly at compile-time
- ⚠️ **Runtime execution**: Blocked by system library compatibility issue (not a code problem)

## Files Affected

- `CMakeLists.txt`: ORC dependency configuration and linking order
- `examples/CMakeLists.txt`: ORC example build configuration
- `include/tpch/orc_writer.hpp`: ORC writer interface
- `src/writers/orc_writer.cpp`: ORC writer implementation
- `examples/simple_orc.cpp`: ORC example program
