# Docker Images for TPC-H C++ CI

This directory contains Dockerfiles for pre-built CI environments to speed up GitHub Actions workflows.

## Images

### Base Image (`tpch-cpp-base:latest`)
- **Base OS**: Ubuntu 22.04
- **Includes**:
  - Build tools (cmake, gcc, g++, git, pkg-config)
  - System dependencies (liburing, compression libraries, etc.)
  - **Pre-compiled Arrow + Parquet** libraries (installed to `/opt/dependencies`)
- **Size**: ~800-1000 MB
- **Saves**: ~5-10 minutes per build

### ORC Image (`tpch-cpp-orc:latest`)
- **Extends**: Base image
- **Includes**: 
  - Everything from base image
  - **Pre-compiled ORC** library (static, installed to `/opt/dependencies`)
- **Size**: ~1-1.2 GB
- **Saves**: ~10-15 minutes per build

### Lance Image (`tpch-cpp-lance:latest`)
- **Extends**: Base image
- **Includes**:
  - Everything from base image
  - Rust toolchain 1.93.0
  - Mold linker
  - **Pre-compiled Lance FFI** library (`liblance_ffi.a` in `/opt/dependencies/lib`)
- **Size**: ~1.5-2 GB
- **Saves**: ~10-15 minutes per build

## Usage in CI

Images are automatically pulled in the CI workflow:

```yaml
container:
  image: ghcr.io/tsafin/tpch-cpp-base:latest
```

## Building Images

### Automatic (Recommended)
Images are automatically built and pushed when:
- Dockerfiles or build scripts change
- Submodules are updated
- Manually triggered via workflow dispatch

### Manual Build
```bash
# Build base image
docker build -f .docker/Dockerfile.base -t ghcr.io/tsafin/tpch-cpp-base:latest .

# Build ORC image (after base)
docker build -f .docker/Dockerfile.orc -t ghcr.io/tsafin/tpch-cpp-orc:latest .

# Build Lance image (after base)
docker build -f .docker/Dockerfile.lance -t ghcr.io/tsafin/tpch-cpp-lance:latest .
```

## Registry

Images are stored in GitHub Container Registry (ghcr.io):
- Base: `ghcr.io/tsafin/tpch-cpp-base:latest`
- ORC: `ghcr.io/tsafin/tpch-cpp-orc:latest`
- Lance: `ghcr.io/tsafin/tpch-cpp-lance:latest`

Public repositories can pull these images for free.

## Performance Impact

Using these images reduces CI build times:
- **Before**: 30-45 minutes (compile Arrow/ORC/Rust from scratch)
- **After**: 10-15 minutes (only compile project code)
- **Savings**: ~20-30 minutes per workflow run

## Cache Strategy

Instead of caching build artifacts in GitHub Actions cache (10GB limit), we:
1. Pre-compile dependencies in Docker images
2. Store images in Container Registry (unlimited for public repos)
3. Pull image once at workflow start
4. Only compile project-specific code

This is more efficient than artifact caching because:
- Images are shared across all workflow runs
- No cache restore overhead
- More consistent build environment
- Better cache utilization

## Maintenance

Images should be rebuilt when:
- Arrow/ORC/Lance versions change
- System dependencies are updated
- Rust version changes
- Build configuration changes

The workflow automatically rebuilds on relevant file changes.
