# Compiler warning and optimization flags for TPC-H C++

# Base warning flags for both GCC and Clang
set(WARNING_FLAGS
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
    -Wshadow
    -Wunused
    -Wredundant-decls
    -Wdisabled-optimization
    -Wpointer-arith
    -Wcast-qual
    -Wwrite-strings
    -Wformat=2
    -Wlogical-op
    -Wnon-virtual-dtor
    -Wold-style-cast
    -Wopenmp-simd
    -Woverloaded-virtual
    -Wpacked
    -Wparentheses
    -Wswitch-default
    -Wswitch-enum
    -Wundef
    -Wunused-function
    -Wunused-label
    -Wunused-parameter
    -Wunused-variable
    -Wvarargs
    -Wvariadic-macros
)

# Suppress some warnings that are overly strict for this project
set(WARNING_SUPPRESSIONS
    -Wno-unknown-pragmas      # Allow pragmas for architecture-specific code
    -Wno-missing-field-initializers  # Arrow types may have optional fields
)

# Apply warnings
add_compile_options(${WARNING_FLAGS} ${WARNING_SUPPRESSIONS})

# GCC-specific flags
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_compile_options(
        -Wduplicated-branches
        -Wduplicated-cond
        -Wnull-dereference
        -Wuseless-cast
        -Wstrict-aliasing=2
    )
endif()

# Clang-specific flags
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    add_compile_options(
        -Wsometimes-uninitialized
        -Wunreachable-code
        -Wself-assign
    )
endif()

# Optimization flags are handled in the root CMakeLists.txt based on CMAKE_BUILD_TYPE
