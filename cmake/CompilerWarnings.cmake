# Compiler warning and optimization flags for TPC-H C++

# Common warning flags for both C and C++
set(COMMON_WARNING_FLAGS
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
    -Wopenmp-simd
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

# C++-only warning flags
set(CXX_ONLY_WARNING_FLAGS
    -Wnon-virtual-dtor
    -Wold-style-cast
    -Woverloaded-virtual
)

# Suppress some warnings that are overly strict for this project
set(WARNING_SUPPRESSIONS
    -Wno-unknown-pragmas      # Allow pragmas for architecture-specific code
    -Wno-missing-field-initializers  # Arrow types may have optional fields
)

# Apply common warnings to all languages
add_compile_options(${COMMON_WARNING_FLAGS} ${WARNING_SUPPRESSIONS})

# Apply C++-specific warnings only to C++
add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wnon-virtual-dtor>)
add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wold-style-cast>)
add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Woverloaded-virtual>)

# GCC-specific flags
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_compile_options(
        -Wduplicated-branches
        -Wduplicated-cond
        -Wnull-dereference
    )
    # GCC-specific C++-only flags
    add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wuseless-cast>)
    add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wstrict-aliasing=2>)
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
