# TPC-H dbgen integration
# Wraps the dbgen Makefile-based build system with CMake

# Check if dbgen source directory exists
# The dbgen source is in third_party/tpch/dbgen (from the git submodule)
set(DBGEN_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../tpch/dbgen")
if(NOT EXISTS "${DBGEN_SOURCE_DIR}")
    message(FATAL_ERROR "dbgen source not found at ${DBGEN_SOURCE_DIR}. Make sure git submodule is initialized: git submodule update --init --recursive")
endif()

# Custom build target for dbgen using its Makefile
# dbgen uses makefile.suite which needs to be copied to Makefile
# We compile just OBJ1 (dbgen executable objects) to avoid query generation
# Note: MACHINE, DATABASE, WORKLOAD are used in CFLAGS preprocessing
add_custom_target(dbgen_build ALL
    COMMAND cp makefile.suite Makefile
    # Build individual object files needed for data generation
    # These are the core TPC-H data generation objects (excluding driver.o which has main())
    COMMAND env -u CFLAGS -u CXXFLAGS ${CMAKE_MAKE_PROGRAM}
        CC=${CMAKE_C_COMPILER}
        MACHINE=LINUX
        DATABASE=PGSQL
        WORKLOAD=TPCH
        build.o bm_utils.o rnd.o print.o load_stub.o bcd2.o speed_seed.o text.o permute.o rng64.o
    # Create library from object files (excluding driver which has main())
    COMMAND ${CMAKE_AR} r libdbgen.a build.o bm_utils.o rnd.o print.o load_stub.o bcd2.o speed_seed.o text.o permute.o rng64.o
    WORKING_DIRECTORY "${DBGEN_SOURCE_DIR}"
    COMMENT "Building TPC-H dbgen library (data generation objects only)"
)

# Create imported library target
add_library(dbgen STATIC IMPORTED GLOBAL)
set_target_properties(dbgen PROPERTIES
    IMPORTED_LOCATION "${DBGEN_SOURCE_DIR}/libdbgen.a"
    INTERFACE_INCLUDE_DIRECTORIES "${DBGEN_SOURCE_DIR}"
)
# Make sure the library is built before anything uses it
add_dependencies(dbgen dbgen_build)

# Also create an object library that explicitly depends on dbgen_build
# This ensures targets linking dbgen will wait for the build
add_library(dbgen_obj OBJECT IMPORTED GLOBAL)
add_dependencies(dbgen_obj dbgen_build)

message(STATUS "dbgen configured with source: ${DBGEN_SOURCE_DIR}")
