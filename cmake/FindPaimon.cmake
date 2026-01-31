# Find Apache Paimon C++ library
# Supports pkg-config discovery and fallback path-based search

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(PC_PAIMON QUIET paimon)
endif()

find_path(PAIMON_INCLUDE_DIR
    NAMES paimon/write_context.h
    HINTS ${PC_PAIMON_INCLUDEDIR} ${PC_PAIMON_INCLUDE_DIRS}
    PATH_SUFFIXES include
)

find_library(PAIMON_LIBRARY
    NAMES paimon
    HINTS ${PC_PAIMON_LIBDIR} ${PC_PAIMON_LIBRARY_DIRS}
    PATH_SUFFIXES lib lib64
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Paimon
    REQUIRED_VARS PAIMON_INCLUDE_DIR PAIMON_LIBRARY
)

if(Paimon_FOUND)
    if(NOT TARGET Paimon::paimon)
        add_library(Paimon::paimon UNKNOWN IMPORTED)
        set_target_properties(Paimon::paimon PROPERTIES
            IMPORTED_LOCATION "${PAIMON_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${PAIMON_INCLUDE_DIR}"
        )
    endif()
endif()

mark_as_advanced(PAIMON_INCLUDE_DIR PAIMON_LIBRARY)
