# Find Apache Arrow C++
#
# This module tries to find Arrow's include directory and libraries.
#
# Once done this will define:
#  Arrow_FOUND       - System has Arrow C++ library
#  Arrow_INCLUDE_DIR - The Arrow include directory
#  Arrow_LIBRARY     - Location of the Arrow library (if using SHARED)
#  Arrow::arrow      - Imported library target for Arrow
#  Parquet::parquet  - Imported library target for Parquet
#

find_path(Arrow_INCLUDE_DIR
    NAMES arrow/api.h
    PATH_SUFFIXES include
    HINTS
        ${ARROW_ROOT}
        $ENV{ARROW_ROOT}
        /usr
        /usr/local
        /opt/arrow
)

find_library(Arrow_LIBRARY
    NAMES arrow arrow_shared
    PATH_SUFFIXES lib lib64
    HINTS
        ${ARROW_ROOT}
        $ENV{ARROW_ROOT}
        /usr
        /usr/local
        /opt/arrow
)

find_library(Parquet_LIBRARY
    NAMES parquet parquet_shared
    PATH_SUFFIXES lib lib64
    HINTS
        ${ARROW_ROOT}
        $ENV{ARROW_ROOT}
        /usr
        /usr/local
        /opt/arrow
)

# Handle version
if(Arrow_INCLUDE_DIR)
    set(_arrow_version_file "${Arrow_INCLUDE_DIR}/arrow/util/macros.h")
    if(EXISTS "${_arrow_version_file}")
        file(READ "${_arrow_version_file}" _arrow_version_content)
        if(_arrow_version_content MATCHES "ARROW_VERSION_MAJOR ([0-9]+)")
            set(Arrow_VERSION_MAJOR "${CMAKE_MATCH_1}")
        endif()
    endif()
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Arrow
    FOUND_VAR Arrow_FOUND
    REQUIRED_VARS Arrow_INCLUDE_DIR Arrow_LIBRARY Parquet_LIBRARY
    VERSION_VAR Arrow_VERSION_MAJOR
)

if(Arrow_FOUND)
    if(NOT TARGET Arrow::arrow)
        add_library(Arrow::arrow UNKNOWN IMPORTED)
        set_target_properties(Arrow::arrow PROPERTIES
            IMPORTED_LOCATION "${Arrow_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${Arrow_INCLUDE_DIR}"
        )
    endif()

    if(NOT TARGET Arrow::parquet)
        add_library(Arrow::parquet UNKNOWN IMPORTED)
        set_target_properties(Arrow::parquet PROPERTIES
            IMPORTED_LOCATION "${Parquet_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${Arrow_INCLUDE_DIR}"
            INTERFACE_LINK_LIBRARIES "Arrow::arrow"
        )
    endif()
endif()

mark_as_advanced(Arrow_INCLUDE_DIR Arrow_LIBRARY Parquet_LIBRARY)
