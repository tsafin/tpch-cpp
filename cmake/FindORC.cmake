# Find Apache ORC
#
# This module tries to find ORC's include directory and libraries.
#
# Once done this will define:
#  ORC_FOUND         - System has ORC library
#  ORC_INCLUDE_DIR   - The ORC include directory
#  ORC_LIBRARY       - Location of the ORC library
#  ORC::orc          - Imported library target for ORC
#

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(PC_ORC QUIET orc)
endif()

find_path(ORC_INCLUDE_DIR NAMES orc/OrcFile.hh
    HINTS ${PC_ORC_INCLUDEDIR} ${PC_ORC_INCLUDE_DIRS}
    PATH_SUFFIXES include
    HINTS /usr/local /usr
)

find_library(ORC_LIBRARY NAMES orc
    HINTS ${PC_ORC_LIBDIR} ${PC_ORC_LIBRARY_DIRS}
    PATH_SUFFIXES lib lib64
    HINTS /usr/local /usr
)

# Handle version
if(ORC_INCLUDE_DIR)
    set(_orc_version_file "${ORC_INCLUDE_DIR}/orc/OrcFile.hh")
    if(EXISTS "${_orc_version_file}")
        file(READ "${_orc_version_file}" _orc_version_content)
        # Try to extract version from header
        if(_orc_version_content MATCHES "LIBORC_VERSION_MAJOR ([0-9]+)")
            set(ORC_VERSION_MAJOR "${CMAKE_MATCH_1}")
        endif()
    endif()
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ORC
    REQUIRED_VARS ORC_INCLUDE_DIR ORC_LIBRARY
    VERSION_VAR ORC_VERSION_MAJOR
)

if(ORC_FOUND)
    if(NOT TARGET ORC::orc)
        add_library(ORC::orc UNKNOWN IMPORTED)
        set_target_properties(ORC::orc PROPERTIES
            IMPORTED_LOCATION "${ORC_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${ORC_INCLUDE_DIR}"
        )
    endif()
endif()

mark_as_advanced(ORC_INCLUDE_DIR ORC_LIBRARY)
