# Unit tests for TPC-H C++

if(TPCH_BUILD_TESTS)
    # Enable testing
    enable_testing()

    # Find or fetch Google Test
    find_package(GTest QUIET)

    if(NOT GTest_FOUND)
        # Fetch Google Test from GitHub if not found
        include(FetchContent)
        FetchContent_Declare(
            googletest
            URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
        )

        # For Windows: Prevent overriding the parent project's compiler/linker settings
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

        FetchContent_MakeAvailable(googletest)
    endif()

    # Buffer lifetime manager unit tests
    add_executable(buffer_lifetime_manager_test
        buffer_lifetime_manager_test.cpp
    )

    target_link_libraries(buffer_lifetime_manager_test
        PRIVATE
            tpch_core
            GTest::gtest_main
    )

    target_include_directories(buffer_lifetime_manager_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
    )

    # Register tests
    gtest_discover_tests(buffer_lifetime_manager_test)

    message(STATUS "Test targets configured successfully")
endif()
