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

    # DBGen batch iterator tests
    add_executable(dbgen_batch_iterator_test
        dbgen_batch_iterator_test.cpp
    )

    target_link_libraries(dbgen_batch_iterator_test
        PRIVATE
            tpch_core
            GTest::gtest_main
    )

    target_include_directories(dbgen_batch_iterator_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
    )

    gtest_discover_tests(dbgen_batch_iterator_test)

    # Paimon writer tests (only if Paimon is enabled)
    if(TPCH_ENABLE_PAIMON)
        add_executable(paimon_writer_test
            paimon_writer_test.cpp
        )

        target_link_libraries(paimon_writer_test
            PRIVATE
                tpch_core
                GTest::gtest_main
        )

        target_include_directories(paimon_writer_test
            PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/../include
        )

        gtest_discover_tests(paimon_writer_test)
    endif()

    message(STATUS "Test targets configured successfully")
endif()
