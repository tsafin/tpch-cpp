# Example programs demonstrating TPC-H functionality

if(TPCH_BUILD_EXAMPLES)
    # simple_arrow_parquet - Demonstrates Arrow schema and Parquet writing
    add_executable(simple_arrow_parquet simple_arrow_parquet.cpp)
    target_link_libraries(simple_arrow_parquet PRIVATE
        Arrow::arrow
        Arrow::parquet
    )

    # simple_csv - Demonstrates CSV writing
    add_executable(simple_csv simple_csv.cpp)
    target_link_libraries(simple_csv PRIVATE
        tpch_core
        Arrow::arrow
    )

    # simple_orc - Demonstrates ORC writing using ONLY ORC's native API
    # (does NOT use Arrow to avoid protobuf descriptor conflicts)
    # Example shows that ORC uses its own built-in protobuf without needing external dependencies
    if(TPCH_ENABLE_ORC AND ORC_FOUND)
        add_executable(simple_orc simple_orc.cpp)
        # Link only ORC and compression libraries - NO Arrow/Parquet
        target_link_libraries(simple_orc PRIVATE
            ORC::orc
            z snappy lz4 zstd
        )

        # Link protobuf for ORC (required since ORC is a static library built with protobuf)
        find_package(Protobuf QUIET)
        if(Protobuf_FOUND)
            if(TARGET protobuf::libprotobuf)
                target_link_libraries(simple_orc PRIVATE protobuf::libprotobuf)
            elseif(TARGET Protobuf::protobuf)
                target_link_libraries(simple_orc PRIVATE Protobuf::protobuf)
            elseif(Protobuf_LIBRARIES)
                target_link_libraries(simple_orc PRIVATE ${Protobuf_LIBRARIES})
            endif()
        endif()
    endif()

    # async_io_demo - Demonstrates async I/O with io_uring (if enabled)
    if(TPCH_ENABLE_ASYNC_IO AND Uring_FOUND)
        add_executable(async_io_demo async_io_demo.cpp)
        target_link_libraries(async_io_demo PRIVATE
            tpch_core
            Uring::uring
        )
    else()
        # Build stub version when async I/O not available
        add_executable(async_io_demo async_io_demo.cpp)
        target_link_libraries(async_io_demo PRIVATE
            tpch_core
        )
    endif()

    # multi_table_benchmark - Demonstrates multi-file async I/O (Phase 12.5)
    if(TPCH_ENABLE_ASYNC_IO AND Uring_FOUND)
        add_executable(multi_table_benchmark multi_table_benchmark.cpp)
        target_link_libraries(multi_table_benchmark PRIVATE
            tpch_core
            Uring::uring
        )
    else()
        # Build stub version when async I/O not available
        add_executable(multi_table_benchmark multi_table_benchmark.cpp)
        target_link_libraries(multi_table_benchmark PRIVATE
            tpch_core
        )
    endif()
endif()
