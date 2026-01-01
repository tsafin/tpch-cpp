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

    # simple_orc - Demonstrates ORC writing (if ORC is available)
    if(ORC_FOUND)
        add_executable(simple_orc simple_orc.cpp)
        # tpch_core already includes snappy via its dependency on arrow
        # We just need to link in the right order
        target_link_libraries(simple_orc PRIVATE
            tpch_core
            Arrow::arrow
            ORC::orc
        )

        find_package(Protobuf REQUIRED)
        if(TARGET protobuf::libprotobuf)
            target_link_libraries(simple_orc PRIVATE protobuf::libprotobuf)
        elseif(TARGET Protobuf::protobuf)
            target_link_libraries(simple_orc PRIVATE Protobuf::protobuf)
        else()
            target_link_libraries(simple_orc PRIVATE ${Protobuf_LIBRARIES})
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
endif()
