cmake_minimum_required(VERSION 3.20)

# Find required packages
find_package(gRPC REQUIRED)
find_package(Protobuf REQUIRED)

# Generate protobuf files
set(PROTO_FILES
    "../proto/context_engine.proto"
)

foreach(proto_file ${PROTO_FILES})
    get_filename_component(proto_name ${proto_file} NAME_WE)
    protobuf_generate_cpp(
        ${proto_name}_PROTO_SRCS
        ${proto_name}_PROTO_HDRS
        ${proto_file}
        PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated
    )
endforeach()

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/generated)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../proto)

# Quick start example
add_executable(quick_start
    quick_start.cpp
    ${context_engine_PROTO_SRCS}
)

target_link_libraries(quick_start
    gRPC::grpc++
    protobuf::libprotobuf
    xxHash::xxhash
    pthread
)

# Performance test example
add_executable(performance_test
    performance_test.cpp
    ${context_engine_PROTO_SRCS}
)

target_link_libraries(performance_test
    gRPC::grpc++
    protobuf::libprotobuf
    xxHash::xxhash
    pthread
)

# Client benchmark
add_executable(client_benchmark
    client_benchmark.cpp
    ${context_engine_PROTO_SRCS}
)

target_link_libraries(client_benchmark
    gRPC::grpc++
    protobuf::libprotobuf
    xxHash::xxhash
    pthread
)
