cmake_minimum_required(VERSION 3.16)
project(NSCFStore VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler flags for performance
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native -mtune=native")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -DDEBUG -fsanitize=thread,undefined")

# Find required packages
find_package(Threads REQUIRED)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party)

# Find gRPC
find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPC REQUIRED grpc++)
pkg_check_modules(PROTOBUF REQUIRED protobuf)

# Find gRPC and protobuf includes and libraries
find_path(GRPC_INCLUDE_DIR grpc/grpc++.h)
find_path(PROTOBUF_INCLUDE_DIR google/protobuf/message.h)

find_library(GRPC_LIBRARY grpc++)
find_library(PROTOBUF_LIBRARY protobuf)

# Find XXHash
find_library(XXHASH_LIBRARY xxhash)

# Generate protobuf files
set(PROTO_FILES
    "proto/context_engine.proto"
)

# Add protobuf compiler
find_program(PROTOC protoc)
if(NOT PROTOC)
    message(FATAL_ERROR "protoc not found")
endif()

# Generate protobuf files only (gRPC service generation is complex)
foreach(proto_file ${PROTO_FILES})
    get_filename_component(proto_name ${proto_file} NAME_WE)
    
    # Generate protobuf files manually
    set(proto_src "${CMAKE_CURRENT_BINARY_DIR}/proto/${proto_name}.pb.cc")
    set(proto_hdr "${CMAKE_CURRENT_BINARY_DIR}/proto/${proto_name}.pb.h")
    
    add_custom_command(
        OUTPUT ${proto_src} ${proto_hdr}
        COMMAND ${PROTOC}
        ARGS --cpp_out=${CMAKE_CURRENT_BINARY_DIR} -I${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/${proto_file}
        DEPENDS ${proto_file}
        COMMENT "Generating ${proto_name} protobuf files"
    )
    
    list(APPEND PROTO_SRCS ${proto_src})
    list(APPEND PROTO_HDRS ${proto_hdr})
endforeach()

# Include generated files
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/proto)

# Source files - gRPC implementation with LSM engine
set(SOURCES
    src/nscfstore/main.cpp
    src/nscfstore/grpc_server.cpp
    src/nscfstore/grpc_client.cpp
    src/nscfstore/lsmstore.cpp
    src/nscfstore/shard.cpp
    src/nscfstore/memory_pool.cpp
    src/nscfstore/relationship.cpp
    src/nscfstore/relationship_manager.cpp
    src/nscfstore/write_path_coordinator.cpp
    src/nscfstore/direct_io_wal.cpp
    src/nscfstore/lockfree_memtable.cpp
    src/nscfstore/wal.cpp
    src/nscfstore/memtable.cpp
    src/nscfstore/network.cpp
    src/nscfstore/relationship_inference.cpp
    src/nscfstore/sstable.cpp
    ${PROTO_SRCS}
)

# Header files
file(GLOB_RECURSE HEADERS
    "include/*.h"
    "include/*.hpp"
)

# Create executable
add_executable(nscfstore ${SOURCES})

# Create static library for linking
add_library(nscfstore_lib STATIC ${SOURCES} ${HEADERS})

# Find gRPC
find_package(PkgConfig REQUIRED grpc++)
if(gRPC_FOUND)
    set(GRPC_LIBRARIES gRPC++::grpc++)
    set(GRPC_INCLUDE_DIRS ${gRPC_INCLUDE_DIRS})
else()
    # Fallback to manual linking
    set(GRPC_LIBRARIES grpc++ grpc)
    set(GRPC_INCLUDE_DIRS)
endif()

# Link libraries for executable
target_link_libraries(nscfstore 
    Threads::Threads
    ${CMAKE_DL_LIBS}
    ${GRPC_LIBRARIES}
    ${PROTOBUF_LIBRARY}
    ${XXHASH_LIBRARY}
    uring
)

# Link libraries for static library
target_link_libraries(nscfstore_lib 
    Threads::Threads
    ${CMAKE_DL_LIBS}
    ${GRPC_LIBRARIES}
    ${PROTOBUF_LIBRARY}
    ${XXHASH_LIBRARY}
    uring
)

# Platform-specific libraries
if(UNIX AND NOT APPLE)
    target_link_libraries(nscfstore pthread rt)
endif()

# Install targets
install(TARGETS nscfstore DESTINATION bin)
install(FILES ${HEADERS} DESTINATION include)

# Testing
enable_testing()
# add_subdirectory(tests)

# Benchmarks  
# add_subdirectory(benchmarks)
