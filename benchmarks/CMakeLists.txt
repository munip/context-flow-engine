cmake_minimum_required(VERSION 3.20)

# Benchmark source files
set(BENCHMARK_SOURCES
    benchmark_write_path.cpp
)

# Create benchmark executable
add_executable(lsmstore_benchmarks ${BENCHMARK_SOURCES})

# Link libraries
target_link_libraries(lsmstore_benchmarks 
    lsmstore_lib
    pthread
    dl
    ${XXHASH_LIBRARY}
)

# Include directories
target_include_directories(lsmstore_benchmarks PRIVATE 
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/third_party
)

# Compiler flags for benchmarks
target_compile_options(lsmstore_benchmarks PRIVATE 
    -O3 
    -DNDEBUG 
    -march=native 
    -mtune=native
)
