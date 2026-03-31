cmake_minimum_required(VERSION 3.20)

# Find Google Test
find_package(GTest REQUIRED)

# Test source files
set(TEST_SOURCES
    test_write_path.cpp
    test_relationship.cpp
)

# Create test executable
add_executable(lsmstore_tests ${TEST_SOURCES})

# Link libraries
target_link_libraries(lsmstore_tests 
    lsmstore_lib
    gtest
    gtest_main
    pthread
    ${XXHASH_LIBRARY}
)

# Include directories
target_include_directories(lsmstore_tests PRIVATE 
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/third_party
)

# Enable testing
enable_testing()

# Add tests
add_test(NAME AllTests COMMAND lsmstore_tests)

# Individual tests
add_test(NAME MemoryPoolTest COMMAND lsmstore_tests --gtest_filter=MemoryPool*)
add_test(NAME MemtableTest COMMAND lsmstore_tests --gtest_filter=Memtable*)
add_test(NAME WALTest COMMAND lsmstore_tests --gtest_filter=WAL*)
add_test(NAME SSTableTest COMMAND lsmstore_tests --gtest_filter=SSTable*)
add_test(NAME ShardTest COMMAND lsmstore_tests --gtest_filter=Shard*)
add_test(NAME IntegrationTest COMMAND lsmstore_tests --gtest_filter=Integration*)
