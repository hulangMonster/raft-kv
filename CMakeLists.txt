cmake_minimum_required(VERSION 3.16)
project(raftkv VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

add_compile_options(-Wall -Wextra)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

find_package(Threads REQUIRED)

add_library(raftkv_core STATIC
  src/codec.cpp
  src/wal.cpp
  src/store.cpp
  src/server.cpp
)
target_include_directories(raftkv_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(raftkv_core PUBLIC Threads::Threads)

add_executable(raftkv_server src/main_server.cpp)
target_link_libraries(raftkv_server PRIVATE raftkv_core)

add_executable(raftkv_cli src/main_client.cpp)
target_link_libraries(raftkv_cli PRIVATE raftkv_core)

enable_testing()
add_executable(raftkv_tests
  tests/test_main.cpp
  tests/test_store.cpp
  tests/test_codec.cpp
)
target_link_libraries(raftkv_tests PRIVATE raftkv_core)
add_test(NAME raftkv_unit COMMAND raftkv_tests)
