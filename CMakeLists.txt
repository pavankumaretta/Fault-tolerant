cmake_minimum_required(VERSION 3.20)
project(fault_tolerant_sharded_kv_store VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

find_package(Threads REQUIRED)
find_package(SQLite3 REQUIRED)

add_library(kvcore
    src/sqlite_store.cpp
    src/raft.cpp
    src/metrics.cpp
    src/cluster.cpp
    src/gateway.cpp
)

target_include_directories(kvcore PUBLIC include)
target_link_libraries(kvcore PUBLIC SQLite::SQLite3 Threads::Threads)
target_compile_options(kvcore PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)

add_executable(kvstore_server
    src/main.cpp
    src/http_server.cpp
)
target_link_libraries(kvstore_server PRIVATE kvcore Threads::Threads)
target_compile_options(kvstore_server PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)

enable_testing()

add_executable(kvstore_tests tests/unit_tests.cpp)
target_link_libraries(kvstore_tests PRIVATE kvcore)
target_compile_options(kvstore_tests PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME kvstore_tests COMMAND kvstore_tests)

if(ENABLE_SANITIZERS AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    foreach(target kvcore kvstore_server kvstore_tests)
        target_compile_options(${target} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
        target_link_options(${target} PRIVATE -fsanitize=address,undefined)
    endforeach()
endif()
