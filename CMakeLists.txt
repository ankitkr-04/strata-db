cmake_minimum_required(VERSION 3.28)

project(StrataDB LANGUAGES CXX)

# ------------------ Global Config ------------------
set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ------------------ Options ------------------
option(STRATADB_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(STRATADB_ENABLE_UBSAN "Enable UBSan" OFF)
option(STRATADB_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(STRATADB_ENABLE_LTO "Enable LTO" OFF)
option(STRATADB_USE_MOLD "Use the mold linker when it is installed" ON)
option(STRATADB_BUILD_BENCHMARKS "Build Google Benchmark executables" ON)

# Architectural / Hardware Sympathy Options
option(STRATADB_DISABLE_EXCEPTIONS "Disable C++ exceptions for embedded builds" OFF)
option(STRATADB_DISABLE_RTTI "Disable RTTI for embedded builds" OFF)
option(STRATADB_ENABLE_PROFILING "Retain frame pointers for perf/flamegraph profiling" OFF)

if(STRATADB_USE_MOLD)
  find_program(STRATADB_MOLD_LINKER mold)
  if(NOT STRATADB_MOLD_LINKER)
    message(STATUS "mold linker requested but not found; using default")
  endif()
endif()

# ------------------ Strict Warnings & Profiles ------------------
add_library(stratadb_warnings INTERFACE)

target_compile_options(stratadb_warnings INTERFACE
  -Wall
  -Wextra
  -Wpedantic
  -Wconversion
  -Wsign-conversion
  -Wshadow
  -Wnon-virtual-dtor
  -Wold-style-cast
  -Wcast-align
  -Werror
  -Wno-interference-size
)

# Apply compiler profiles to the interface
if(STRATADB_DISABLE_EXCEPTIONS)
  target_compile_options(stratadb_warnings INTERFACE -fno-exceptions)
  target_compile_definitions(stratadb_warnings INTERFACE STRATADB_NO_EXCEPTIONS)
endif()

if(STRATADB_DISABLE_RTTI)
  target_compile_options(stratadb_warnings INTERFACE -fno-rtti)
endif()

if(STRATADB_ENABLE_PROFILING)
  target_compile_options(stratadb_warnings INTERFACE -fno-omit-frame-pointer -g)
  message(STATUS "StrataDB: Profiling mode enabled (frame pointers retained)")
endif()

# ------------------ Sanitizer & Linker Helpers ------------------
function(enable_sanitizers target)
  if(STRATADB_ENABLE_ASAN AND STRATADB_ENABLE_TSAN)
    message(FATAL_ERROR "ASAN and TSAN cannot be enabled together")
  endif()
  if(STRATADB_ENABLE_ASAN)
    target_compile_options(${target} PRIVATE -fsanitize=address)
    target_link_options(${target} PRIVATE -fsanitize=address)
  endif()
  if(STRATADB_ENABLE_UBSAN)
    target_compile_options(${target} PRIVATE -fsanitize=undefined)
    target_link_options(${target} PRIVATE -fsanitize=undefined)
  endif()
  if(STRATADB_ENABLE_TSAN)
    target_compile_options(${target} PRIVATE -fsanitize=thread)
    target_link_options(${target} PRIVATE -fsanitize=thread)
  endif()
endfunction()

function(enable_mold_linker target)
  if(STRATADB_USE_MOLD AND STRATADB_MOLD_LINKER)
    target_link_options(${target} PRIVATE -fuse-ld=mold)
  endif()
endfunction()

# ------------------ Core Library ------------------
add_library(stratadb STATIC
  src/memory/epoch_manager.cpp
  src/config/config_manager.cpp
  src/memory/arena.cpp
  src/memory/tlab.cpp
  src/memtable/skiplist_node.cpp
  src/memtable/skiplist_memtable.cpp
  src/utils/hardware.cpp
  src/utils/os.cpp
  src/memory/block_pool.cpp
)

target_include_directories(stratadb PUBLIC include PRIVATE src)
target_link_libraries(stratadb PRIVATE numa stratadb_warnings)
target_compile_features(stratadb PUBLIC cxx_std_26)

enable_mold_linker(stratadb)
enable_sanitizers(stratadb)

if(STRATADB_ENABLE_LTO)
  set_property(TARGET stratadb PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# ======================= TESTING ===========================
include(FetchContent)
enable_testing()
set(INSTALL_GTEST OFF)

fetchcontent_declare(
  googletest
  URL https://github.com/google/googletest/releases/download/v1.17.0/googletest-1.17.0.tar.gz
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
fetchcontent_makeavailable(googletest)

add_executable(stratadb_tests
  tests/memory/epoch_manager_test.cpp
  tests/config/config_manager_test.cpp
  tests/memory/arena_test.cpp
  tests/memory/tlab_test.cpp
  tests/memtable/skiplist_memtable_test.cpp
)

target_link_libraries(stratadb_tests PRIVATE stratadb stratadb_warnings GTest::gtest_main)
target_compile_features(stratadb_tests PUBLIC cxx_std_26)

enable_mold_linker(stratadb_tests)
enable_sanitizers(stratadb_tests)

include(GoogleTest)
gtest_discover_tests(stratadb_tests)

# ===================== BENCHMARKING =======================
if(STRATADB_BUILD_BENCHMARKS)
  add_library(stratadb_benchmark_flags INTERFACE)
  if(CMAKE_BUILD_TYPE STREQUAL "Release" AND CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(stratadb_benchmark_flags INTERFACE -O3 -march=native)
  endif()

  fetchcontent_declare(
    benchmark
    URL https://github.com/google/benchmark/archive/refs/tags/v1.9.5.tar.gz
  )
  set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
  fetchcontent_makeavailable(benchmark)

  macro(add_stratadb_benchmark name source_file)
    add_executable(${name} ${source_file})
    target_link_libraries(${name} PRIVATE
      stratadb
      stratadb_warnings
      stratadb_benchmark_flags
      benchmark::benchmark
      benchmark::benchmark_main
    )
    target_compile_features(${name} PUBLIC cxx_std_26)
    enable_mold_linker(${name})
    enable_sanitizers(${name})
  endmacro()

  add_stratadb_benchmark(allocator_bench benchmarks/allocator_bench.cpp)
  add_stratadb_benchmark(tlab_bench benchmarks/tlab_bench.cpp)
  add_stratadb_benchmark(page_bench benchmarks/page_bench.cpp)
  add_stratadb_benchmark(memtable_bench benchmarks/memtable_bench.cpp)
endif()
