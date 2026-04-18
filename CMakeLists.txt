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

# ------------------ Strict Warnings ------------------
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
)

# Compiler-specific flags
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(stratadb_warnings INTERFACE
    -Wno-interference-size
  )
endif()

# ------------------ Sanitizer Helper ------------------
function(enable_sanitizers target)
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

# ------------------ Core Library ------------------
add_library(stratadb STATIC
  src/memory/epoch_manager.cpp
  src/config/config_manager.cpp
)

target_include_directories(stratadb
  PUBLIC include
  PRIVATE src
)

target_link_options(stratadb PRIVATE -fuse-ld=mold)

target_link_libraries(stratadb PRIVATE stratadb_warnings)

target_compile_features(stratadb PUBLIC cxx_std_26)

enable_sanitizers(stratadb)

# ------------------ LTO ------------------
if(STRATADB_ENABLE_LTO)
  set_property(TARGET stratadb PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# ======================= TESTING ===========================

enable_testing()

include(FetchContent)

set(INSTALL_GTEST OFF)

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/releases/download/v1.17.0/googletest-1.17.0.tar.gz
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(googletest)

# ------------------ Test Executable ------------------
add_executable(stratadb_tests
  tests/memory/epoch_manager_test.cpp
  tests/config/config_manager_test.cpp
)

target_link_libraries(stratadb_tests
  PRIVATE
    stratadb
    GTest::gtest_main
)

target_link_options(stratadb_tests PRIVATE -fuse-ld=mold)

target_compile_features(stratadb_tests PUBLIC cxx_std_26)

# Apply strict warnings ONLY to your test code (not gtest)
target_compile_options(stratadb_tests PRIVATE
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
)

# Compiler-specific for tests
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(stratadb_tests PRIVATE
    -Wno-interference-size
  )
endif()

enable_sanitizers(stratadb_tests)

# ------------------ Register Tests ------------------
include(GoogleTest)
gtest_discover_tests(stratadb_tests)