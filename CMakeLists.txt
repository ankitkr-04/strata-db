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

if(STRATADB_USE_MOLD)
  find_program(STRATADB_MOLD_LINKER mold)
  if(NOT STRATADB_MOLD_LINKER)
    message(STATUS "mold linker requested but not found; using the compiler default linker")
  endif()
endif()

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

# ------------------ Sanitizer Helper ------------------
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
)

target_include_directories(stratadb
  PUBLIC include
  PRIVATE src
)

enable_mold_linker(stratadb)

target_link_libraries(stratadb PRIVATE numa)

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
  tests/memory/arena_test.cpp
  tests/memory/tlab_test.cpp
)

target_link_libraries(stratadb_tests
  PRIVATE
    stratadb
    GTest::gtest_main
)

enable_mold_linker(stratadb_tests)

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

enable_sanitizers(stratadb_tests)

# ------------------ Register Tests ------------------
include(GoogleTest)
gtest_discover_tests(stratadb_tests)
