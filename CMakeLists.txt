cmake_minimum_required(VERSION 3.28)

project(StrataDB LANGUAGES CXX)

# ------------------ Global Config ------------------
set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ------------------ Linker ------------------
add_link_options(-fuse-ld=mold)

# ------------------ Options ------------------
option(STRATADB_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(STRATADB_ENABLE_UBSAN "Enable UBSan" OFF)
option(STRATADB_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(STRATADB_ENABLE_LTO "Enable LTO" OFF)

# ------------------ Strict Warnings ------------------
add_library(stratadb_strict_warnings INTERFACE)

target_compile_options(stratadb_strict_warnings INTERFACE
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

# ------------------ Core Library ------------------
add_library(stratadb STATIC
  src/memory/epoch_manager.cpp
)

target_include_directories(stratadb
  PUBLIC include
  PRIVATE src
)

target_link_libraries(stratadb PRIVATE stratadb_strict_warnings)

target_compile_features(stratadb PUBLIC cxx_std_26)

# ------------------ Sanitizers (Library) ------------------
if(STRATADB_ENABLE_ASAN)
  target_compile_options(stratadb PRIVATE -fsanitize=address)
  target_link_options(stratadb PRIVATE -fsanitize=address)
endif()

if(STRATADB_ENABLE_UBSAN)
  target_compile_options(stratadb PRIVATE -fsanitize=undefined)
  target_link_options(stratadb PRIVATE -fsanitize=undefined)
endif()

if(STRATADB_ENABLE_TSAN)
  target_compile_options(stratadb PRIVATE -fsanitize=thread)
  target_link_options(stratadb PRIVATE -fsanitize=thread)
endif()

# ------------------ LTO ------------------
if(STRATADB_ENABLE_LTO)
  set_property(TARGET stratadb PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# ==========================================================
# ======================= TESTING ===========================
# ==========================================================

enable_testing()

include(FetchContent)

# Avoid installing gtest
set(INSTALL_GTEST OFF)

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/releases/download/v1.17.0/googletest-1.17.0.tar.gz
)

# Prevent CRT override (important on some platforms)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(googletest)

# ------------------ Test Executable ------------------
add_executable(epoch_manager_tests
  tests/memory/epoch_manager_test.cpp
)

target_link_libraries(epoch_manager_tests
  PRIVATE
    stratadb
    GTest::gtest_main
    stratadb_strict_warnings
)

target_compile_features(epoch_manager_tests PUBLIC cxx_std_26)

# ------------------ Sanitizers (Tests) ------------------
if(STRATADB_ENABLE_ASAN)
  target_compile_options(epoch_manager_tests PRIVATE -fsanitize=address)
  target_link_options(epoch_manager_tests PRIVATE -fsanitize=address)
endif()

if(STRATADB_ENABLE_UBSAN)
  target_compile_options(epoch_manager_tests PRIVATE -fsanitize=undefined)
  target_link_options(epoch_manager_tests PRIVATE -fsanitize=undefined)
endif()

if(STRATADB_ENABLE_TSAN)
  target_compile_options(epoch_manager_tests PRIVATE -fsanitize=thread)
  target_link_options(epoch_manager_tests PRIVATE -fsanitize=thread)
endif()

# ------------------ Register Tests ------------------
include(GoogleTest)
gtest_discover_tests(epoch_manager_tests)