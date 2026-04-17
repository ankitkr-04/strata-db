# Build and Toolchain Architecture

Author: Ankit Kumar
Date: 2026-04-17

## Purpose
Describe the active build and test toolchain in this repository so contributors can build, run tests, and diagnose performance with the same assumptions.

## Overview
StrataDB uses CMake with C++26, strict compiler warnings, optional sanitizers, optional LTO, and GoogleTest-based unit tests. The build produces a static library target (`stratadb`) and one test executable target (`stratadb_tests`).

## System Context
- Primary build file: `CMakeLists.txt`
- Library implementation files used by the build:
	- `src/memory/epoch_manager.cpp`
	- `src/config/config_manager.cpp`
- Test files used by the build:
	- `tests/memory/epoch_manager_test.cpp`
	- `tests/config/config_manager_test.cpp`
- Attached profiling artifact: `real_perf.txt`

## Components and Responsibilities

### Build Configuration
- Minimum CMake version: `3.28`
- Language: `CXX`
- Standard: `C++26` (`CMAKE_CXX_STANDARD 26`, required, no compiler extensions)
- Compilation database export is enabled (`CMAKE_EXPORT_COMPILE_COMMANDS ON`).

### Warning Policy
An interface target named `stratadb_warnings` centralizes warning flags and is linked into both production and test targets.

Configured warnings include:
- `-Wall`
- `-Wextra`
- `-Wpedantic`
- `-Wconversion`
- `-Wsign-conversion`
- `-Wshadow`
- `-Wnon-virtual-dtor`
- `-Wold-style-cast`
- `-Wcast-align`
- `-Werror`
- `-Wno-interference-size`

### Library Target
- Target: `stratadb` (`STATIC`)
- Sources:
	- `src/memory/epoch_manager.cpp`
	- `src/config/config_manager.cpp`
- Includes:
	- `PUBLIC include`
	- `PRIVATE src`
- Link option:
	- `-fuse-ld=mold`

### Test Target
- Testing is enabled with `enable_testing()`.
- GoogleTest is fetched via `FetchContent` (v1.17.0 tarball URL).
- Target: `stratadb_tests`
- Sources:
	- `tests/memory/epoch_manager_test.cpp`
	- `tests/config/config_manager_test.cpp`
- Linked libraries:
	- `stratadb`
	- `GTest::gtest_main`
	- `stratadb_warnings`
- Registration method:
	- `gtest_discover_tests(stratadb_tests)`

### Optional Build Modes
Build options in `CMakeLists.txt`:
- `STRATADB_ENABLE_ASAN`
- `STRATADB_ENABLE_UBSAN`
- `STRATADB_ENABLE_TSAN`
- `STRATADB_ENABLE_LTO`

Sanitizer options apply to both `stratadb` and `stratadb_tests` through the shared `enable_sanitizers(...)` helper.

## Data Flow
1. CMake configures targets and compile/link options from `CMakeLists.txt`.
2. `stratadb` is built as the core static library.
3. `stratadb_tests` links `stratadb` and GoogleTest.
4. `gtest_discover_tests(...)` exposes individual test cases to CTest.
5. Profiling data in `real_perf.txt` reports runtime hotspots from `stratadb_tests` execution.

## Key Design Decisions
- Shared warning policy through an interface target keeps diagnostics consistent across library and tests.
- Shared sanitizer helper avoids drift between production and test instrumentation.
- A dedicated test executable containing both memory and config tests provides one place for correctness and performance profiling.
- Linker selection (`-fuse-ld=mold`) is explicit to stabilize link-time behavior across environments.

## Trade-offs and Constraints
- The linker requirement (`mold`) must be available in the environment for successful linking with current flags.
- Strict warning-as-error behavior can block builds for minor issues, but improves long-term code quality.
- A single combined test binary is convenient for CTest integration, but can mix module-specific performance signals in one profile.

## Testing and Performance Evidence

### Test Coverage in Build Graph
`CMakeLists.txt` includes both module test sources in `stratadb_tests`:
- `tests/memory/epoch_manager_test.cpp`
- `tests/config/config_manager_test.cpp`

This confirms both architecture docs below correspond to code with dedicated tests.

### Attached Perf Evidence
From `real_perf.txt` (sampled on `stratadb_tests`):
- `EpochManager::reclaim()` appears as a major hotspot (~22% self in report table).
- `EpochManager::advance_epoch()` appears in top contributors (~7% self in report table).
- `ConfigManager::update_mutable(...)` and `ConfigManager::get_mutable() const` are present in sampled hot paths.

## Not Verified
- Build and test command execution in the current shell session for this update.
- Absolute reproducibility of percentages in `real_perf.txt` on a different machine or compiler build.
