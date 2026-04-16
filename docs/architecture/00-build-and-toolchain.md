# Build System and Toolchain

Author: Ankit Kumar
Date: 2026-04-16

## Purpose
Provide a stable, verified reference for how StrataDB is built and validated in this repository so contributors can make changes without introducing toolchain drift.

## Overview
StrataDB currently uses a CMake-based C++26 toolchain with strict warning enforcement, optional sanitizer profiles, and optional LTO. This document records the active configuration in the repository and the constraints contributors should preserve when evolving build settings.

## System Context
The build configuration is defined primarily in `CMakeLists.txt`, with code-style and static-analysis policies in `.clang-format` and `.clang-tidy`. Repository hygiene for generated artifacts is controlled by `.gitignore`.

## Build Configuration

### CMake Baseline
- Minimum required CMake version: `3.28`.
- Project language: `CXX`.
- Enforced language standard: `C++26` (`CMAKE_CXX_STANDARD 26`, required, extensions disabled).
- Compilation database export is enabled (`CMAKE_EXPORT_COMPILE_COMMANDS ON`).

### Linker Configuration
- Global link options include `-fuse-ld=mold`.
- Environments building this repository must provide a linker compatible with that flag.

### Target Model
- `stratadb_strict_warnings` is defined as an `INTERFACE` target for warning policy.
- `stratadb` is defined as a static library target.
- `stratadb` links against `stratadb_strict_warnings`.
- Target-level compile feature requirement is `cxx_std_26`.

### LTO Control
- `STRATADB_ENABLE_LTO` (default `OFF`) controls LTO.
- When enabled, interprocedural optimization is turned on for target `stratadb`.

## Compiler Safety Policy
Strict warnings are treated as part of correctness, not optional linting.

Enabled warning/error flags:
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

## Sanitizer Profiles
The CMake configuration exposes dedicated options for runtime diagnostics:

- `STRATADB_ENABLE_ASAN`: applies `-fsanitize=address` at compile and link time for `stratadb`.
- `STRATADB_ENABLE_UBSAN`: applies `-fsanitize=undefined` at compile and link time for `stratadb`.
- `STRATADB_ENABLE_TSAN`: applies `-fsanitize=thread` at compile and link time for `stratadb`.

All three options default to `OFF` and are intended for explicit diagnostic builds.

## Formatting and Static Analysis

### clang-format Policy
The repository style is LLVM-based with explicit overrides for readability and modern C++ syntax handling, including:
- `IndentWidth: 4`
- `ColumnLimit: 100`
- `BreakBeforeBraces: Allman`
- `RequiresClausePosition: SingleLine`
- `BinPackParameters: false`
- `BinPackArguments: false`

### clang-tidy Policy
Enabled check groups:
- `cppcoreguidelines-*`
- `bugprone-*`
- `performance-*`
- `modernize-*`

Additional enforcement and filtering:
- `WarningsAsErrors: '*'`
- `HeaderFilterRegex: 'src/|include/'`
- `cppcoreguidelines-pro-bounds-pointer-arithmetic.IgnoreMacros: true`

## Repository Hygiene
Generated artifacts and local machine outputs are excluded from version control via `.gitignore`, including:
- build directories (`build/`, `out/`, `bin/`, `lib/`)
- CMake-generated files (`CMakeFiles/`, `CMakeCache.txt`, `cmake_install.cmake`)
- compilation database output (`compile_commands.json`)
- local editor/cache/runtime artifacts

## Verified Gaps
- `CMakeLists.txt` references `src/dummy.cpp` and include directories `include` and `src`.
- In the current workspace snapshot, `src/` and `include/` directories are not present.
- Build success is `Not verified` in this state because referenced source paths are missing.

## Maintenance Guidance
- Keep warning and sanitizer policy target-scoped.
- Keep documentation synchronized with build-policy changes in the same pull request.
- If linker or standard constraints change, update this document immediately to avoid configuration drift.
