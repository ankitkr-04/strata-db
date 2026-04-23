# Build and Toolchain Architecture

Author: Ankit Kumar
Date: 2026-04-17

## Last Updated
2026-04-23

## Change Summary
- 2026-04-17: Initial architecture document for the build and test toolchain.
- 2026-04-19: Expanded to systems-level format with explicit model, component rationale, tables, failure analysis, and observability guidance.
- 2026-04-20: Updated for NUMA linkage and current CMake behavior, including arena/tlab targets, sanitizer conflict checks, and test warning policy separation.
- 2026-04-23: Updated architecture navigation to include phase-3 memtable and skiplist node documentation. Synced with current `CMakeLists.txt` behavior for optional mold auto-detection, memtable source/test inclusion, and linker failure semantics.

## Purpose
Define the exact build, test, and instrumentation model used by this repository so contributors can reason about correctness gates and runtime diagnostic workflows without reverse-engineering `CMakeLists.txt`.

## Overview
The repository builds one static library (`stratadb`) and one test binary (`stratadb_tests`) using CMake, C++26, strict warning policy, optional sanitizers, and optional LTO. Toolchain behavior is centralized in one build graph so test and runtime diagnostics use the same compiler and link assumptions as production code.

## System Model
Think of this toolchain as three layers:

1. Policy layer: compiler standard, warnings-as-errors, sanitizer toggles.
2. Artifact layer: `stratadb` library and `stratadb_tests` executable.
3. Validation layer: CTest-discovered GoogleTest cases and optional perf-based inspection.

This model exists to prevent drift between local builds, CI-style checks, and profiling runs.

## Architecture / Design

| Area | Current Implementation | Why It Matters |
| --- | --- | --- |
| Build system | CMake >= 3.28 | Single declarative source for targets and options |
| Language mode | C++26 required, no extensions | Prevents compiler-specific behavior from leaking into core logic |
| Diagnostics | `stratadb_warnings` for library + explicit warning flags on `stratadb_tests` | Allows strict project warnings while avoiding warning-policy leakage into external test dependencies |
| Sanitization | `STRATADB_ENABLE_{ASAN,UBSAN,TSAN}` via shared helper | Keeps instrumentation behavior aligned between artifacts |
| Linker selection | `STRATADB_USE_MOLD` + `find_program(mold)` with fallback | Keeps fast-linker optimization optional instead of hard-required |
| NUMA dependency | `stratadb` links `numa` | Enables NUMA-aware memory components in core library build |
| Testing | `stratadb_tests` + `gtest_discover_tests(...)` | Exposes fine-grained tests through CTest without manual registration |

## Data Flow
```mermaid
graph TD
    A[CMake Configure] --> B[Policy Resolution]
    B --> C[stratadb_warnings]
    B --> D[Sanitizer Options]
    B --> ML["Optional mold detection"]
    B --> N[NUMA Link Dependency]
    C --> E[stratadb Static Library]
    ML --> E
    N --> E
    D --> E
    B --> TW[Test Warning Flags]
    TW --> F[stratadb_tests]
    ML --> F
    D --> F
    E --> F
    F --> G[gtest_discover_tests]
    G --> H[CTest Execution]
    H --> I[Perf/Debug Inspection]
```

## Components

### CMake Build Entry
#### Responsibility
Defines project language mode, targets, options, and test registration.

#### Why This Exists
Without a single build entry point, warning policy and sanitizer usage diverge quickly between modules and environments.

#### How It Works
`CMakeLists.txt` sets standard/compiler policy, defines options (`STRATADB_ENABLE_*`, `STRATADB_USE_MOLD`), resolves optional mold linker availability, creates targets, links dependencies, and registers tests.

#### Concurrency Model
Build graph generation is declarative; build parallelism is delegated to the underlying generator (`ninja` or `make`) and command-line flags.

#### Trade-offs
One central file improves consistency, but it can grow dense as more targets and build modes are added.

### Warning and Sanitizer Policy
#### Responsibility
Defines hard correctness gates and optional runtime race/memory instrumentation.

#### Why This Exists
Concurrency-heavy code can pass functional tests while still containing races or undefined behavior; instrumentation must be easy to enable without editing target definitions.

#### How It Works
`stratadb_warnings` is applied to `stratadb`, while `stratadb_tests` receives an explicit strict warning list through `target_compile_options(...)`. `enable_sanitizers(...)` conditionally adds compile/link flags for ASAN, UBSAN, and TSAN, and aborts configuration when ASAN and TSAN are enabled together.

#### Concurrency Model
Sanitizers instrument synchronization and memory operations at runtime; TSAN specifically observes inter-thread ordering behavior.

#### Trade-offs
Sanitizers increase build and runtime overhead, but provide higher confidence in concurrency safety.

### Core Target Composition and NUMA Linkage
#### Responsibility
Build one consistent core library artifact and provide explicit linkage to NUMA APIs used by allocator components.

#### Why This Exists
Memory and memtable components must compile under the same warning/sanitizer/link assumptions, and arena code requires NUMA symbols at link time.

#### How It Works
`stratadb` includes epoch/config, arena/TLAB, and skiplist memtable/node sources in one static library target, links `numa`, and conditionally gets mold link options when enabled and found.

#### Concurrency Model
NUMA configuration influences memory locality under multi-threaded workloads; it does not replace synchronization primitives but affects where memory is allocated and accessed.

#### Trade-offs
NUMA linkage enables topology-aware behavior but adds an external system dependency that must exist on build machines.

### Artifact and Test Topology
#### Responsibility
Builds `stratadb` from source modules and validates behavior via `stratadb_tests`.

#### Why This Exists
A shared artifact graph ensures tests execute the same core code paths that production builds compile.

#### How It Works
`stratadb_tests` links `stratadb` and `GTest::gtest_main`, includes memory/config/memtable test sources (`epoch_manager`, `config_manager`, `arena`, `tlab`, `skiplist_memtable`), and is discovered via `gtest_discover_tests(...)`.

#### Concurrency Model
Test binary includes multi-threaded test cases from memory, config, and memtable modules, so runtime toolchain behavior directly affects race visibility.

#### Trade-offs
A single test binary is operationally simple, but hotspot attribution across modules can require symbol-level filtering in perf reports.

## Key Design Decisions
| Decision | Why | Alternative Rejected | Trade-off |
| --- | --- | --- | --- |
| Interface warning target (`stratadb_warnings`) | One source of truth for diagnostics | Per-target duplicated warning flags | Less local flexibility per target |
| Shared sanitizer helper | Avoids sanitizer drift across targets | Manual sanitizer flags on each target | Helper complexity in CMake logic |
| ASAN+TSAN conflict guard | Prevents invalid sanitizer combination at configure time | Let both be enabled and fail later | Stricter configuration behavior |
| Explicit `numa` link on `stratadb` | Guarantees NUMA symbol availability for memory subsystem | Rely on transitive/system-default linkage | Requires libnuma availability on build hosts |
| Dedicated `stratadb_tests` binary | Unified execution path for module tests | Per-module test binaries only | Mixed performance signals in one process |
| Optional mold linker (`STRATADB_USE_MOLD`) | Prefer fast linker when installed while remaining portable | Hard-require mold in all environments | Linker behavior can differ between machines |

## Failure Modes
| Scenario | Cause | Impact | Mitigation |
| --- | --- | --- | --- |
| Link behavior differs across machines | mold present on one machine and absent on another | Different linker performance characteristics | Pin `STRATADB_USE_MOLD` policy in shared build presets/CI |
| Build fails at link step (NUMA) | `libnuma` unavailable while `stratadb` links `numa` | No library/test artifact output | Install NUMA development package for host OS |
| Sanitizer build fails unexpectedly | Unsupported sanitizer/runtime combo | Instrumented test run blocked | Enable one sanitizer at a time and verify compiler/runtime support |
| Configure fails before build | ASAN and TSAN are enabled together | CMake configuration stops | Enable only one of ASAN or TSAN |
| Warnings block merge | Strict `-Werror` policy | Build break on warning regressions | Fix warning at source or adjust policy intentionally with review |
| Incomplete test registration | Misconfigured `gtest_discover_tests` path | Tests silently skipped in CTest | Validate discovered tests list in CTest output |

## Observability
- Build graph and target definition: inspect `CMakeLists.txt`.
- Runtime validation path: `stratadb_tests` discovered by CTest.
- Performance evidence source: `real_perf.txt`.
- Hot symbols already present in profile include epoch and config paths (`EpochManager::reclaim`, `EpochManager::advance_epoch`, `ConfigManager::update_mutable`, `ConfigManager::get_mutable`).
- Memory subsystem build/test scope now includes arena/TLAB plus skiplist memtable targets through core and test source entries.

## Usage / Interaction
| Task | Interaction Point | Expected Outcome |
| --- | --- | --- |
| Build core library and tests | Configure and build from `CMakeLists.txt` | `stratadb` and `stratadb_tests` produced |
| Run correctness suite | Execute CTest-discovered tests | Module-level behavior validation |
| Inspect runtime cost | Review `real_perf.txt` or run perf workflow | Hotpath candidates for optimization |
| Validate NUMA linkage path | Build `stratadb` with host NUMA library available | Successful link for arena/TLAB-capable core library |

## Related Documents
- [01-epoch-reclamation.md](01-epoch-reclamation.md)
- [02-configuration-management.md](02-configuration-management.md)
- [03-memory-arena.md](03-memory-arena.md)
- [04-thread-local-allocation.md](04-thread-local-allocation.md)
- [05-skiplist-memtable.md](05-skiplist-memtable.md)
- [06-skiplist-node.md](06-skiplist-node.md)

## Notes
- Not verified: exact build/test command success in this edit session.
- Not verified: portability of `STRATADB_USE_MOLD` linker-selection behavior across all developer machines.
- Not verified: portability of `numa` linkage assumptions to all developer machines.
