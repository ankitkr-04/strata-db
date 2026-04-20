# Memory Arena Architecture

Author: Ankit Kumar
Date: 2026-04-20

## Last Updated
2026-04-20

## Change Summary
- 2026-04-20: Created architecture documentation for Arena allocation, NUMA/page strategy behavior, and validation/perf workflow.

## Purpose
Explain how the Arena provides page-backed block allocation for memory subsystems, how policy from `memory_policy.hpp` controls mapping behavior, and how correctness and runtime behavior are validated.

## Overview
`Arena` is a monotonic, block-based allocator backed by `mmap`. It consumes `MemoryConfig` from `include/stratadb/config/memory_policy.hpp` and applies page strategy, NUMA policy, and optional prefaulting at creation time. Runtime allocation is atomic-offset based with no per-allocation locks.

## System Model
Arena follows a reservation-and-slice model:

1. Reserve one contiguous virtual memory region (`total_budget_bytes`) with page strategy fallback rules.
2. Optionally apply NUMA placement policy.
3. Serve fixed-or-larger aligned blocks from a single atomic offset.
4. Return empty span on budget exhaustion.
5. Rewind to start only through explicit `reset()`.

This model avoids per-allocation metadata churn and makes allocation cost predictable under concurrency.

## Architecture / Design

| Area | Implementation | Why It Matters |
| --- | --- | --- |
| Config source | `include/stratadb/config/memory_policy.hpp` (`MemoryConfig`) | Centralizes page/NUMA/budget controls |
| Backing memory | `mmap` in `src/memory/arena.cpp` | Large contiguous region for block slicing |
| Allocation cursor | `std::atomic<std::size_t> offset_` | Lock-free block reservation across threads |
| Block sizing | `max(min_size, tlab_size_bytes)` then 4KB align | Ensures predictable lower bound and alignment |
| Failure signaling | Empty `std::span<std::byte>` | Non-throwing OOM behavior in hot paths |

| Policy Dimension | Options in `memory_policy.hpp` | Runtime Effect |
| --- | --- | --- |
| Page strategy | 4K, 2MB (strict/opportunistic), 1GB (strict/opportunistic) | Controls huge-page attempt and fallback path |
| NUMA policy | UMA, Interleaved, StrictLocal | Controls mbind placement mode |
| Prefault | `bool prefault` | Forces touch of mapped pages at create time |

## Data Flow
```mermaid
flowchart TD
    A[MemoryConfig] --> B[Arena.create]
    B --> C[Page-strategy mmap sequence]
    C --> D{mapping success?}
    D -- no --> E[return ArenaError]
    D -- yes --> F[apply NUMA policy]
    F --> G{mbind success?}
    G -- no --> H[fallback effective NUMA=UMA]
    G -- yes --> I[keep configured NUMA policy]
    H --> J[optional prefault]
    I --> J
    J --> K[Arena ready]
    K --> L[allocate_block(min_size)]
    L --> M[atomic fetch_add offset]
    M --> N{within budget?}
    N -- yes --> O[return aligned span]
    N -- no --> P[return empty span]
```

## Components

### Arena
#### Responsibility
Reserve mapped memory and provide concurrent block allocations from a monotonic cursor.

#### Why This Exists
Threaded allocation paths need low-overhead reservation without allocator-global lock contention for each medium-sized block.

#### How It Works
- `Arena::create(...)` performs page-strategy mapping attempts.
- `allocate_block(...)` computes block size, aligns to 4KB, then reserves space using `offset_.fetch_add`.
- `reset()` rewinds offset to zero.

#### Concurrency Model
`allocate_block` uses one atomic cursor (`offset_`) with relaxed ordering. No per-call mutex exists in the hot path.

#### Trade-offs
- Very fast concurrent reservation path.
- No per-allocation free; memory reuse happens only via `reset()`.

### Memory Policy Integration
#### Responsibility
Translate configuration policy into mapping and placement behavior.

#### Why This Exists
The same allocator must run on systems with different huge-page and NUMA capabilities.

#### How It Works
`MemoryConfig` fields (`page_strategy`, `numa_policy`, `prefault`, budgets) are consumed during create. Opportunistic strategies try larger pages first and fall back when needed.

#### Concurrency Model
Policy is immutable during arena lifetime and not synchronized at allocation-time.

#### Trade-offs
Startup path is more complex, but runtime allocation path stays simple.

### NUMA and Mapping Path
#### Responsibility
Apply NUMA placement and robust fallback when placement cannot be enforced.

#### Why This Exists
Strict policy-only allocation would fail on hosts without compatible NUMA setup.

#### How It Works
After successful mapping, `mbind` is attempted according to configured policy. On failure, implementation records effective fallback to UMA behavior instead of failing arena creation.

#### Concurrency Model
Placement is applied once during create; ongoing allocations are lock-free cursor operations.

#### Trade-offs
Improves portability, but effective runtime policy can differ from requested policy when host support is missing.

## Key Design Decisions
| Decision | Why | Alternative Rejected | Trade-off |
| --- | --- | --- | --- |
| Atomic monotonic offset allocation | Keep multi-threaded block reservation lock-free | Per-thread mutex-protected freelists in Arena | No granular free/return path |
| Opportunistic huge-page fallback chain | Survive on hosts lacking huge pages | Strict-only page strategy by default | Runtime page size may be smaller than requested |
| NUMA fallback to UMA on mbind failure | Preserve allocator availability | Hard fail on NUMA placement error | Topology preference may not be enforced |
| Empty-span OOM signaling | Keep hot path noexcept and branchable | Throwing allocation exceptions | Caller must check span emptiness |

## Failure Modes
| Scenario | Cause | Impact | Mitigation |
| --- | --- | --- | --- |
| Create fails with `MmapFailed` | Mapping fails for all attempted strategies | Arena unavailable | Reduce budget or use simpler page strategy |
| Create fails with `OutOfMemory` | Kernel returns `ENOMEM` | Arena unavailable under pressure | Lower total budget or free system memory |
| Allocation returns empty span | Cursor crosses budget bound | Caller cannot obtain new block | Handle OOM path and/or increase budget |
| Effective NUMA differs from requested | `mbind` failure | Different locality behavior than requested | Inspect runtime behavior and host NUMA setup |

## Observability
- Source of truth:
  - `include/stratadb/memory/arena.hpp`
  - `src/memory/arena.cpp`
  - `include/stratadb/config/memory_policy.hpp`
- Correctness tests: `tests/memory/arena_test.cpp` (initialization, alignment, OOM, reset, move semantics, concurrency, NUMA/prefault safety).
- Full test run status (current workspace): all 37 discovered tests pass.
- Heavy run status: `perf/gtest_heavy_release.log` contains 40 repeated full-suite passes.
- Perf artifact: `perf/perf_report_release.txt` and `perf/perf.data.release`.

## Performance Characteristics
- Allocation: O(1) atomic bump pointer
- mmap / mbind: one-time cost during initialization
- Prefault (if enabled): O(N) upfront memory touch

### Notes
- No significant runtime overhead after initialization
- Not a primary bottleneck in current design

## Usage / Interaction
| Step | Call Site Pattern | Expected Outcome |
| --- | --- | --- |
| Configure policy | Fill `MemoryConfig` | Defines page, NUMA, and budget behavior |
| Create arena | `Arena::create(cfg)` | `expected<Arena, ArenaError>` success/failure path |
| Reserve blocks | `arena.allocate_block(min_size)` | Returns aligned span or empty span on OOM |
| Rewind arena | `arena.reset()` | Offset returns to start for reuse |

## Notes
- Not verified: absolute NUMA placement enforcement behavior on every target host kernel configuration.
- Not verified: release-only profiling deltas, because active build used current non-sanitized workspace build configuration.