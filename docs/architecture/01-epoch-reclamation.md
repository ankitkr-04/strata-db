# Epoch Reclamation Architecture

Author: Ankit Kumar
Date: 2026-04-17

## Purpose
Document how epoch-based reclamation is implemented in StrataDB so readers can reason about thread lifecycle rules, memory safety boundaries, and reclaim behavior under contention.

## Overview
The module implements epoch-based deferred reclamation in `EpochManager`. Threads register into fixed slots, readers publish active epochs through an RAII read guard, retired objects are appended to per-thread retire lists, and reclaim frees only objects older than the minimum active epoch.

## System Context
- Public API and constants: `include/stratadb/memory/epoch_manager.hpp`
- Implementation: `src/memory/epoch_manager.cpp`
- Unit tests: `tests/memory/epoch_manager_test.cpp`

## Components and Responsibilities

### EpochManager
`EpochManager` owns global epoch state and per-thread slot state.

Verified state model:
- `global_epoch_`: `std::atomic<uint64_t>`
- `thread_states_`: fixed `std::array<ThreadState, defaults::MAX_THREADS>`
- `thread_index_`: thread-local slot index (`INVALID_THREAD` when not registered)

### ReadGuard
`EpochManager::ReadGuard` calls `enter()` on construction and `leave()` on destruction. It is move-only and nulls the source on move.

### ThreadGuard
`EpochManager::ThreadGuard` calls `register_thread()` on construction and `unregister_thread()` on destruction. It is non-copyable.

### RetireNode and ThreadState
Each `ThreadState` contains:
- `state` (`std::atomic<uint64_t>`) where `UINT64_MAX` means inactive
- `retire_list_` (`std::vector<RetireNode>`)
- `in_use` (`std::atomic<bool>`)

`RetireNode` stores `ptr`, `deleter`, and `retire_epoch`.

## Data Flow
1. A thread enters the subsystem via `ThreadGuard`, which claims one slot.
2. A read critical section creates `ReadGuard`, which publishes current `global_epoch_` to the slot.
3. `retire(T*)` wraps type-specific deletion into a type-erased deleter and appends to the caller's retire list with current epoch.
4. `advance_epoch()` increments logical time.
5. `reclaim()` computes `min_epoch` from active thread states and frees nodes where `retire_epoch < min_epoch`.
6. `ReadGuard` and `ThreadGuard` destructors leave the epoch and unregister thread state respectively.

## Key Design Decisions

### Fixed Slot Capacity
The implementation uses a fixed upper bound (`defaults::MAX_THREADS = 128`) to avoid dynamic thread metadata allocation during hot paths.

### Sentinel Encoding for Active State
The thread activity marker is encoded in one atomic `state` value (`UINT64_MAX` for inactive), which keeps active/inactive transitions simple and cheap.

### Per-thread Retire Lists
Each thread retires into its own vector to avoid global retire-list lock contention.

### Periodic Reclaim Trigger
`retire_node(...)` attempts reclaim when `(retire_list_.size() & defaults::RECLAIM_MASK) == 0`, and yields if list size exceeds `defaults::RETIRE_LIST_THRESHOLD`.

## Trade-offs and Constraints
- Hard cap on concurrently registered threads (`128`).
- Reclamation latency depends on readers progressing and leaving their read sections.
- Reclaim scans all slots, so slot count affects reclaim cost.
- Correctness depends on using `ThreadGuard`/`register_thread()` before API calls that assume a valid slot.

## Testing and Performance Evidence

### Test Evidence
`tests/memory/epoch_manager_test.cpp` includes dedicated tests for:
- Single-thread reclaim correctness
- Deferred deletion while a reader remains active
- Contention stress (`TSANStress`)
- Batching behavior
- Epoch stall behavior
- Thread slot reuse
- Multi-epoch reclaim
- Reclaim idempotence

### Perf Evidence from Attached Profile
`perf` shows `stratadb_tests` samples that include epoch manager hotspots:
- `stratadb::memory::EpochManager::reclaim()` is one of the largest self-time contributors in the report.
- `stratadb::memory::EpochManager::advance_epoch()` appears among the highest sampled symbols.
- `stratadb::memory::EpochManager::retire_node(void*, void (*)(void*))` and retire deleter paths are present.

This aligns with stress-heavy tests that repeatedly retire, advance epochs, and reclaim.

## Not Verified
- Production workload behavior beyond the provided tests.
- Throughput comparison against alternate reclamation strategies.
- Reproducibility of attached profile percentages across different machines and compiler settings.
