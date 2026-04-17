# Memory Reclamation via Epoch-Based Reclamation

Author: Ankit Kumar
Date: 2026-04-17

## Purpose
Explain the implemented Epoch-Based Reclamation (EBR) design in StrataDB, including thread registration, read-side protection, deferred deletion, and reclaim behavior under concurrency.

## Overview
StrataDB uses an epoch manager to defer object deletion until it is safe relative to active readers. The API is defined in [include/stratadb/memory/epoch_manager.hpp](include/stratadb/memory/epoch_manager.hpp), the behavior is implemented in [src/memory/epoch_manager.cpp](src/memory/epoch_manager.cpp), and concurrency behavior is validated through [tests/memory/epoch_manager_test.cpp](tests/memory/epoch_manager_test.cpp).

## System Context

### Public API
`EpochManager` provides:
- `register_thread()`
- `unregister_thread()`
- `enter()`
- `leave()`
- `retire(T* ptr)`
- `advance_epoch()`
- `reclaim()`

It also exposes `ReadGuard`, an RAII helper that calls `enter()` on construction and `leave()` on destruction.

### Core Data Model

**Global epoch**
- `global_epoch_` is `std::atomic<uint64_t>`.
- `advance_epoch()` increments it using `fetch_add(1, std::memory_order_acq_rel)`.

**Thread slots**
- `thread_states_` is a fixed `std::array<ThreadState, 128>`.
- Each `ThreadState` is aligned to `std::hardware_destructive_interference_size`.
- Each slot contains:
  - `state` (`std::atomic<uint64_t>`)
  - `retire_list_` (`std::vector<RetireNode>`)
  - `in_use` (`std::atomic<bool>`)

**Thread-local index**
- `thread_index_` caches each thread's slot index.
- `INVALID_THREAD` is used to detect unregistered access.

**State encoding**
- `state == UINT64_MAX`: thread is not currently visible as an active reader.
- `state == e` (finite epoch): thread is active in epoch `e`.

**Retire nodes**
- Each node stores:
  - `ptr`
  - `deleter` (`void (*)(void*)`)
  - `retire_epoch`

### Defaults and Reclaim Tuning
From [include/stratadb/memory/epoch_manager.hpp](include/stratadb/memory/epoch_manager.hpp):
- `MAX_THREADS = 128`
- `RECLAIM_BATCH = 64`
- `RECLAIM_MASK = 63`
- `RETIRE_LIST_THRESHOLD = 10000`

## Components and Responsibilities

### Thread Lifecycle
- `register_thread()` atomically claims a free slot (`in_use: false -> true`) and stores the slot index in TLS.
- Double registration on the same thread triggers assertion failure and then termination (`std::terminate()`).
- If no slot is available, `register_thread()` throws `std::runtime_error`.
- `unregister_thread()` marks the thread inactive (`state = UINT64_MAX`), runs `reclaim()`, clears `in_use`, and resets TLS index.

### Read Path
- `enter()` loads `global_epoch_` (`memory_order_acquire`) and publishes it to slot `state` (`memory_order_release`).
- `leave()` stores `UINT64_MAX` to `state` (`memory_order_release`).
- `ReadGuard` wraps this lifecycle for scoped reads.

### Retirement Path
- `retire(T* ptr)` ignores null pointers.
- Non-null pointers are wrapped via type-erased deleter and forwarded to `retire_node()`.
- `retire_node()` records `retire_epoch = global_epoch_.load(memory_order_acquire)` and appends to the calling thread's `retire_list_`.
- Periodic reclaim is attempted when `(retire_list_.size() & RECLAIM_MASK) == 0`.
- If retire backlog exceeds `RETIRE_LIST_THRESHOLD`, the thread yields (`std::this_thread::yield()`).

### Reclaim Path
- `reclaim()` starts with `min_epoch = global_epoch_.load(memory_order_acquire)`.
- It scans all in-use slots and reduces `min_epoch` using each active slot state (`state != UINT64_MAX`).
- It reclaims only from the current thread's `retire_list_`.
- A node is deletable only when `node.retire_epoch < min_epoch`.

## Key Design Decisions

### Sentinel-Based Active Tracking
Instead of separate `active` and `epoch` fields, the implementation uses one atomic `state` with sentinel encoding. This reduces metadata and keeps active/inactive transitions explicit.

### Fixed Slot Array
A fixed-size array avoids relocation and simplifies ownership of per-thread retire lists, at the cost of a hard cap (`MAX_THREADS`).

### Thread-Local Retire Ownership
Retire operations are lock-free with local vector append. Reclaim remains local to the owning thread, which keeps write-side contention low.

### Periodic Reclaim and Backpressure
Batch-triggered reclaim and yield-on-growth provide simple backpressure controls to limit retire-list growth under sustained retirement rates.

## Trade-offs and Constraints

### What This Design Optimizes
- Low-overhead read entry/exit.
- Cheap retirement path via per-thread vectors.
- Deterministic safety condition for deletion (`retire_epoch < min_epoch`).

### Costs and Constraints
- Hard thread limit of 128 concurrent registered threads.
- Correctness depends on callers respecting register/enter/leave/unregister lifecycle.
- A stalled active reader can delay reclamation progress.
- Reclaim is local to the calling thread's retire list, so cleanup cadence depends on thread behavior.

## Validation Results

### Local test execution (2026-04-17)
Verified from [tests/memory/epoch_manager_test.cpp](tests/memory/epoch_manager_test.cpp):

- `ctest --output-on-failure` from build directory:
  - 8/8 tests passed
  - Total real time: `1.09 sec`
- `TSAN_OPTIONS="halt_on_error=1" ./epoch_manager_tests`:
  - 8/8 tests passed
  - Suite runtime: `828 ms`
- `ctest -R DeferredDeletion --output-on-failure`:
  - 1/1 test passed
  - Total real time: `0.02 sec`

### Behaviors covered by tests
- Single-thread reclamation correctness.
- Deferred deletion while another reader pins an epoch.
- High-concurrency stress behavior.
- Reclaim behavior under stalled reader conditions.
- Thread slot reuse under repeated register/unregister cycles.
- Multi-epoch reclaim and reclaim idempotence.

## Not Verified
- Long-duration memory footprint behavior under production-scale load.
- Throughput/latency benchmarks against alternative reclamation designs.
- End-to-end engine call paths that invoke epoch APIs outside unit tests.

## Usage Implications
- Register each participating thread before using epoch APIs.
- Prefer `ReadGuard` where scoped read safety is needed.
- Keep read critical sections short to avoid reclaim stalls.
- Call `advance_epoch()` and `reclaim()` regularly on retirement-heavy paths.
