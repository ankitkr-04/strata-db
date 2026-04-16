# Memory Reclamation via Epoch-Based Reclamation

Author: Ankit Kumar
Date: 2026-04-16

## Purpose
Explain how StrataDB safely reclaims memory in highly concurrent scenarios where readers and writers operate simultaneously, and why this mechanism was chosen over alternatives.

## Overview
StrataDB provides lock-free memory reclamation through an **Epoch-Based Reclamation (EBR)** manager. This mechanism allows threads to retire memory (request deletion) and safely reclaim it once all active readers have moved to a new epoch. The implementation prioritizes lightweight read paths and is located in [include/stratadb/memory/epoch_manager.hpp](include/stratadb/memory/epoch_manager.hpp).

## System Context

### Public API
The `EpochManager` class provides these operations:

- `register_thread()`: Called by a new thread on startup to allocate its slot.
- `unregister_thread()`: Called by a thread on shutdown to release its slot.
- `enter()`: Called when a thread begins a critical read section.
- `leave()`: Called when a thread exits a critical read section.
- `retire(T* ptr)`: Type-safe API to defer deletion of a pointer.
- `advance_epoch()`: Called to increment the global epoch and begin garbage collection.
- `reclaim()`: Called to actually reclaim retired memory.

### Data Structures

**Global Epoch Counter** (`global_epoch_`)
- Type: `std::atomic<uint64_t>`.
- Only incremented by `advance_epoch()`.
- Synchronizes visibility of retirements across threads.

**Thread States Array** (`thread_states_`)
- Type: `std::array<ThreadState, 128>`.
- Fixed size of 128 threads; hardcoded to match typical concurrent workloads.
- Each element is aligned to `std::hardware_destructive_interference_size` (64 bytes on x86-64).

**Thread-Local Index** (`thread_index_`)
- Type: `thread_local std::size_t`.
- Each thread caches its own slot index to avoid repeated array lookups.
- Initialized to `INVALID_THREAD` (maximum size_t) for detection of unregistered threads.

**Thread State Structure**
- `epoch`: Current epoch observed by this thread.
- `active`: Boolean flag indicating if the thread is in a critical section.
- `retire_list_`: Vector of retired pointers awaiting reclamation.
- `in_use`: Boolean flag indicating if this thread slot is currently allocated.

**Retirement Nodes** (`RetireNode`)
- `ptr`: The pointer to be deleted.
- `deleter`: Function pointer to a type-erased deleter (`void (*)(void*)`).
- `retire_epoch`: The global epoch at the time of retirement.

## Design Decisions and Tradeoffs

### Decision 1: Epoch-Based Reclamation vs. Alternatives

**EBR (Chosen)**
- Read path cost: Two atomic operations (`load` and `store`) with acquire/release semantics.
- Write path cost: Each retirement appends to a thread-local vector.
- Garbage collection: Blocked until all threads have observed a newer epoch.
- **Rationale**: Minimizes latency on the hot read path. Readers do not traverse pointers or inspect addresses; they simply announce their epoch and proceed.

**What was not chosen:**
- Hazard Pointers: Require readers to publish exact memory addresses being accessed, adding full sequential consistency barriers to every pointer traversal.
- Reference Counting (e.g., `std::shared_ptr`): Atomic increment/decrement on every access; worse cache behavior and higher synchronization overhead.
- Manual Memory Management: Risk of Use-After-Free in concurrent scenarios.

### Decision 2: Fixed Thread Array vs. Dynamic Scaling

**Fixed Array of 128 (Chosen)**
- Memory layout is stable; no reallocation.
- Thread slot addresses are known at compile-time.
- Concurrent slot updates do not invalidate memory.
- **Hard limit**: Only 128 threads can be registered simultaneously.

**Why not dynamic?**
- Reallocation invalidates memory addresses, leading to Use-After-Free bugs in a lock-free system.
- 128 threads is sufficient for most storage layer workloads; context-switching overhead dominates beyond this point on typical hardware.

### Decision 3: Thread-Local Caching vs. Global Lookup

**Thread-Local Index (Chosen)**
- Each thread caches `thread_index_` in thread-local storage.
- Array access is O(1) without map lookups or hashing.
- No synchronization required for thread to find its own slot.

**Why not a global lookup?**
- Hash map or std::map lookup adds latency to every enter/leave call.
- Lock contention on a global map would serialize the fast path.

### Decision 4: Cache-Line Alignment for ThreadState

**Alignment to `hardware_destructive_interference_size` (Chosen)**
- ThreadState structures are 64-byte aligned on x86-64.
- Concurrent updates to adjacent thread epochs do not cause L1 cache line bouncing.
- Eliminates false sharing between independent threads.

**Impact**: Multiple threads can call `enter()` and `leave()` simultaneously on different cores without triggering unnecessary cache invalidations.

### Decision 5: Type-Erased Deletion

**Template with Function Pointer (Chosen)**
- `retire<T>()` is a template that captures `delete` of type `T`.
- Type information is erased into `void (*)(void*)` at call site.
- No virtual destructors or RTTI required.

**Why this approach?**
- Avoids vtable overhead and pointer-chasing in deferred deletion.
- Type safety at retirement time, but memory-efficient storage.

### Decision 6: Thread-Local Retire Queues vs. Global Queue

**Thread-Local Vectors (Chosen)**
- Each thread appends directly to its own `retire_list_` without synchronization.
- No CAS (Compare-And-Swap) loops or atomic contention on retirement.

**Tradeoff**:
- If a thread exits without calling `reclaim()`, its retired memory remains in its retire list.
- Mitigation: Long-lived threads (writers, compactors) must periodically call `reclaim()` to avoid accumulation.

**Why not a global MPSC queue?**
- Would require lock-free queue operations (atomic CAS) on every retirement.
- Adds contention to the write path.

## Behavioral Constraints

### Critical Section Scoping
- `enter()` announces thread presence in current epoch.
- `leave()` announces thread departure.
- **Critical constraint**: A stalled thread (e.g., preempted by OS) blocks garbage collection from advancing multiple epochs.
- **Mitigation**: Critical sections must be short and non-blocking.

### Epoch Advancement Dependency
- Memory is only reclaimed after **all threads** have left a prior epoch.
- If even one thread remains in an older epoch, garbage from that epoch cannot be reclaimed.
- `reclaim()` should be called regularly by writer/compaction threads to trigger cleanup.

### Retirement Semantics
- `retire(ptr)` does not immediately delete; it queues the deletion.
- Caller is responsible for ensuring `ptr` is no longer reachable before retirement.

## Not Verified
- Exact timing and synchronization semantics of `enter()`, `leave()`, `advance_epoch()`, and `reclaim()` implementations (not present in header).
- Specific memory usage under high contention or thread churn.
- Performance numbers under concurrent load.

## Implications for Usage
- Lock-free readers can call `enter()` and `leave()` with minimal overhead.
- Writers should batch retirements and periodically call `reclaim()`.
- System should avoid patterns where a thread enters a critical section and stalls indefinitely (e.g., blocking I/O inside a critical section).
