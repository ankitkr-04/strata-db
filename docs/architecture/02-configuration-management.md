# Configuration Management Architecture

Author: Ankit Kumar
Date: 2026-04-17

## Purpose
Describe how StrataDB models immutable and mutable configuration, and how `ConfigManager` publishes runtime updates safely for concurrent readers.

## Overview
Configuration is split into startup-fixed values (`ImmutableConfig`) and runtime-replaceable values (`MutableConfig`). `ConfigManager` owns both layers and uses `EpochManager` to keep pointer-based mutable snapshots safe while updates and reads happen concurrently.

## System Context
- Public headers:
	- `include/stratadb/config/config_manager.hpp`
	- `include/stratadb/config/immutable_config.hpp`
	- `include/stratadb/config/mutable_config.hpp`
	- `include/stratadb/config/thresholds.hpp`
	- `include/stratadb/config/wal_policy.hpp`
- Implementation:
	- `src/config/config_manager.cpp`
- Tests:
	- `tests/config/config_manager_test.cpp`

## Components and Responsibilities

### ImmutableConfig
`ImmutableConfig` stores startup-time values by value.

Verified fields and defaults:
- `block_size` default: `4096`
- `wal_directory` default: `./wal`

### MutableConfig
`MutableConfig` stores runtime-adjustable values.

Verified fields:
- `memtable` (`MemTableThresholds`)
- `background_compaction_threads` default `2`
- `wal` (`WalConfig`)

### MemTableThresholds
Verified defaults:
- `max_byte`: `64 * 1024 * 1024`
- `flush_trigger_bytes`: `48 * 1024 * 1024`
- `stall_trigger_bytes`: `60 * 1024 * 1024`

### WalConfig and WalSyncPolicy
Verified enum values:
- `Immediate`
- `Batch`
- `Async`

Verified defaults:
- `policy`: `WalSyncPolicy::Immediate`
- `max_batch_size`: `1024`
- `max_interval_ms`: `10ms`

### ConfigManager
Verified design from header and source:
- Constructor: `ConfigManager(ImmutableConfig, MutableConfig, memory::EpochManager&) noexcept`
- Non-copyable and non-movable
- Stores immutable state as `immutable_config_`
- Stores mutable state as `std::atomic<MutableConfig*> current_mutable_config_`
- Uses `std::mutex update_mutex_` in update path
- Uses `memory::EpochManager& epoch_mgr_` for read safety and deferred reclamation

### ConfigManager::ReadGuard
`ReadGuard` wraps `memory::EpochManager::ReadGuard` plus a `const MutableConfig*`.

Verified behavior:
- Move constructible, non-copyable
- `operator->()` and `get()` provide const access
- Lifetime of epoch participation is tied to the embedded epoch guard

## Data Flow
1. Construction allocates the first mutable snapshot (`new (std::nothrow) MutableConfig`) and stores it atomically.
2. `get_mutable()` creates `memory::EpochManager::ReadGuard`, then loads current mutable pointer with `memory_order_acquire`, then returns `ConfigManager::ReadGuard`.
3. `update_mutable(...)` allocates a replacement snapshot, acquires `update_mutex_`, and atomically swaps pointers using `exchange(..., memory_order_acq_rel)`.
4. The old pointer is retired through `epoch_mgr_.retire(old_ptr)`.
5. Destructor retires the current pointer and forces progress with two `advance_epoch()` calls followed by `reclaim()`.

## Key Design Decisions

### Snapshot Replacement via Atomic Pointer
Writers do not mutate an in-place shared object. They allocate a new snapshot and atomically publish it, which keeps read path simple.

### Epoch-protected Read Lifetime
Reads hold an epoch guard while dereferencing the mutable snapshot pointer, preventing reclamation of the pointed object until readers leave.

### Serialized Pointer Publication
`update_mutex_` serializes concurrent updates so only one writer performs exchange/retire at a time.

### Defensive Allocation Failure Handling
Allocation uses `new (std::nothrow)` and terminates on allocation failure, avoiding partial-update states.

## Trade-offs and Constraints
- Snapshot replacement allocates memory on every update.
- Old snapshots are reclaimed asynchronously, so temporary memory growth is expected under heavy write rates.
- Update throughput is bounded by one writer at a time due to `update_mutex_`.
- Correctness depends on proper `EpochManager` participation by all threads that call into configuration reads and writes.

## Testing and Performance Evidence

### Test Evidence
`tests/config/config_manager_test.cpp` provides dedicated tests for:
- Basic read defaults
- Update visibility
- Concurrent readers
- Concurrent read/write workload
- No-use-after-free stress pattern
- Reclamation occurrence
- Pointer stability during guard lifetime

### Perf Evidence from Attached Profile
`perf tests` has samples from `stratadb_tests` that include config paths:
- `stratadb::config::ConfigManager::update_mutable(...)` appears in top sampled paths.
- `stratadb::config::ConfigManager::get_mutable() const` appears in sampled read paths.
- Allocation and mutex symbols (`operator new`, `malloc`, `pthread_mutex_lock`) are present alongside update calls.

This is consistent with a snapshot-allocation + synchronized update design.

## Not Verified
- End-to-end production workload tuning values for mutable configuration.
- Runtime policy for how often callers should invoke epoch advancement/reclaim outside tests.
- Reproducibility of attached profile percentages across toolchain/environment changes.
