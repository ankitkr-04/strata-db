# Configuration Architecture

Author: Ankit Kumar
Date: 2026-04-16

## Purpose
Explain how StrataDB separates startup-only configuration from runtime-adjustable configuration, and how the configuration manager keeps read access safe under concurrency.

## Overview
The configuration subsystem is centered on `ConfigManager`, which holds an immutable configuration snapshot, manages a dynamically replaceable mutable configuration, and uses the memory epoch manager to protect readers while the mutable state is updated. The public API is defined in [include/stratadb/config/config_manager.hpp](include/stratadb/config/config_manager.hpp) and its supporting config types live in the same directory.

## System Context
Configuration is split into two distinct layers:

- `ImmutableConfig` for values that should be fixed after startup.
- `MutableConfig` for values that can change while the engine is running.

The module is header-defined in [include/stratadb/config/immutable_config.hpp](include/stratadb/config/immutable_config.hpp), [include/stratadb/config/mutable_config.hpp](include/stratadb/config/mutable_config.hpp), [include/stratadb/config/thresholds.hpp](include/stratadb/config/thresholds.hpp), and [include/stratadb/config/wal_policy.hpp](include/stratadb/config/wal_policy.hpp).

## Components and Responsibilities

### ImmutableConfig
[`ImmutableConfig`](include/stratadb/config/immutable_config.hpp) stores values that are intended to remain stable for the lifetime of the engine instance.

Verified fields and defaults:
- `block_size` defaults to `4096`.
- `wal_directory` defaults to `./wal`.

These values represent startup configuration that higher-level code can read but should not mutate through the runtime update path.

### MutableConfig
[`MutableConfig`](include/stratadb/config/mutable_config.hpp) groups the settings that are intended to be replaceable at runtime.

Verified fields:
- `memtable` of type `MemTableThresholds`
- `background_compaction_threads` with default value `2`
- `wal` of type `WalConfig`

### MemTableThresholds
[`MemTableThresholds`](include/stratadb/config/thresholds.hpp) defines memory thresholds used by the memtable subsystem.

Verified defaults:
- `max_byte`: 64 MiB
- `flush_trigger_bytes`: 48 MiB
- `stall_trigger_bytes`: 60 MiB

These thresholds provide a staged response model: regular operation, flush pressure, and stall pressure.

### WalConfig
[`WalConfig`](include/stratadb/config/wal_policy.hpp) controls write-ahead-log behavior.

Verified fields and defaults:
- `policy` default: `WalSyncPolicy::Immediate`
- `max_batch_size` default: `1024`
- `max_interval_ms` default: `10ms`

`WalSyncPolicy` currently exposes three values:
- `Immediate`
- `Batch`
- `Async`

### ConfigManager
[`ConfigManager`](include/stratadb/config/config_manager.hpp) is the owning and access coordination point for the configuration model.

Verified characteristics:
- Constructor takes `ImmutableConfig`, `MutableConfig`, and `memory::EpochManager&`.
- Copy and move operations are deleted.
- It stores `ImmutableConfig immutable_` by value.
- It stores the mutable configuration as `std::atomic<MutableConfig*> current_mutable_{nullptr}`.
- It stores a reference to `memory::EpochManager`.

This structure indicates that immutable settings are embedded directly, while mutable settings are replaced through an atomic pointer and protected through epochs.

### ReadGuard
[`ConfigManager::ReadGuard`](include/stratadb/config/config_manager.hpp) is the scoped access helper for reading the mutable configuration.

Verified behavior from the header:
- Constructor stores a pointer to the epoch manager and a pointer to the mutable config.
- Destructor calls `epoch_mgr_->leave()` if the epoch manager pointer is not null.
- Copy operations are deleted.
- Move constructor transfers both pointers and nulls out the source.
- Move assignment calls `leave()` on the current guard if needed, then transfers ownership and nulls the source.
- `operator->()` returns a const pointer to `MutableConfig`.
- `get()` returns a const reference to `MutableConfig`.

## Data Flow

### Read Path
1. A caller asks `ConfigManager` for mutable configuration access through `get_mutable()`.
2. The returned `ReadGuard` keeps the epoch manager alive for the duration of the read scope.
3. The guard exposes const access to the active `MutableConfig` snapshot.
4. When the guard is destroyed, it calls `leave()` on the epoch manager unless ownership has been transferred.

The implementation of `get_mutable()` is not present in the header, so the exact sequencing of epoch entry and atomic load is `Not verified` from the available source.

### Write Path
1. A caller supplies a replacement `MutableConfig` to `update_mutable()`.
2. `ConfigManager` is responsible for updating the atomic mutable pointer.
3. The old configuration must be handled in a way that keeps active readers safe.

The implementation of `update_mutable()` is not present in the header, so the exact swap and retirement steps are `Not verified` from the available source.

## Key Design Decisions

### Split Immutable and Mutable Settings
The configuration model separates values that are stable after startup from values that may need runtime adjustment. This keeps startup-only data simple while allowing operational parameters to evolve without recreating the engine.

### Atomic Pointer for Mutable State
`current_mutable_` is stored as `std::atomic<MutableConfig*>` rather than as an owning smart pointer. The header shows a deliberate pointer-based model for the mutable snapshot, which keeps the active configuration reference lightweight.

### Epoch-Guarded Reads
`ConfigManager` depends on `memory::EpochManager`, and the nested `ReadGuard` is the mechanism that ties configuration reads to the epoch system. This is the key concurrency boundary for safe reuse of old mutable snapshots.

### Move-Aware ReadGuard
The guard is movable and clears the source pointers after transfer. That prevents double-leave behavior and makes it practical to return or forward a read guard without copying it.

## Trade-offs and Constraints

### What This Design Optimizes For
- Fast reads of the active mutable configuration.
- Clear separation between permanent and runtime-adjustable values.
- Safe replacement of mutable configuration snapshots under concurrency.

### What It Costs
- The runtime configuration path depends on epoch management discipline.
- `ConfigManager` cannot be copied or moved, so ownership is intentionally fixed.
- Thread-safety details for `get_mutable()` and `update_mutable()` cannot be fully audited from declarations alone.

### Operational Constraints
- A read guard must not outlive its intended scope.
- Any code that updates mutable configuration must assume readers may still hold the previous snapshot.
- Long-running work should not be placed inside the guarded read section unless it is known to be safe for epoch-based reclamation.

## Not Verified
- The exact atomic memory ordering used by `get_mutable()` and `update_mutable()`.
- Whether `get_mutable()` explicitly enters the epoch before loading the pointer.
- The reclamation timing for old `MutableConfig` instances.
- Any additional validation or normalization performed before configuration updates are published.
