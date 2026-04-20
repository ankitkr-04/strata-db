# Epoch Reclamation Architecture

Author: Ankit Kumar
Date: 2026-04-17

## Last Updated
2026-04-19

## Change Summary
- 2026-04-17: Initial architecture write-up for epoch-based reclamation.
- 2026-04-19: Expanded with explicit system model, lifecycle visualization, component-level rationale, design/failure tables, and observability workflow.

## Purpose
Document how epoch-based reclamation is implemented in StrataDB so readers can reason about thread lifecycle rules, memory safety boundaries, and reclaim behavior under contention.

## Overview
The module implements epoch-based deferred reclamation in `EpochManager`. Threads register into fixed slots, readers publish active epochs through an RAII read guard, retired objects are appended to per-thread retire lists, and reclaim frees only objects older than the minimum active epoch.

## System Model
The reclamation model is epoch-based quiescence tracking:

1. Each participating thread owns one slot in a fixed slot array.
2. Active readers publish their observed epoch in that slot.
3. Writers retire objects with a retire epoch instead of deleting immediately.
4. Reclaim computes the minimum active epoch and frees only nodes strictly older than that boundary.

This converts unsafe immediate free into a deferred free protocol driven by observed reader progress.

## Architecture / Design

| Element | Backing Type | Role in Safety |
| --- | --- | --- |
| Global logical clock | `std::atomic<uint64_t> global_epoch_` | Defines retirement timeline |
| Per-thread state | `ThreadState` array (`MAX_THREADS=128`) | Captures liveness and reader epoch |
| Reader marker | `state` with `UINT64_MAX` sentinel | Distinguishes active vs inactive slot |
| Deferred free queue | `std::vector<RetireNode> retire_list_` per slot | Holds reclaim candidates |
| Read scope guard | `EpochManager::ReadGuard` | Ensures enter/leave symmetry |
| Registration guard | `EpochManager::ThreadGuard` | Ensures register/unregister symmetry |

## System Context
- Public API and constants: `include/stratadb/memory/epoch_manager.hpp`
- Implementation: `src/memory/epoch_manager.cpp`
- Unit tests: `tests/memory/epoch_manager_test.cpp`

## Data Flow
```mermaid
flowchart TD
	A[ThreadGuard ctor] --> B[register_thread]
	B --> C[ReadGuard ctor]
	C --> D[enter publishes epoch]
	D --> E[retire stores node with retire_epoch]
	E --> F[advance_epoch]
	F --> G[reclaim scans active thread states]
	G --> H{retire_epoch < min_active_epoch}
	H -- yes --> I[deleter(ptr)]
	H -- no --> J[node kept in retire list]
	C --> K[ReadGuard dtor]
	K --> L[leave sets UINT64_MAX]
	A --> M[ThreadGuard dtor]
	M --> N[unregister_thread]
```

## Components

### EpochManager
#### Responsibility
Own global epoch state, per-thread slot metadata, retirement queues, and reclaim execution.

#### Why This Exists
Immediate deletion is unsafe under concurrent readers because readers can still hold pointers after a writer updates shared state.

#### How It Works
- Tracks current logical epoch in `global_epoch_`.
- Tracks each thread in a fixed `thread_states_` array.
- Uses a thread-local slot index (`thread_index_`) to avoid per-call slot lookup.
- Reclaims only nodes older than the minimum active epoch.

#### Concurrency Model
- Atomics are used for epoch and slot liveness/visibility.
- Per-thread retire lists reduce global write contention.
- Reclaim observes all in-use thread slots with acquire loads.

#### Trade-offs
- Fixed maximum thread count simplifies layout but imposes a hard cap.
- Reclaim cost scales with slot scan size.

### ReadGuard
#### Responsibility
Provide scoped reader participation in epoch tracking.

#### Why This Exists
Manual enter/leave calls are fragile in exception paths and early returns.

#### How It Works
Constructor calls `enter()`, destructor calls `leave()`, move operations transfer ownership and null the source guard pointer.

#### Concurrency Model
Publishes reader epoch to the owning thread slot using release store and clears with sentinel on scope exit.

#### Trade-offs
RAII makes control flow safer, but readers still control read section duration, which can delay reclaim.

### ThreadGuard
#### Responsibility
Ensure thread-slot ownership follows scope lifetime.

#### Why This Exists
Thread registration is required before read/retire/reclaim operations that depend on a valid slot index.

#### How It Works
Registers on construction and unregisters on destruction.

#### Concurrency Model
Uses compare-and-exchange on `in_use` to claim slots and release store to return slots.

#### Trade-offs
Prevents accidental leaked registrations, but requires all participating threads to opt in explicitly.

### RetireNode and ThreadState
#### Responsibility
Represent deferred reclamation units and per-thread reclaim context.

#### Why This Exists
Deferred free needs both payload pointer and safe destruction callback plus retirement timestamp.

#### How It Works
`RetireNode` stores `{ptr, deleter, retire_epoch}`. Each slot accumulates nodes in `retire_list_` and periodically triggers reclaim.

#### Concurrency Model
Retire list ownership is thread-local; other threads do not push into that list.

#### Trade-offs
Low retire-path contention, but deferred memory can accumulate when readers stall.

## Key Design Decisions
| Decision | Why | Alternative Rejected | Trade-off |
| --- | --- | --- | --- |
| Fixed slot array (`MAX_THREADS=128`) | Stable storage and cache-friendly slot scan | Dynamic unbounded slot container | Hard upper bound on registered threads |
| Sentinel active-state encoding (`UINT64_MAX`) | Compact active/inactive representation | Separate active flag + epoch field checks | Requires careful sentinel semantics |
| Per-thread retire list | Avoids global queue lock on retire path | Global synchronized retire queue | Reclaim progress can be uneven per thread |
| Periodic reclaim trigger (`RECLAIM_MASK`) | Amortizes reclaim overhead across retire calls | Reclaim on every retire | Delayed memory release in low-retire cadence |

## Failure Modes
| Scenario | Cause | Impact | Mitigation |
| --- | --- | --- | --- |
| Reclaim stall | Long-lived reader keeps old epoch published | Retired memory cannot be freed | Keep read sections short and bounded |
| Registration failure | All slots in use (`MAX_THREADS` reached) | Runtime exception from `register_thread()` | Increase limit or reduce concurrent participant count |
| API misuse by unregistered thread | Missing `ThreadGuard`/registration | Assertion failure or terminate path | Enforce thread entry policy in call sites |
| Retire-list growth pressure | Retire rate exceeds reclaim progress | Memory growth and latency spikes | Periodic reclaim, epoch advancement, and backpressure yield |

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

## Observability
- Correctness surface: `tests/memory/epoch_manager_test.cpp` cases for reclaim, stall, and slot reuse behavior.
- Runtime cost surface: perf hotspot symbols in attached profile identify reclaim and epoch-advance overhead concentration.
- Debug strategy:
	- Inspect reclaim path state transitions in `src/memory/epoch_manager.cpp`.
	- Correlate retire pressure with list growth controls (`RECLAIM_MASK`, `RETIRE_LIST_THRESHOLD`).

## Usage / Interaction
| Operation | Required Context | Expected Safety Property |
| --- | --- | --- |
| Read shared data | Hold `ReadGuard` in active thread | Deferred frees cannot reclaim data visible to that reader |
| Publish retire candidate | Registered thread with valid slot | Object is retired, not immediately deleted |
| Progress reclamation | Advance epoch and call reclaim | Objects older than minimum active epoch become reclaimable |
| Thread teardown | Scope-exit `ThreadGuard` | Slot is released and thread marked inactive |

## Not Verified
- Production workload behavior beyond the provided tests.
- Throughput comparison against alternate reclamation strategies.
- Reproducibility of attached profile percentages across different machines and compiler settings.
