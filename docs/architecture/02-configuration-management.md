# Configuration Management Architecture

Author: Ankit Kumar
Date: 2026-04-17

## Last Updated
2026-04-27

## Change Summary
- 2026-04-17: Initial architecture document for immutable/mutable config split.
- 2026-04-19: Expanded with system model, data-flow diagrams, and failure analysis.
- 2026-04-20: Refocused component breakdown around runtime behavior.
- 2026-04-23: Synced API naming and navigation links.
- 2026-04-27: Corrected lifecycle semantics to match current code: destructor retires current snapshot (not force-drain), get_mutable has explicit registration guard, and update path uses serialized pointer exchange plus epoch retirement.

## Purpose
Describe how runtime configuration updates are published safely to concurrent readers using immutable snapshots and epoch-protected pointer lifetime.

## Overview
The subsystem uses a split config model:

1. ImmutableConfig is copied at manager construction and never mutated.
2. MutableConfig is published through an atomic pointer.
3. Readers access MutableConfig through a guard that carries an epoch read guard.
4. Writers replace snapshots under a writer mutex and retire old snapshots through EpochManager.

The design avoids in-place shared mutation and keeps reader code lock-free after thread registration.

## System Model
| Layer | Type | Mutation Policy | Concurrency Boundary |
| --- | --- | --- | --- |
| Immutable state | ImmutableConfig | Construction only | None needed at runtime |
| Mutable runtime state | MutableConfig | Whole-snapshot replacement | Atomic pointer publication |
| Coordinator | ConfigManager | Serialized writer updates | update_mutex_ + epoch retire |

| Runtime API | Behavior | Safety Contract |
| --- | --- | --- |
| get_mutable() | Returns ReadGuard that exposes const MutableConfig* | Requires registered epoch thread, otherwise terminates |
| update_mutable(new_cfg) | Allocates replacement and atomically publishes it | Returns expected<void, ConfigError>; old snapshot retired via epoch manager |

## Architecture / Design
| Element | Current Implementation | Why It Matters |
| --- | --- | --- |
| Initial mutable snapshot | Constructor allocates with new (nothrow), terminates on failure | Ensures manager always starts with valid mutable state |
| Reader access | ReadGuard owns EpochManager::ReadGuard + loaded pointer | Snapshot pointer lifetime protected for guard scope |
| Writer publication | update_mutex_ + atomic exchange(acq_rel) | Deterministic writer ordering and publication |
| Snapshot retirement | epoch_mgr_.retire(old_ptr) | Defers free until readers quiesce |
| Teardown path | Destructor exchanges pointer to null and retires old pointer | Avoids immediate free while read guards may still exist |

## Data Flow
```mermaid
flowchart TD
    A[ConfigManager constructor] --> B[allocate MutableConfig snapshot]
    B --> C[store pointer in atomic current_mutable_config_]

    D[get_mutable] --> E{Epoch thread registered?}
    E -- no --> X[terminate]
    E -- yes --> F[construct Epoch ReadGuard]
    F --> G[atomic load current snapshot pointer]
    G --> H[return ConfigManager ReadGuard]

    I[update_mutable(new_cfg)] --> J[allocate replacement snapshot]
    J --> K{allocation success?}
    K -- no --> L[return ConfigError.OutOfMemory]
    K -- yes --> M[lock update_mutex_]
    M --> N[atomic exchange old and new pointer]
    N --> O[epoch_mgr.retire(old_ptr)]
    O --> P[return success]

    Q[ConfigManager destructor] --> R[exchange pointer with nullptr]
    R --> S{old pointer present?}
    S -- yes --> T[epoch_mgr.retire(old_ptr)]
    S -- no --> U[done]
```

## Components
### ImmutableConfig
#### Responsibility
Store startup-only settings that must remain stable.

#### Why This Exists
Startup invariants should not be affected by runtime tuning operations.

#### How It Works
Held by value in ConfigManager and initialized in constructor.

#### Concurrency Model
No runtime synchronization required.

#### Trade-offs
Changing immutable policy requires new manager construction.

### MutableConfig
#### Responsibility
Represent runtime-tunable knobs.

#### Why This Exists
Operational tuning must be possible without global reader pauses.

#### How It Works
Writers publish fully-formed replacement snapshots.

#### Concurrency Model
Readers observe immutable snapshot instances through atomic pointer loads.

#### Trade-offs
Update frequency directly impacts temporary memory footprint until reclaim catches up.

### ConfigManager
#### Responsibility
Coordinate snapshot publication, read guards, and deferred retirement.

#### Why This Exists
Without publication and lifetime coordination, readers can observe freed or partially updated state.

#### How It Works
- Constructor allocates initial MutableConfig snapshot and stores atomic pointer.
- get_mutable checks EpochManager registration before creating ReadGuard.
- ReadGuard creates EpochManager::ReadGuard first, then loads snapshot pointer.
- update_mutable allocates new snapshot, serializes with update_mutex_, exchanges pointer, retires old snapshot.
- Destructor exchanges pointer with null and retires final snapshot.

#### Concurrency Model
- Readers: epoch guard plus acquire pointer load.
- Writers: mutex-serialized exchange path.
- Reclamation: delegated to EpochManager.

#### Trade-offs
Simple read-side API, but relies on strict epoch registration discipline for both read and write threads.

## Key Design Decisions
| Decision | Why | Alternative Rejected | Trade-off |
| --- | --- | --- | --- |
| Atomic snapshot pointer publication | Keep reader path lock-free and simple | Shared mutable object with internal locks | Full snapshot allocation per update |
| Guard-based read API | Make snapshot lifetime explicit in call sites | Naked pointer return | Slightly heavier call-site object lifetime |
| Writer serialization with mutex | Deterministic update ordering | Lock-free multi-writer CAS loop | Writer throughput is serialized |
| Explicit registration gate in get_mutable | Fail fast on unsafe read usage | Best-effort fallback without guard | Misuse terminates process |
| Nothrow allocation + expected error in update path | Keep API explicit without exceptions | Throw-based update failures | Caller must branch on expected result |

## Failure Modes
| Scenario | Cause | Impact | Mitigation |
| --- | --- | --- | --- |
| get_mutable called unregistered | EpochManager::is_registered false | Process termination | Register every reader thread before config access |
| update_mutable allocation fails | new (nothrow) returns null | Update rejected with OutOfMemory | Handle error and retry with pressure relief |
| update thread not epoch-registered | retire precondition violated in EpochManager | Assertion/termination in debug or undefined lifecycle in misuse | Register writer threads too |
| Excess update churn | Updates outpace reclaim progress | Higher transient memory retention | Batch updates and drive reclaim cadence |
| Mis-scoped ReadGuard use | Caller stores views beyond guard scope | Potential stale access | Keep all usage within guard lifetime |

## Observability
- Source of truth:
  - include/stratadb/config/config_manager.hpp
  - src/config/config_manager.cpp
  - tests/config/config_manager_test.cpp
- Runtime checks:
  - Fatal guard for unregistered get_mutable callers.
  - Explicit OutOfMemory result for update failures.
- Reclaim behavior is explained in epoch architecture document.

## Validation / Test Matrix
| Test | What It Verifies | Safety Property |
| --- | --- | --- |
| BasicGetMutable | Reader can observe mutable snapshot | Snapshot visibility correctness |
| UpdateMutablePublishesNewValue | Writer publish path updates reader-visible state | Publication correctness |
| ConcurrentReadWrite | Reads and updates interleave under concurrency | No obvious read-path corruption |
| UpdateOutOfMemoryPath | Allocation failure returns ConfigError | Non-throwing failure behavior |

## Usage / Interaction
| Step | Caller Action | Required Condition | Expected Outcome |
| --- | --- | --- | --- |
| 1 | Register thread with EpochManager | Before read/write API use | Thread may safely participate |
| 2 | Read with get_mutable guard | Registered thread | Stable mutable snapshot for guard scope |
| 3 | Publish with update_mutable(new_cfg) | Registered writer thread | New snapshot published or explicit OOM error |
| 4 | Advance reclaim periodically | Active update workloads | Old snapshots eventually reclaimed |

## Related Documents
- [01-epoch-reclamation.md](01-epoch-reclamation.md)
- [05-skiplist-memtable.md](05-skiplist-memtable.md)

## Notes
- Not verified: recommended update frequency policy for production workloads.
- Not verified: memory retention profile under sustained high-frequency update traffic.
