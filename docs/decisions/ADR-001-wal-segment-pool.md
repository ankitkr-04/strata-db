# ADR-001: Transition WAL from ring-based to sliding-window segment pool

**Date:** 2026-06-08
**Status:** Accepted

## Context
The previous WAL persistence used a circular ring file model where the on-disk file(s) were logically reused in-place. This pattern induced hidden write amplification and unpredictable P99 write latency on modern NVMe SSDs and ZNS devices because logical overwrites force the drive's FTL to perform internal GC and read-modify-write cycles.

The project needs a WAL implementation that maps naturally to modern SSD/ZNS semantics, reduces write-amplification and latency jitter, and provides a simple, auditable recovery model.

## Alternatives Considered
* **Maintain circular ring files (status quo):** Simple on-disk layout, but causes FTL write-amplification and drive-internal GC jitter. *Rejected because it produces P99 latency spikes and accelerates flash wear.*
* **Pre-allocated multi-file reuse (reuse rotated files):** Reduces some fragmentation but still relies on in-place reuse semantics and complex metadata updates. *Rejected due to residual fragmentation and filesystem inode churn.*
* **Append-only Sliding Window Segment Pool (chosen):** Create monotonically-numbered append-only segment files, preallocate extents, append sequentially, seal and unlink old segments to trigger TRIM/Zone Reset.

## Decision
Adopt an append-only, rolling `WalSegmentPool` that manages a sliding window of pre-allocated WAL segments indexed by monotonic sequence numbers (e.g., `wal_000000000.log`). Responsibilities and actors are separated into three operational roles:

- **Vanguard:** Background thread(s) responsible for low-latency pre-allocation of new segments using `fallocate()` (or ZNS `Zone Open`) so the filesystem/drive prepares physical extents ahead of writes.
- **Flusher:** Single-writer actor that owns the active segment and performs sequential appends. Flusher provides exclusive append access and ensures ordering and batching appropriate for durability semantics.
- **GC (Reclamation):** Executed by the MemTable flush pipeline. When a MemTable is safely persisted as an L0 SSTable, the flush thread calls into the WAL pool to unlink corresponding sealed segments, releasing blocks back to the device (triggering TRIM on traditional NVMe or Zone Reset on ZNS).

Key implementation constraints:
- No physical file reuse; instead, create new files and unlink old ones.
- Recovery discovers segments via directory scan ordered by sequence number, not by an index file.
- Use directory-level `fsync()` after segment creation/deletion to improve crash durability of the segment inventory.

## Consequences
### Positive
* Eliminates the common-case FTL read-modify-write cycles that cause write amplification.
* Greatly reduces SSD-induced background GC jitter, improving P99 write latency predictability.
* Preallocation decouples metadata work from the low-latency write path, reducing syscall cost during hot path appends.
* Maps cleanly to ZNS semantics: a WAL segment equals a Zone; Vanguard's create == Zone Open; GC's unlink == Zone Reset.

### Negative (Accepted Risks)
* Increased number of files and inode churn — monitor filesystem limits and tooling.
* Recovery requires directory scans; for very large WAL histories this can add startup latency (mitigated by bounded sliding window and archived segments).
* Requires careful coordination between Vanguard/Flusher/GC to avoid leaking preallocated segments or unlinking segments still referenced by readers.

## Rationale / Engineering Justification
1. **Write Amplification:** The ring model forces in-place updates which cause FTL read-modify-write and internal GC activity. Creating new files and unlinking old files lets drives reclaim blocks more efficiently via TRIM/Zone Reset, reducing write amplification and improving device longevity.
2. **Latency Predictability:** By avoiding frequent in-place overwrites and preallocating extents, the system prevents drive-side GC stalls that manifest as 1–10ms P99 write spikes.
3. **Filesystem-level Optimization:** Performing `fallocate()` in a Vanguard thread lets the VFS calculate extent trees ahead of time. Flusher appends then operate against pre-reserved extents, making write syscalls near-zero-cost for metadata.
4. **ZNS Future-Proofing:** The append-only segment model maps 1:1 to ZNS zones, providing a clear migration path to ZNS APIs with minimal semantic changes.

## Implementation Notes
- File naming: `wal_{sequence}.log` with zero-padded monotonic sequence numbers.
- Segment lifecycle: `Vanguard` scans the fixed-size pool and transitions `Empty` -> `Ready` after preallocation/open; `Flusher` transitions `Ready` -> `Active` and performs sequential appends; once sealed and no longer needed, reclamation unlinks eligible segments during MemTable flush completion.
- Durability: `Flusher` uses ordered write + fdatasync per configured durability level; after creating a new segment the code performs a directory `fsync()` to ensure the segment name is durable.
- Recovery: on startup, scan WAL directory, parse `wal_*` names, sort by sequence, and replay in order. Validate partial/zero-length segments and apply heuristics to recover partially-written segments.
- Concurrency & ownership:
  - `Flusher` holds exclusive ownership of the active segment file descriptor and write buffer; hand-offs are move semantics (unique ownership transfer).
  - `Vanguard` produces preallocated descriptors by operating on a fixed-size, array-backed state machine (`segments_[MAX_POOL_CAPACITY]`), transitioning `Empty` slots to `Ready`.
  - `Flusher` takes ownership by scanning for the next `Ready` slot and transitioning it to `Active`; payload data from user threads reaches `Flusher` through a separate lock-free MPSC queue.
  - Reclamation runs in the MemTable flush path and must observe any outstanding readers via epoch or reference counting before unlinking.

## Memory Ownership, Thread-Safety & Locality
- Use RAII wrappers for segment file descriptors and ensure move-only semantics for ownership transfer.
- Keep segment descriptor ownership in the fixed-size pool state machine (array-backed slot transitions) and reserve lock-free queueing for user payload delivery into `Flusher`.
- Protect the segment catalog (on-disk inventory) with sequence monotonicity; track in-memory minimal and maximal sequence numbers using atomics for cheap concurrent reads.
- Keep per-thread write buffers and align them to cache-line boundaries to reduce false sharing and improve cache locality for sequential append workloads.

## Migration & Rollout
1. Implement the segment-pool alongside the existing ring writer behind a feature flag.
2. Run dual-write testing and benchmark comparisons to validate P50/P95/P99 latency and device write amplification.
3. Flip default to segment-pool when metrics validate improvements; deprecate ring-based writer and mark any replaced ADR as superseded.

## Alternatives Revisited
- Reuse circular ring files: simple but unacceptable for NVMe/ZNS workloads.
- In-place file rotation: still retains fragmentation risks; insufficient benefit vs. complexity.

## Timeline & Updates
* **2026-06-08:** Initial decision accepted. Implementation includes `WalSegmentPool`, `Vanguard`, `Flusher`, `GC`. Directory-scan recovery and directory-level `fsync()` added.
* **2026-06-09:** Corrected implementation details: segment descriptor handoff is managed by a fixed-size array-backed pool state machine (`Empty` -> `Ready` -> `Active`), payload delivery uses a separate lock-free MPSC queue, and WAL segment reclamation is executed by the MemTable flush pipeline rather than a dedicated GC thread.
