# WAL Staging Architecture

Author: Ankit Kumar  
Date: 2026-04-27

## Last Updated
2026-05-24

## Change Summary
- 2026-05-24: Synced the staging document with the current `manager.hpp` / `pipeline_variant.hpp` layout and the six-pipeline selection matrix.
- 2026-04-27: Original WAL staging draft (basic staging & handoff).  
- 2026-05-17: Rewrote to clarify the critical distinction "Block Sealing ≠ Block Flushing", document `GammaBlock` (SSD-oriented) vs `DeltaBlock` (HDD-oriented) layouts, and justify `XXH3` vs `CRC32C` selection. Aligned claims to source code where possible.
- 2026-05-18: Overhauled block physics for `O_DIRECT` compatibility and eliminated per-record if/else overhead. Added:
  - The 2D Template Matrix explanation showing how `WalPipeline<Layout, Queue>` + `std::visit` produce batch-level dispatch with zero virtual-call overhead on the hot path.
  - `FlushResult` semantics and how `partial_flush()` / `finalize()` map to sector-aligned write spans for the I/O engine.
  - A focused description of divergent device physics: why `GammaBlock` relies on overlapping overwrites (SSD) and `DeltaBlock` uses padded per-sector sealing (HDD).

## Purpose
Explain the in-memory WAL staging semantics that matter for correctness and durability, focusing on the recent break-through: sealing a block for handoff is not the same as flushing it to durable media. Document the two concrete block layouts (`GammaBlock` and `DeltaBlock`) and why they exist.

## Overview
The staging subsystem assembles WAL blocks in thread-local memory and hands completed (sealed) blocks to a flush pipeline. Two distinct hardware realities drive different block layouts and handoff semantics:

- SSDs (atomic overlapping overwrites on small LBAs) are modeled by `GammaBlock` and permit whole-block metadata (a single 128-bit `XXH3` hash) written with an overwrite of sector 0. See [include/stratadb/wal/block/gamma_block.hpp](include/stratadb/wal/block/gamma_block.hpp).
- HDDs (rotational, 4Kn physical sectors, mechanical seek cost) are modeled by `DeltaBlock` and use per-sector `CRC32C` checksums to seal each sector before advancing. See [include/stratadb/wal/block/delta_block.hpp](include/stratadb/wal/block/delta_block.hpp).

Key takeaway: sealing a block makes it ready for the I/O engine (no further in-memory mutation), but flushing is the act of handing that sealed, sector-aligned span to the `PosixIoEngine` (or other engine) and ensuring the OS/hardware makes it durable. The staging code performs sealing; the IO engine performs flushing.

## System Model
| Component | Responsibility | Code reference |
| --- | --- | --- |
| `GammaBlock<BlockSize>` | SSD-oriented block layout: whole-block final hash computed with `XXH3_128bits`, seals by rewriting from offset `0` to the final end to persist header mutation. | [include/stratadb/wal/block/gamma_block.hpp](include/stratadb/wal/block/gamma_block.hpp)
| `DeltaBlock<BlockSize>` | HDD-oriented block layout: per-sector CRC32C checksums are written into reserved CRC slots (last 4 bytes of each 4KiB sector); sealing a sector advances the append cursor to the next sector boundary. | [include/stratadb/wal/block/delta_block.hpp](include/stratadb/wal/block/delta_block.hpp)
| `WalManager` | Probes `IOCapabilities`, resolves `WalConfig`, and selects one of six `StagingVariant` alternatives: `Ssd4kMpscPipeline`, `Ssd4kSpscPipeline`, `Ssd16kMpscPipeline`, `Ssd16kSpscPipeline`, `Hdd4kMpscPipeline`, or `Hdd4kSpscPipeline`. | [include/stratadb/wal/manager.hpp](include/stratadb/wal/manager.hpp), [include/stratadb/wal/pipeline_variant.hpp](include/stratadb/wal/pipeline_variant.hpp)

## Architecture / Design
### Why Block Sealing ≠ Block Flushing
What: sealing is a purely in-memory state transition on the staging side (compute trailing checksums/hashes, zero-pad to sector boundaries, and mark the block's flush span). Flushing is the I/O engine's responsibility to transfer the sealed span to storage and drive it durable.

Why this matters: conflating the two hides subtle but important assumptions about hardware write atomicity, overwrites, and the cost model for rotational devices. A sealed `GammaBlock` may require rewriting sector zero because the header (at offset 0) is updated during finalize — on SSDs this is safe (overwrites don't tear across sectors); on HDDs this would require extra care because partial sector writes and mechanical ordering can break checksum invariants.

How it works (summary):
- Staging appends records into a thread-local block until append fails.  
- `partial_flush()` and `finalize()` produce `FlushResult` describing a sector-aligned span to give to the IO engine.  
- The IO engine performs the writev/pwritev and then calls the appropriate sync/truncate/fdatasync semantics to guarantee durability.

### The 2D Template Matrix (Batch-Level Dispatch)

What: The staging subsystem implements a 2D compile-time matrix of implementations: `WalPipeline<Layout, Queue>` is a template over two orthogonal axes — the physical `Layout` (e.g., `GammaBlock`, `DeltaBlock`) and the concurrency `Queue` implementation (e.g., a Vyukov MPSC queue or an SPSC mailbox).

Why: Encoding these orthogonal choices as template parameters produces a family of fully inlined, zero-virtual functions for the hot path (`stage_write`) while still allowing the runtime to pick the single correct pipeline instance once per batch.

How it works: `WalManager` constructs exactly one concrete pipeline type (one cell of the 2D matrix) based on the probed device capabilities and the resolved `WalConfig`. At runtime `WalManager` stores that concrete pipeline inside a `std::variant` (`StagingVariant` in `pipeline_variant.hpp`) and uses `std::visit` to resolve the variant once per batch in `write_batch()`. Inside the `std::visit` callback the compiler sees a concrete `WalPipeline<Layout, Queue>` type and can inline `stage_write` across the loop of records, eliminating virtual calls and branching inside the hot record-append path.

Example (conceptual) `std::visit` usage inside `WalManager::write_batch`:

```cpp
std::visit([&batch](auto &pipeline) {
  for (auto &rec : batch.records) {
    pipeline.stage_write(rec);
  }
}, pipeline_variant_);
```

Trade-offs: `std::variant` resolution costs a single type-dispatch per batch (or per `write_batch()` invocation) instead of per-record overhead. This shifts the dispatch cost to batch boundaries and keeps the inner loop extremely cheap. The downside is increased binary code size due to template instantiations for each matrix cell.

See: [include/stratadb/wal/pipeline.hpp](include/stratadb/wal/pipeline.hpp) and [include/stratadb/wal/manager.hpp](include/stratadb/wal/manager.hpp).

### Partial Flushes and `FlushResult`

What: `FlushResult` is the small value-type that `WALBlockLayout` implementations return from `partial_flush()` and `finalize()` to describe exactly what the IO engine must write.

Structure (from `wal_concept.hpp`):
- `std::span<const std::byte> memory_to_write` — a contiguous, sector-aligned view of the block memory region that should be submitted to the I/O engine (typically used directly with `writev`/`pwritev`).
- `std::size_t block_internal_offset` — the offset (in bytes) inside the physical block where `memory_to_write` starts. The IO engine can use this to determine where in the logical block the write applies and how to compose multi-span writes.

Why: Explicitly returning the span plus the offset decouples the staging logic from the IO engine's write strategy: the block is free to return any contiguous, properly padded span that satisfies device alignment and verification invariants (whole-block rewrite for `GammaBlock`, per-sector sealed spans for `DeltaBlock`).

How: `partial_flush()` advances internal `flush_offset_` and returns the span between the previous `flush_offset_` and the new `append`-aligned end. `finalize()` returns the final sealed span (for `GammaBlock` this starts at offset 0 because the header is updated). The IO engine then consumes `FlushResult.memory_to_write` and must ensure durability semantics (fdatasync/fsync or suitable device-specific barriers) as required by the higher-level policy.

Not verified: exact IO engine uses of `block_internal_offset` across platforms — see `docs/architecture/08-io-engine-physics.md` for IO-engine contract notes.

## Data Flow (high level)
```mermaid
graph TD
  Writer --> Staging[WalStaging (thread-local)]
  Staging -->|seal (finalize/partial_flush)| SealedBlock[Sealed Block]
  SealedBlock --> IOEngine[PosixIoEngine / other]
  IOEngine -->|writev + sync| Storage[SSD / HDD]
```

## Components
### GammaBlock (SSD-oriented)
#### Responsibility
Pack records, zero-pad to sector boundaries, compute a whole-block 128-bit `XXH3` hash in `finalize()`, and return a contiguous memory span for writing; because the header is updated at finalize, the implementation rewrites from offset `0` through the end of valid data so the header update reaches disk with the same write sequence.

#### Why This Exists
Modern NVMe/SSD controllers and Linux block layers provide write atomicity properties for overlapping overwrites of small LBAs (the code comments call this "AWUPF" physics). This permits a layout that relies on whole-block re-writes without per-sector padding checks.

#### How It Works (code facts)
- `header.block_hash = XXH3_128bits(this, end_offset);` — compute final hash.  
- `finalize()` returns a span starting at offset `0` to ensure the updated header (sector 0) is included in the write.  
- `partial_flush()` aligns end to 4KiB and zero-pads the tail before returning a span starting at the previous flush offset.

#### Concurrency Model
Mutation occurs only in the producing writer thread until `finalize()`/handoff. After handoff the block is immutable from the staging point of view.

#### Trade-offs
- Pros: fewer per-sector metadata writes, compact final verification via a single 128-bit hash.  
- Cons: requires the IO engine and device to safely handle overlapping overwrites; not safe on HDDs.

### DeltaBlock (HDD-oriented)
#### Responsibility
Pack records into sectors, reserve the last 4 bytes of each sector for a per-sector CRC32C, and ensure that before progressing to the next sector the CRC for the current sector is written. `finalize()` is implemented as a per-sector sealing operation.

#### Why This Exists
Mechanical disks have strong constraints: partial-sector updates can corrupt checksums, and mechanical seeks dominate latency. Padded per-sector sealing lets the system avoid costly RMW of earlier sectors and ensures each sector has an independent checksum invariant.

#### How It Works (code facts)
- On append, when `append_offset_ & SECTOR_MASK == CRC_OFFSET`, code computes `crc32c` over the sector payload and writes it into the reserved CRC slot.  
- `partial_flush()` pads the incomplete sector, computes tail CRC via `utils::crc32c(...)`, writes the CRC into the sector, and advances `append_offset_` to the next sector boundary.  
- `finalize()` simply invokes the same per-sector sealing logic.

#### Concurrency Model
Same as GammaBlock — writer-local until handoff.

#### Trade-offs
- Pros: per-sector integrity, safe on HDDs without relying on overwrite atomicity; avoids needing to rewrite sector 0 just because header changed.  
- Cons: per-sector CRCs consume extra bytes/capacity and require additional CPU for checksum computation.

## XXH3 vs CRC32C — hardware rationale
| Use | Algorithm | Reason |
| --- | --- | --- |
| Whole-block final integrity (SSD / GammaBlock) | `XXH3_128bits` | Fast non-cryptographic 128-bit hash with excellent speed on wide registers; cheap to compute for whole-block verification and stable across large contiguous overwrites. Implementation uses `xxhash.h` (`gamma_block.hpp`). |
| Per-sector integrity (HDD / DeltaBlock) | `CRC32C` | CRC32C is native-friendly (hardware-accelerated on many CPUs via SSE/ARM CRC instructions), produces a small 32-bit checksum stored per-sector, and matches the requirement for lightweight per-sector verification in constrained sector-sized writes. Code uses `utils::crc32c(...)` (`delta_block.hpp`). |

Trade-off summary: `XXH3` gives a stronger (longer) catch-all end-to-end fingerprint for grouped sectors on devices that can atomically accept overlapping overwrites; `CRC32C` gives a cheap per-sector guard for devices where sector-level sealing is necessary and costly rewrites are unacceptable.

## Failure Modes
| Scenario | Cause | Impact | Mitigation |
| --- | --- | --- | --- |
| Header update requires rewrite on device not supporting safe overlapping overwrites | Using `GammaBlock.finalize()` on an HDD-like device | Corrupted checksum invariants or inconsistent header on disk | Ensure `WalManager` selects `DeltaBlock` when `is_rotational==true` (see `manager.hpp`) or add detection checks in IO engine before handing the span to storage |
| Checksum computation cost | High append rates leading to frequent per-sector CRCs | CPU pressure in writer thread | Move sealing work to background thread (compute+write) or use hardware-accelerated CRC routines where available |
| Incorrect device probing | Wrong `hw_sector_size`/`is_rotational` detection | Wrong pipeline chosen (`GammaBlock` vs `DeltaBlock`) | Ensure robust device probing and conservative defaults (prefer `DeltaBlock` when unsure)

## Observability
- Code locations to inspect:  
  - [include/stratadb/wal/block/gamma_block.hpp](include/stratadb/wal/block/gamma_block.hpp#L1-L240)  
  - [include/stratadb/wal/block/delta_block.hpp](include/stratadb/wal/block/delta_block.hpp#L1-L240)  
  - [include/stratadb/wal/manager.hpp](include/stratadb/wal/manager.hpp#L1-L200)  
- Runtime signals to add or monitor: stage_write latency, per-block finalize duration (hash/crc compute), ready-queue length, IO engine writev/sync latency.

## Validation / Test Matrix
| Scenario | How to validate | Code / test hint |
| --- | --- | --- |
| GammaBlock finalize correctness | Create an in-memory block, append records, call `finalize()`, verify `header.block_hash` matches `XXH3_128bits` of the returned span | Unit test invoking `GammaBlock::finalize()` and `XXH3_128bits` over the returned span |
| DeltaBlock sector sealing correctness | Append payload so that partial sector sealing is triggered, inspect written CRC slots for correct `crc32c` values | Unit test checking `utils::crc32c` and the contents of sector CRC fields after `partial_flush()` |

## Usage / Interaction
- Choice of pipeline: `WalManager` auto-selects one of `Ssd4kMpscPipeline`, `Ssd4kSpscPipeline`, `Ssd16kMpscPipeline`, `Ssd16kSpscPipeline`, `Hdd4kMpscPipeline`, or `Hdd4kSpscPipeline` based on `is_rotational`, `physical_sector_size`, and the resolved SPSC mode (`manager.hpp` / `pipeline_variant.hpp`).  
- Staging only seals blocks — ensure the flush pipeline (IO engine + durable sync) is implemented and verified before assuming durability.

### Pipeline selection pseudocode

At construction time `WalManager` resolves hardware capabilities and the resolved `WalConfig` to pick one concrete pipeline cell. The decision tree is intentionally simple and conservative:

```cpp
StagingVariant make_pipeline(const IOCapabilities& hw, const WalConfig& cfg) {
  // Prefer explicit manual SPSC override when requested
  if (cfg.spsc_mode == SpscMode::ManualOverride) {
    if (hw.is_rotational) {
      return (hw.physical_sector_size == 16384) ? StagingVariant::Hdd16kSpscPipeline
                                                : StagingVariant::Hdd4kSpscPipeline;
    } else {
      return (hw.physical_sector_size == 16384) ? StagingVariant::Ssd16kSpscPipeline
                                                : StagingVariant::Ssd4kSpscPipeline;
    }
  }

  // Default: MPSC fallback for general multi-writer deployments
  if (hw.is_rotational) {
    return StagingVariant::Hdd4kMpscPipeline;
  }
  return (hw.physical_sector_size == 16384) ? StagingVariant::Ssd16kMpscPipeline
                                            : StagingVariant::Ssd4kMpscPipeline;
}
```

Notes:
- The decision inputs are: `IOCapabilities::is_rotational`, `IOCapabilities::physical_sector_size`, and the resolved `WalConfig::spsc_mode`.
- `WalManager` constructs one concrete `WalPipeline<Layout, Queue>` and stores it in `StagingVariant`; `write_batch()` then uses `std::visit` once per batch to dispatch into the concrete type.

## Notes
- Not verified: end-to-end durability until flush pipeline integration and sync semantics are exercised by system tests.  
- Not verified: the precise interaction with device write caches and enterprise NVMe guarantees on every platform; see `docs/architecture/08-io-engine-physics.md` for the IO engine and OS-level sync considerations.
- Tearing matrix size scales with block cache-line count and is cache-line padded.

