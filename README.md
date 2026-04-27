# StrataDB

An embedded key-value storage engine built from scratch in C++26 -
focused on concurrency safety, memory locality, and learning how
storage systems actually work at the hardware level.

## Purpose
StrataDB is a systems-focused database engine project built in modern C++ with emphasis on concurrency safety, memory locality, and predictable performance under heavy multi-threaded workloads.

## Current Project Status

| Phase | Name | Status | Summary |
| --- | --- | --- | --- |
| 0 | Foundation | Completed | Build system, toolchain, strict warnings, and GoogleTest integration are in place. |
| 1 | Configuration and Concurrency | Completed | YAML-oriented configuration model with copy-and-publish mutable state plus epoch-based read safety. |
| 2 | Memory Subsystem | Completed | NUMA-aware Arena plus thread-local allocation buffers (TLAB) implemented and tested. |
| 3 | Lock-Free MemTable | In Progress | Skip-list memtable and node encoding are implemented with concurrent tests; flush integration and full phase completion remain in progress. |
| 4 | Durability (WAL) | In Progress | WAL block layout and WAL staging path are implemented; flush orchestration and durability validation are still in progress. |
| 5 | Storage Layer (SSTables and io_uring) | Planned | Immutable on-disk tables, block cache, and asynchronous I/O path. |
| 6 | Compaction | Planned | Background merge and tombstone cleanup to maintain read performance. |
| 7 | Interface Layer | Planned | Native C++ API, C FFI for language bindings, optional Redis-protocol front door. |
| 8 | Observability and Telemetry | Planned | Metrics export for allocator, cache, and compaction behavior. |
| 9 | CLI Toolchain | Planned | Operational tools for SSTable inspection, WAL verification, and manual maintenance actions. |
| 10 | Chaos Testing | Planned | Kill-and-recover validation to prove durability and correctness under process crashes. |

## What Is Implemented Till Today
- Build and toolchain architecture with sanitizer toggles and test discovery.
- Configuration subsystem with immutable/mutable split and epoch-protected mutable snapshot reads.
- Epoch reclamation mechanism with thread registration guards and deferred free pipeline.
- Arena allocator with mmap-backed blocks, huge-page strategy fallback, and NUMA policy integration.
- TLAB fast-path allocator backed by Arena refill semantics.
- SkipList memtable with `put`/`remove`/`get`/`scan`, threshold signaling, and lock-free insertion path.
- SkipList node binary layout with packed sequence+type trailer and overflow-safe allocation sizing.
- WAL block representation with cache-line aligned metadata, tearing matrix, and bounded record metadata.
- WAL staging path with per-thread block assembly and handoff queue for flush workers.
- Unit test suites for config, memory, and memtable subsystems.

## Advanced Documentation
Start here for architecture details beyond this README:

- [docs](docs)
- [docs/architecture/00-build-and-toolchain.md](docs/architecture/00-build-and-toolchain.md)
- [docs/architecture/01-epoch-reclamation.md](docs/architecture/01-epoch-reclamation.md)
- [docs/architecture/02-configuration-management.md](docs/architecture/02-configuration-management.md)
- [docs/architecture/03-memory-arena.md](docs/architecture/03-memory-arena.md)
- [docs/architecture/04-thread-local-allocation.md](docs/architecture/04-thread-local-allocation.md)
- [docs/architecture/05-skiplist-memtable.md](docs/architecture/05-skiplist-memtable.md)
- [docs/architecture/06-skiplist-node.md](docs/architecture/06-skiplist-node.md)
- [docs/architecture/07-wal-staging.md](docs/architecture/07-wal-staging.md)

## Build and Test Quickstart

Configure:

```bash
cmake -B build -DSTRATADB_ENABLE_TSAN=ON
```

Build:

```bash
cmake --build build -j
```

Run tests:

```bash
ctest --output-on-failure --test-dir build
```

## Repository Layout
- [include/stratadb](include/stratadb): public interfaces for config, memory, and memtable subsystems.
- [src](src): implementation source files.
- [tests](tests): unit tests for configuration, memory, and memtable components.
- [docs/architecture](docs/architecture): deep architecture documentation.

## Design Direction
StrataDB is being built incrementally from correctness-critical internals outward.
The order is intentional: verify concurrency and memory foundations first, then layer in write path, durability, storage, and operational tooling.
That sequencing keeps higher layers anchored to already-validated lower-level guarantees.

## Notes
- The Adaptive Radix Tree (ART) path is currently treated as experimental and is expected after the core phased roadmap is substantially complete.
- For implementation-level details, use the architecture documents listed above.
