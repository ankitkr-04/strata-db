# Benchmarks

## Last Updated
2026-05-24

## Change Summary
- 2026-05-24: Synced benchmark guidance with the current script-driven workflow and benchmark targets.

## Overview

### What is here

| File | Responsibility |
|---|---|
| `benchmarks/benchmark_common.hpp` | Shared profiles, key generators, latency summarizers, thread helpers |
| `benchmarks/allocator_bench.cpp` | `new` vs Arena vs Arena+TLAB throughput and latency |
| `benchmarks/tlab_bench.cpp` | CAS contention isolation: cursor-per-alloc vs TLAB refill amortisation |
| `benchmarks/page_bench.cpp` | 4 K vs 2 MB page scan; prefault startup vs first-write cost |
| `benchmarks/memtable_bench.cpp` | Full SkipListMemTable suite — put, get, remove, scan, mixed R/W, scaling |

### What is not here

- Shell scripts inside `benchmarks/` — use `scripts/bench.sh` for all operations.
- Per-benchmark doc files — this file covers everything.
- Result files — raw outputs go to `perf/` which is gitignored.

### Adding a new benchmark

1. Add `benchmarks/<subsystem>_bench.cpp`.
2. Register with `add_stratadb_benchmark(<name> benchmarks/<subsystem>_bench.cpp)` in `CMakeLists.txt`.
3. Add a row to the file table above.
4. `scripts/bench.sh` auto-discovers `*_bench` binaries — no script changes needed.

---

## Build prerequisites

| Requirement | Why |
|---|---|
| `CMAKE_BUILD_TYPE=Release` | `-O3 -march=native` applied by benchmark flag target |
| All sanitizers **OFF** | Sanitizer overhead invalidates throughput and latency numbers |
| `STRATADB_BUILD_BENCHMARKS=ON` | CMake gate that materializes benchmark executables |

The `CMakeLists.txt` gate enforces this. Benchmark binaries simply do not appear in Debug or sanitizer builds.

---

## Using the script

All operations go through `scripts/bench.sh`. It builds if needed and auto-discovers binaries.

```bash
# Build + run everything
./scripts/bench.sh

# Run a specific benchmark binary
./scripts/bench.sh --target memtable

# Narrow to a specific benchmark with a filter
./scripts/bench.sh --target memtable --filter 'BM_MemTablePut/1/1/0'

# Wrap with linux perf stat
./scripts/bench.sh --target memtable --perf

# Generate a flamegraph SVG (requires STRATADB_ENABLE_PROFILING=ON)
./scripts/bench.sh --target memtable --flamegraph --filter 'BM_MemTableMixedRW/8/80/1'

# Wrap with valgrind/callgrind
./scripts/bench.sh --target allocator --valgrind

# TSAN run (rebuilds in Debug+TSAN, runs test suite — not benchmark binaries)
./scripts/bench.sh --tsan

# See all flags
./scripts/bench.sh --help
```

Outputs land in `perf/` with the naming convention `perf/<target>_<context>.<ext>`:

| Output | Example |
|---|---|
| Text results | `perf/memtable_put_1t.txt` |
| Flamegraph SVG | `perf/memtable_mixed_8t.svg` |
| raw perf record | `perf/memtable_scan.perf.data` |
| Callgrind output | `perf/allocator_tlab.callgrind.out` |

`perf/` is gitignored. Never commit anything from there.

---

## Common recipes

### Write throughput — 1 thread, small KV, sequential keys

```bash
./scripts/bench.sh --target memtable --filter 'BM_MemTablePut/1/1/0' --min-time 1s
```

### Read throughput — 8 threads, 100 % hit rate

```bash
./scripts/bench.sh --target memtable --filter 'BM_MemTableGet/8/1/100' --min-time 1s
```

### Thread-scaling write curve

```bash
./scripts/bench.sh --target memtable --filter 'BM_MemTablePutScaling' --min-time 1s
```

### Concurrent mixed R/W

```bash
./scripts/bench.sh --target memtable --filter 'BM_MemTableMixedRW' --min-time 1s
```

### Forward scan — page strategy comparison

```bash
./scripts/bench.sh --target memtable --filter 'BM_MemTableScan' --min-time 1s
```

### CAS contention — TLAB vs shared Arena cursor

```bash
./scripts/bench.sh --target tlab --perf
```

### Allocator comparison headline

```bash
./scripts/bench.sh --target allocator --perf
```

---

## perf stat recipes (manual)

When you need a specific hardware event set not covered by the script flags:

**Cache misses during point lookups:**
```bash
perf stat -e cache-misses,dTLB-load-misses,cycles,instructions \
  ./build/memtable_bench \
  --benchmark_filter='BM_MemTableGet/1/1/100' \
  --benchmark_min_time=1s
```

**CAS contention — 16 threads, hotspot distribution:**
```bash
perf stat -e cycles,instructions,bus-cycles \
  ./build/memtable_bench \
  --benchmark_filter='BM_MemTablePut/16/1/2' \
  --benchmark_min_time=1s
```

**Scan TLB miss rate:**
```bash
perf stat -e dTLB-load-misses,dTLB-loads \
  ./build/memtable_bench \
  --benchmark_filter='BM_MemTableScan.*512' \
  --benchmark_min_time=2s
```

---

## TSAN validation

Benchmark binaries are never built under TSAN. Concurrency correctness lives in the test suite.

```bash
./scripts/bench.sh --tsan \
  --gtest-filter='*SkipList*Concurrent*:*SkipList*Sequence*'
```

Or manually:

```bash
cmake -S . -B build/tsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DSTRATADB_ENABLE_TSAN=ON \
  -DSTRATADB_BUILD_BENCHMARKS=OFF

cmake --build build/tsan --target stratadb_tests -j

./build/tsan/stratadb_tests \
  --gtest_filter='*SkipList*Concurrent*:*SkipList*Sequence*'
```

---

## Parameter reference

### Range encoding

| Benchmark | `range(0)` | `range(1)` | `range(2)` |
|---|---|---|---|
| `BM_MemTablePut` | threads | profile_idx | key_dist |
| `BM_MemTableGet` | threads | profile_idx | hit_pct |
| `BM_MemTableRemove` | threads | profile_idx | — |
| `BM_MemTableScan` | page strategy (0=4K, 1=2MB) | node_count_k | — |
| `BM_MemTableMixedRW` | threads | read_pct | profile_idx |
| `BM_MemTablePutScaling` | threads | 1 (small fixed) | 1 (uniform fixed) |
| `BM_NewAllocate` | threads | profile_idx | — |
| `BM_ArenaAllocate` | threads | profile_idx | — |
| `BM_TLABAllocate` | threads | profile_idx | — |

### KV profile index

| idx | name | key | value | representative workload |
|---|---|---|---|---|
| 0 | tiny | 8 B | 32 B | counters / flags |
| 1 | small | 24 B | 128 B | session keys / web KV |
| 2 | medium | 64 B | 512 B | document fragments |
| 3 | large | 128 B | 2 KiB | serialised protos / blobs |

### Key distribution

| code | name | behaviour |
|---|---|---|
| 0 | seq | monotonically increasing — best-case cache locality |
| 1 | rnd | iid uniform over key space |
| 2 | hot | 80 % of ops hit 20 % of keys (Pareto) |

### Output counters

| counter | meaning |
|---|---|
| `items/s` | operations per second across all threads |
| `bytes/s` | key + value payload bytes per second |
| `p50_ns` | median per-op latency (nanoseconds) |
| `p95_ns` | 95th-percentile latency |
| `p99_ns` | 99th-percentile latency |
| `put_ops` | (mixed) successful inserts |
| `get_ops` | (mixed) successful lookups |
| `oom_hits` | (mixed) OOM allocation events |
| `total_atomic_ops` | (tlab) total CAS attempts |
| `failed_cas_retries` | (tlab) failed CAS attempts |
| `atomic_ops_per_alloc` | (tlab) CAS operations per allocation |

---

## Anti-patterns

| Do not | Why |
|---|---|
| Add a shell script inside `benchmarks/` | Instructions and scripts are not source code |
| Create `docs/benchmarks/<subsystem>.md` per-benchmark files | This file covers all benchmarks; scatter = bitrot |
| Commit anything from `perf/` | Machine-specific, no place in version history |
| Run benchmark binaries under sanitizers | Overhead invalidates the numbers |
| Hardcode binary names in `scripts/bench.sh` | Auto-discovery means new benchmarks cost nothing to add |