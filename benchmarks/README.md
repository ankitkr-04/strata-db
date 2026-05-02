# Benchmark Runbook

Build benchmarks in `Release` with native tuning:

```bash
cmake -S . -B build/bench -DCMAKE_BUILD_TYPE=Release -DSTRATADB_BUILD_BENCHMARKS=ON
cmake --build build/bench --target allocator_bench tlab_bench page_bench memtable_bench -j
```

---

## allocator_bench — Heap vs Arena vs TLAB

Headline allocator comparison:

```bash
./build/bench/allocator_bench --benchmark_min_time=0.5s
```

Shared atomic cursor contention:

```bash
./build/bench/tlab_bench --benchmark_min_time=0.5s
```

Page-size and prefault benchmarks:

```bash
./build/bench/page_bench --benchmark_min_time=0.5s
```

Recommended `perf stat` runs:

```bash
perf stat -e cache-misses,cache-references,dTLB-load-misses,dTLB-loads,cycles,instructions,branches,branch-misses \
  ./build/bench/allocator_bench --benchmark_filter=BM_TLABAllocate
```

```bash
perf stat -e dTLB-load-misses,dTLB-loads \
  ./build/bench/page_bench --benchmark_filter=BM_SkipListScanPageStrategy
```

Notes:
- `allocator_bench` is the headline chart: baseline `new` vs shared `Arena` cursor vs `Arena + TLAB`.
- `tlab_bench` isolates CAS retries and total atomic operations to show why TLAB refill amortization matters.
- `page_bench` includes both the 4K-vs-2MB scan comparison and the `prefault_on_init` startup versus first-write tradeoff.
- `Huge2M_Strict` will skip on systems that do not have 2MB huge pages configured.

---

## memtable_bench — SkipListMemTable Performance

### Quick smoke test (all benchmarks, reduced time)

```bash
./build/bench/memtable_bench --benchmark_min_time=0.2s
```

### Full suite (use for real measurements)

```bash
./build/bench/memtable_bench --benchmark_min_time=0.5s 2>&1 | tee memtable_results.txt
```

The benchmark sizes its arena from host physical memory and keeps the cap conservative on smaller machines, so it should not assume a fixed 256 MiB budget.

### Focused runs

**Write throughput by key distribution (1 thread, small KV):**

```bash
./build/bench/memtable_bench \
  --benchmark_filter='BM_MemTablePut/1/1/' \
  --benchmark_min_time=1s
```

**Read throughput (hit rate sweep, 8 threads, small KV):**

```bash
./build/bench/memtable_bench \
  --benchmark_filter='BM_MemTableGet/8/1/' \
  --benchmark_min_time=1s
```

**Thread-scaling curve (write throughput vs thread count):**

```bash
./build/bench/memtable_bench \
  --benchmark_filter='BM_MemTablePutScaling' \
  --benchmark_min_time=1s
```

**Concurrent mixed read/write (realistic workload):**

```bash
./build/bench/memtable_bench \
  --benchmark_filter='BM_MemTableMixedRW' \
  --benchmark_min_time=1s
```

**Full forward scan (TLB pressure, huge pages vs 4K):**

```bash
./build/bench/memtable_bench \
  --benchmark_filter='BM_MemTableScan' \
  --benchmark_min_time=1s
```

### perf stat recipes

**Cache misses during point lookups (get hit rate = 100 %):**

```bash
perf stat -e cache-misses,dTLB-load-misses,cycles,instructions \
  ./build/bench/memtable_bench \
  --benchmark_filter='BM_MemTableGet/1/1/100' \
  --benchmark_min_time=1s
```

**CAS contention during concurrent puts (16 threads, hotspot keys):**

```bash
perf stat -e cycles,instructions,bus-cycles \
  ./build/bench/memtable_bench \
  --benchmark_filter='BM_MemTablePut/16/1/2' \
  --benchmark_min_time=1s
```

**Scan TLB miss rate (4K vs 2MB pages, 512K nodes):**

```bash
perf stat -e dTLB-load-misses,dTLB-loads \
  ./build/bench/memtable_bench \
  --benchmark_filter='BM_MemTableScan.*512' \
  --benchmark_min_time=2s
```

### Flamegraph (requires STRATADB_ENABLE_PROFILING=ON)

```bash
cmake -S . -B build/prof \
  -DCMAKE_BUILD_TYPE=Release \
  -DSTRATADB_BUILD_BENCHMARKS=ON \
  -DSTRATADB_ENABLE_PROFILING=ON
cmake --build build/prof --target memtable_bench -j

perf record -g --call-graph dwarf \
  ./build/prof/memtable_bench \
  --benchmark_filter='BM_MemTableMixedRW/8/80/1' \
  --benchmark_min_time=5s

perf script | stackcollapse-perf.pl | flamegraph.pl > memtable_mixed.svg
```

### TSAN validation (concurrency tests)

```bash
cmake -S . -B build/tsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DSTRATADB_ENABLE_TSAN=ON \
  -DSTRATADB_BUILD_BENCHMARKS=OFF

cmake --build build/tsan --target stratadb_tests -j

./build/tsan/stratadb_tests \
  --gtest_filter='*SkipListEdge*Concurrent*:*SkipListEdge*Sequence*'
```

### Benchmark parameter guide

| Range | Meaning | Values |
|-------|---------|--------|
| `range(0)` threads | writer/reader thread count | 1 2 4 8 16 32 |
| `range(1)` profile_idx | KV size profile | 0=tiny(8B/32B) 1=small(24B/128B) 2=medium(64B/512B) 3=large(128B/2KB) |
| `range(2)` key_dist | key access pattern | 0=sequential 1=uniform-random 2=hotspot-80/20 |
| `range(2)` hit_pct (get) | fraction of lookups that find a key | 0 50 100 |
| `range(1)` read_pct (mixed) | percentage of ops that are reads | 50 80 95 |
| `range(1)` node_count_k (scan) | nodes in the memtable × 1024 | 128 512 1024 |

### Counter legend

| Counter | Meaning |
|---------|---------|
| `items/s` | operations per second across all threads |
| `bytes/s` | key+value payload bytes per second |
| `p50_ns` | median per-op wall-clock latency (nanoseconds) |
| `p95_ns` | 95th-percentile latency |
| `p99_ns` | 99th-percentile latency |
| `put_ops` | (mixed) successful inserts |
| `get_ops` | (mixed) successful lookups |
| `oom_hits` | (mixed) allocations that hit arena OOM |