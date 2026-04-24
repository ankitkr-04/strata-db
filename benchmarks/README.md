# Benchmark Runbook

Build benchmarks in `Release` with native tuning:

```bash
cmake -S . -B build/bench -DCMAKE_BUILD_TYPE=Release -DSTRATADB_BUILD_BENCHMARKS=ON
cmake --build build/bench --target allocator_bench tlab_bench page_bench -j
```

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
