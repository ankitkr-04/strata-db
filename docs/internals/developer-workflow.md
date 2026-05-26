# Scripts

## Last Updated
2026-05-24

## Change Summary
- 2026-05-24: Synced script documentation to the current `test.sh` and `bench.sh` behavior.

Two scripts cover all developer operations. Everything else is a one-liner CMake command.

```
scripts/
  test.sh     ← build and run the test suite (any sanitizer)
  bench.sh    ← build and run benchmark binaries (perf, flamegraph, valgrind)
```

Make them executable once after cloning:
```bash
chmod +x scripts/test.sh scripts/bench.sh
```

---

## Which script to reach for

| Task | Command |
|---|---|
| Run all tests | `./scripts/test.sh` |
| Run a specific test group | `./scripts/test.sh --filter '*Arena*'` |
| Run tests under TSAN | `./scripts/test.sh --tsan` |
| Run tests under ASAN | `./scripts/test.sh --asan` |
| Run tests under UBSAN | `./scripts/test.sh --ubsan` |
| Just build tests, no run | `./scripts/test.sh --build-only` |
| Run all benchmarks | `./scripts/bench.sh` |
| Run one benchmark binary | `./scripts/bench.sh --target memtable` |
| Run a specific benchmark | `./scripts/bench.sh --target memtable --filter 'BM_MemTablePut/1/1/0'` |
| Benchmark with perf stat | `./scripts/bench.sh --target memtable --perf` |
| Generate a flamegraph | `./scripts/bench.sh --target memtable --flamegraph` |
| Profile with valgrind | `./scripts/bench.sh --target allocator --valgrind` |
| Just build benchmarks, no run | `./scripts/bench.sh --build-only` |

---

## test.sh

```
./scripts/test.sh [OPTIONS]
```

| Flag | Meaning |
|---|---|
| `--tsan` | Rebuild with ThreadSanitizer, run tests |
| `--asan` | Rebuild with AddressSanitizer, run tests |
| `--ubsan` | Rebuild with UBSan, run tests |
| `--filter <pattern>` | GTest `--gtest_filter` pattern |
| `--build-only` | Build `stratadb_tests`, do not run |
| `--no-build` | Skip build, run existing binary |
| `--build-dir <path>` | CMake build directory (default: `build`) |
| `-j <n>` | Parallel build jobs (default: `nproc`) |

Each sanitizer mode builds into its own subdirectory so they do not clobber each other:

| Mode | Build dir |
|---|---|
| normal | `build/` |
| TSAN | `build/tsan/` |
| ASAN | `build/asan/` |
| UBSAN | `build/ubsan/` |

**ASAN and TSAN cannot be combined.** CMake will abort configuration if both are enabled. Use one at a time.

### Examples

```bash
# Run everything
./scripts/test.sh

# Only memtable tests
./scripts/test.sh --filter '*SkipList*'

# Concurrency stress under TSAN
./scripts/test.sh --tsan --filter '*Concurrent*:*Sequence*'

# Build only (useful in CI before running tests separately)
./scripts/test.sh --build-only

# Run without rebuilding (fast iteration when source hasn't changed)
./scripts/test.sh --no-build --filter '*Arena*'
```

---

## bench.sh

```
./scripts/bench.sh [OPTIONS]
```

| Flag | Meaning |
|---|---|
| `--target <name>` | Which benchmark binary: `all`, or name without `_bench` suffix |
| `--filter <pattern>` | Google Benchmark `--benchmark_filter` pattern |
| `--min-time <dur>` | Minimum time per benchmark (default: `0.5s`) |
| `--perf` | Wrap with `perf stat` (hardware event counters) |
| `--flamegraph` | Record with `perf` and generate SVG (requires `STRATADB_ENABLE_PROFILING=ON`) |
| `--valgrind` | Wrap with `valgrind --tool=callgrind` |
| `--build-only` | Build benchmark binaries, do not run |
| `--no-build` | Skip build, run existing binaries |
| `--build-dir <path>` | CMake build directory (default: `build`) |
| `-j <n>` | Parallel build jobs (default: `nproc`) |

**Benchmarks are always built in Release mode.** Sanitizers are never applied to benchmark builds — overhead invalidates the numbers. For concurrency correctness use `test.sh --tsan`.

**New benchmark binaries are auto-discovered.** The script finds every `*_bench` executable in the build directory. Adding a new benchmark requires only a `.cpp` file and a `CMakeLists.txt` entry — no script changes.

### Outputs

All outputs land in `perf/` (gitignored) with a timestamp:

| Mode | Output file |
|---|---|
| `run` | `perf/<target>_<timestamp>.txt` |
| `perf` | `perf/<target>_<timestamp>_perf.txt` |
| `flamegraph` | `perf/<target>_<timestamp>.svg` and `.perf.data` |
| `valgrind` | `perf/<target>_<timestamp>.callgrind` |

### Examples

```bash
# Run all benchmark binaries
./scripts/bench.sh

# Run only the memtable binary
./scripts/bench.sh --target memtable

# Narrow to one benchmark with longer measurement time
./scripts/bench.sh --target memtable --filter 'BM_MemTablePut/1/1/0' --min-time 1s

# Hardware counter profile
./scripts/bench.sh --target memtable --perf

# Flamegraph for a specific mixed workload
./scripts/bench.sh --target memtable --flamegraph --filter 'BM_MemTableMixedRW/8/80/1'

# Callgrind cache simulation on the allocator
./scripts/bench.sh --target allocator --valgrind

# Build benchmarks without running (e.g. to verify compilation)
./scripts/bench.sh --build-only
```

---

## What these scripts do not cover

| Task | How |
|---|---|
| Plain CMake build (no tests, no benchmarks) | `cmake --build build -j` |
| Clean rebuild | `rm -rf build && cmake -S . -B build -G Ninja && cmake --build build -j` |
| Running CTest directly | `ctest --output-on-failure --test-dir build` |
| Viewing callgrind output | `callgrind_annotate perf/<file>.callgrind` or `kcachegrind perf/<file>.callgrind` |

---

## Rules

- Do not add a third script for building alone — `--build-only` covers that.
- Do not add per-subsystem scripts — `--target` and `--filter` cover specialisation.
- Do not commit anything from `perf/` — it is gitignored for a reason.
- Do not run `bench.sh` under a sanitizer build — use `test.sh` for correctness, `bench.sh` for measurement.