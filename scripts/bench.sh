#!/usr/bin/env bash
# scripts/bench.sh
#
# Unified benchmark runner for StrataDB.
# Handles: build, run, perf stat, flamegraph, valgrind/callgrind, build-only.
#
# Usage:
#   ./scripts/bench.sh [OPTIONS]
#
# See: docs/benchmarks/README.md
# For TSAN / concurrency testing use: scripts/test.sh --tsan

set -euo pipefail

# ─────────────────────────── Defaults ─────────────────────────────────────────
BUILD_DIR="build"
TARGET="all"           # all | or any binary name without _bench suffix
FILTER=""
MIN_TIME="0.5s"
MODE="run"             # run | perf | flamegraph | valgrind
BUILD_ONLY=false
SKIP_BUILD=false
JOBS=$(nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 4)

# ─────────────────────────── Usage ────────────────────────────────────────────
usage() {
  cat <<EOF
Usage: ./scripts/bench.sh [OPTIONS]

Build and run StrataDB benchmarks.

Options:
  --target <name>     Benchmark binary to run: all, or name without _bench suffix
                      e.g. --target memtable  runs  build/memtable_bench
                      New benchmarks are auto-discovered; no script change needed.
  --filter <pattern>  Google Benchmark --benchmark_filter pattern
  --min-time <dur>    Minimum time per benchmark, e.g. 0.5s, 1s  (default: 0.5s)
  --build-only        Build only, do not run
  --no-build          Skip build, run existing binaries
  --build-dir <path>  CMake build directory  (default: build)
  -j <n>              Parallel build jobs  (default: nproc)

Profiling modes (pick one):
  --perf              Wrap with perf stat (hardware event counters)
  --flamegraph        Record with perf + generate SVG flamegraph
                      Requires STRATADB_ENABLE_PROFILING=ON build
  --valgrind          Wrap with valgrind --tool=callgrind

  -h, --help          Show this help

Examples:
  ./scripts/bench.sh
  ./scripts/bench.sh --target memtable --filter 'BM_MemTablePut/1/1/0' --min-time 1s
  ./scripts/bench.sh --target memtable --perf
  ./scripts/bench.sh --target memtable --flamegraph --filter 'BM_MemTableMixedRW/8/80/1'
  ./scripts/bench.sh --target allocator --valgrind
  ./scripts/bench.sh --build-only

Outputs land in perf/ (gitignored):
  perf/<target>_<timestamp>.txt          text results
  perf/<target>_<timestamp>.svg          flamegraph SVG
  perf/<target>_<timestamp>.perf.data   raw perf record output
  perf/<target>_<timestamp>.callgrind   callgrind output

For TSAN / concurrency testing use: ./scripts/test.sh --tsan
EOF
}

# ─────────────────────────── Argument parsing ─────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)     TARGET="$2";       shift 2 ;;
    --filter)     FILTER="$2";       shift 2 ;;
    --min-time)   MIN_TIME="$2";     shift 2 ;;
    --build-only) BUILD_ONLY=true;   shift   ;;
    --no-build)   SKIP_BUILD=true;   shift   ;;
    --build-dir)  BUILD_DIR="$2";    shift 2 ;;
    --perf)       MODE="perf";       shift   ;;
    --flamegraph) MODE="flamegraph"; shift   ;;
    --valgrind)   MODE="valgrind";   shift   ;;
    -j)           JOBS="$2";         shift 2 ;;
    -h|--help)    usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

# ─────────────────────────── Helpers ──────────────────────────────────────────
require_cmd() {
  if ! command -v "$1" &>/dev/null; then
    echo "Error: '$1' not found. $2" >&2
    exit 1
  fi
}

timestamp() { date '+%Y%m%d_%H%M%S'; }

# ─────────────────────────── Build ────────────────────────────────────────────
PROFILING_FLAG="OFF"
if [[ "$MODE" == "flamegraph" ]]; then
  PROFILING_FLAG="ON"
  BUILD_DIR="${BUILD_DIR}/prof"
fi

if [[ "$SKIP_BUILD" == false ]]; then
  echo "==> Configuring (Release, profiling=${PROFILING_FLAG}, dir=${BUILD_DIR})..."
  cmake -S . -B "${BUILD_DIR}" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DSTRATADB_BUILD_BENCHMARKS=ON \
    -DSTRATADB_ENABLE_PROFILING="${PROFILING_FLAG}"

  if [[ "$TARGET" == "all" ]]; then
    cmake --build "${BUILD_DIR}" -j "${JOBS}"
  else
    cmake --build "${BUILD_DIR}" --target "${TARGET}_bench" -j "${JOBS}"
  fi
fi

if [[ "$BUILD_ONLY" == true ]]; then
  echo "==> Build complete (--build-only set, skipping run)."
  exit 0
fi

# ─────────────────────────── Auto-discover binaries ───────────────────────────
# No hardcoded names. Any *_bench executable in the build dir is a valid target.
# Adding a new benchmark = add .cpp + CMakeLists entry. Script needs no changes.
mapfile -t ALL_BINARIES < <(find "${BUILD_DIR}" -maxdepth 1 -type f -executable -name '*_bench' | sort)

if [[ ${#ALL_BINARIES[@]} -eq 0 ]]; then
  echo "Error: No *_bench binaries found in ${BUILD_DIR}." >&2
  echo "       Ensure STRATADB_BUILD_BENCHMARKS=ON and CMAKE_BUILD_TYPE=Release." >&2
  exit 1
fi

if [[ "$TARGET" == "all" ]]; then
  BINARIES=("${ALL_BINARIES[@]}")
else
  BINARIES=()
  for bin in "${ALL_BINARIES[@]}"; do
    if [[ "$(basename "$bin")" == "${TARGET}_bench" ]]; then
      BINARIES+=("$bin")
    fi
  done

  if [[ ${#BINARIES[@]} -eq 0 ]]; then
    echo "Error: No binary named '${TARGET}_bench' in ${BUILD_DIR}." >&2
    echo "Available:" >&2
    printf '  %s\n' "${ALL_BINARIES[@]}" >&2
    exit 1
  fi
fi

# ─────────────────────────── Output directory ─────────────────────────────────
mkdir -p perf

# ─────────────────────────── Run ──────────────────────────────────────────────
TS=$(timestamp)

for BIN in "${BINARIES[@]}"; do
  NAME=$(basename "${BIN}" _bench)
  CONTEXT="${NAME}_${TS}"

  BENCH_ARGS=("--benchmark_min_time=${MIN_TIME}" "--benchmark_repetitions=3")
  if [[ -n "$FILTER" ]]; then
    BENCH_ARGS+=("--benchmark_filter=${FILTER}")
  fi

  case "$MODE" in
    run)
      echo "==> Running ${NAME}_bench..."
      "${BIN}" "${BENCH_ARGS[@]}" 2>&1 | tee "perf/${CONTEXT}.txt"
      ;;

    perf)
      require_cmd perf "Install linux-tools-$(uname -r) or equivalent."
      echo "==> perf stat on ${NAME}_bench..."
      perf stat \
        -e cache-misses,cache-references,dTLB-load-misses,dTLB-loads,cycles,instructions,branches,branch-misses \
        "${BIN}" "${BENCH_ARGS[@]}" \
        2>&1 | tee "perf/${CONTEXT}_perf.txt"
      ;;

    flamegraph)
      require_cmd perf                  "Install linux-tools-$(uname -r) or equivalent."
      require_cmd stackcollapse-perf.pl "Install FlameGraph: https://github.com/brendangregg/FlameGraph"
      require_cmd flamegraph.pl         "Install FlameGraph: https://github.com/brendangregg/FlameGraph"

      PERF_DATA="perf/${CONTEXT}.perf.data"
      SVG_OUT="perf/${CONTEXT}.svg"

      echo "==> Recording ${NAME}_bench (output: ${PERF_DATA})..."
      perf record -g --call-graph dwarf -o "${PERF_DATA}" \
        "${BIN}" "${BENCH_ARGS[@]}"

      echo "==> Generating flamegraph (output: ${SVG_OUT})..."
      perf script -i "${PERF_DATA}" \
        | stackcollapse-perf.pl \
        | flamegraph.pl > "${SVG_OUT}"

      echo "==> Flamegraph: ${SVG_OUT}"
      ;;

    valgrind)
      require_cmd valgrind "Install valgrind."
      CALLGRIND_OUT="perf/${CONTEXT}.callgrind"

      echo "==> callgrind on ${NAME}_bench..."
      valgrind \
        --tool=callgrind \
        --callgrind-out-file="${CALLGRIND_OUT}" \
        --cache-sim=yes \
        --branch-sim=yes \
        "${BIN}" "${BENCH_ARGS[@]}"

      echo "==> Callgrind output: ${CALLGRIND_OUT}"
      echo "    View: callgrind_annotate ${CALLGRIND_OUT}"
      echo "    GUI:  kcachegrind ${CALLGRIND_OUT}"
      ;;
  esac
done

echo ""
echo "Done. Outputs in perf/ (gitignored)."