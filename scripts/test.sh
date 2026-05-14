#!/usr/bin/env bash
# scripts/test.sh
#
# Unified test runner for StrataDB.
# Handles: build, normal run, TSAN, ASAN, UBSAN, gtest filter, build-only.
#
# Usage:
#   ./scripts/test.sh [OPTIONS]
#
# See: docs/tests/README.md

set -euo pipefail

# ─────────────────────────── Defaults ─────────────────────────────────────────
BUILD_DIR="build"
SANITIZER="none"       # none | tsan | asan | ubsan
FILTER=""
BUILD_ONLY=false
SKIP_BUILD=false
JOBS=$(nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 4)

# ─────────────────────────── Usage ────────────────────────────────────────────
usage() {
  cat <<EOF
Usage: ./scripts/test.sh [OPTIONS]

Build and run the StrataDB test suite.

Options:
  --tsan              Build with ThreadSanitizer and run tests
  --asan              Build with AddressSanitizer and run tests
  --ubsan             Build with UBSan and run tests
  --filter <pattern>  GTest --gtest_filter pattern (e.g. '*Arena*', '*Concurrent*')
  --build-only        Build only, do not run tests
  --no-build          Skip build, run existing test binary
  --build-dir <path>  CMake build directory  (default: build)
  -j <n>              Parallel build jobs  (default: nproc)
  -h, --help          Show this help

Examples:
  ./scripts/test.sh
  ./scripts/test.sh --filter '*Arena*'
  ./scripts/test.sh --tsan
  ./scripts/test.sh --tsan --filter '*SkipList*Concurrent*:*SkipList*Sequence*'
  ./scripts/test.sh --asan --filter '*MemTable*'
  ./scripts/test.sh --build-only
EOF
}

# ─────────────────────────── Argument parsing ─────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --tsan)        SANITIZER="tsan";  shift ;;
    --asan)        SANITIZER="asan";  shift ;;
    --ubsan)       SANITIZER="ubsan"; shift ;;
    --filter)      FILTER="$2";       shift 2 ;;
    --build-only)  BUILD_ONLY=true;   shift ;;
    --no-build)    SKIP_BUILD=true;   shift ;;
    --build-dir)   BUILD_DIR="$2";    shift 2 ;;
    -j)            JOBS="$2";         shift 2 ;;
    -h|--help)     usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

# ─────────────────────────── Resolve build dir and CMake flags ────────────────
case "$SANITIZER" in
  tsan)
    BUILD_DIR="${BUILD_DIR}/tsan"
    CMAKE_EXTRA=(-DSTRATADB_ENABLE_TSAN=ON -DCMAKE_BUILD_TYPE=Debug)
    LABEL="TSAN"
    ;;
  asan)
    BUILD_DIR="${BUILD_DIR}/asan"
    CMAKE_EXTRA=(-DSTRATADB_ENABLE_ASAN=ON -DCMAKE_BUILD_TYPE=Debug)
    LABEL="ASAN"
    ;;
  ubsan)
    BUILD_DIR="${BUILD_DIR}/ubsan"
    CMAKE_EXTRA=(-DSTRATADB_ENABLE_UBSAN=ON -DCMAKE_BUILD_TYPE=Debug)
    LABEL="UBSAN"
    ;;
  none)
    CMAKE_EXTRA=(-DCMAKE_BUILD_TYPE=Debug)
    LABEL="Debug"
    ;;
esac

# ─────────────────────────── Build ────────────────────────────────────────────
if [[ "$SKIP_BUILD" == false ]]; then
  echo "==> Configuring (${LABEL}, dir=${BUILD_DIR})..."
  cmake -S . -B "${BUILD_DIR}" -G Ninja \
    -DSTRATADB_BUILD_BENCHMARKS=OFF \
    "${CMAKE_EXTRA[@]}"

  echo "==> Building stratadb_tests..."
  cmake --build "${BUILD_DIR}" --target stratadb_tests -j "${JOBS}"
fi

if [[ "$BUILD_ONLY" == true ]]; then
  echo "==> Build complete (--build-only set, skipping run)."
  exit 0
fi

# ─────────────────────────── Locate test binary ───────────────────────────────
TEST_BIN="${BUILD_DIR}/stratadb_tests"

if [[ ! -x "$TEST_BIN" ]]; then
  echo "Error: Test binary not found at ${TEST_BIN}." >&2
  echo "       Run without --no-build, or check your build directory." >&2
  exit 1
fi

# ─────────────────────────── Run ──────────────────────────────────────────────
CMD=("$TEST_BIN" "--gtest_color=yes")
if [[ -n "$FILTER" ]]; then
  CMD+=("--gtest_filter=${FILTER}")
fi

echo "==> Running tests (${LABEL})${FILTER:+, filter: ${FILTER}}..."
echo "    ${CMD[*]}"
"${CMD[@]}"