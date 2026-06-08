// benchmarks/wal_ring_bench.cpp
//
// SSD WAL-shape experiment:
//   - BM_WalCircularRingDirect: write 10 GiB by repeatedly overwriting one
//     preallocated 64 MiB file through O_DIRECT.
//   - BM_WalSegmentAppendDirect: write 10 GiB by creating fresh 64 MiB segment
//     files, writing each sequentially through O_DIRECT, and deleting old ones.
//
// This benchmark is intentionally opt-in. Run it on a scratch filesystem:
//
//   STRATADB_WAL_RING_BENCH_DIR=/mnt/nvme_scratch/stratadb-wal \
//     ./scripts/bench.sh --target wal_ring --min-time 1s
//
// Useful side channels while it runs:
//   iostat -x -y 1 <device>      # -y skips the since-boot first report
//   watch -n 0.5 'cat /mnt/nvme_scratch/stratadb-wal/*/current_pointer.txt'
//
// The benchmark reports host logical bytes and per-pwrite jitter. True NAND WAF
// requires vendor/device counters (for example, nvme smart-log before/after).
// don't try to run on tmps as it may be on a RAM disk and not reflect real device behavior & also cause OOM.

#include "stratadb/io/unique_file_descriptor.hpp"
#include "stratadb/utils/os.hpp"

#include <algorithm>
#include <benchmark/benchmark.h>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#if defined(__linux__) || defined(__APPLE__)
#include <fcntl.h>
#include <unistd.h>
#endif

namespace stratadb::bench {
namespace {

using Clock = std::chrono::steady_clock;

constexpr std::uint64_t kMiB = 1024ULL * 1024ULL;
constexpr std::uint64_t kGiB = 1024ULL * kMiB;
constexpr std::uint64_t kDefaultTotalBytes = 10ULL * kGiB;
constexpr std::uint64_t kDefaultFileBytes = 64ULL * kMiB;
constexpr std::uint64_t kDefaultChunkBytes = 1ULL * kMiB;
constexpr std::uint64_t kDefaultStatusStepBytes = 1ULL * kGiB;
constexpr std::size_t kDirectAlignment = 4096;
constexpr std::size_t kDefaultRetainedSegments = 2;

struct WalBenchConfig {
    std::filesystem::path root;
    std::uint64_t total_bytes{kDefaultTotalBytes};
    std::uint64_t file_bytes{kDefaultFileBytes};
    std::uint64_t chunk_bytes{kDefaultChunkBytes};
    std::uint64_t status_step_bytes{kDefaultStatusStepBytes};
    std::size_t retained_segments{kDefaultRetainedSegments};
    bool keep_files{false};
};

struct WriteStats {
    std::uint64_t bytes_written{0};
    std::uint64_t writes{0};
    std::uint64_t files_created{0};
    std::uint64_t files_deleted{0};
    double max_write_ns{0.0};
    std::vector<double> write_samples_ns;
};

struct LatencyExt {
    double p50_ns{0.0};
    double p95_ns{0.0};
    double p99_ns{0.0};
};

struct FreeDeleter {
    void operator()(std::byte* ptr) const noexcept {
        std::free(ptr);
    }
};

using DirectBuffer = std::unique_ptr<std::byte, FreeDeleter>;

[[nodiscard]] auto summarize_ext(std::vector<double> samples) -> LatencyExt {
    if (samples.empty()) {
        return {};
    }

    const auto percentile = [&](double q) -> double {
        const auto idx = static_cast<std::size_t>(q * static_cast<double>(samples.size() - 1));
        std::nth_element(samples.begin(), samples.begin() + static_cast<std::ptrdiff_t>(idx), samples.end());
        return samples[idx];
    };

    return {
        .p50_ns = percentile(0.50),
        .p95_ns = percentile(0.95),
        .p99_ns = percentile(0.99),
    };
}

[[nodiscard]] auto parse_u64_env(const char* name, std::uint64_t fallback) noexcept -> std::uint64_t {
    const char* raw = std::getenv(name);
    if (!raw || *raw == '\0') {
        return fallback;
    }

    char* end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(raw, &end, 10);
    if (errno != 0 || end == raw || *end != '\0' || parsed == 0ULL) {
        return fallback;
    }
    return static_cast<std::uint64_t>(parsed);
}

[[nodiscard]] auto parse_size_env(const char* name, std::size_t fallback) noexcept -> std::size_t {
    const std::uint64_t parsed = parse_u64_env(name, static_cast<std::uint64_t>(fallback));
    if (parsed > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
        return fallback;
    }
    return static_cast<std::size_t>(parsed);
}

[[nodiscard]] auto env_flag_enabled(const char* name) noexcept -> bool {
    const char* raw = std::getenv(name);
    return raw && (std::string_view(raw) == "1" || std::string_view(raw) == "true" || std::string_view(raw) == "yes");
}

[[nodiscard]] auto config_from_env(std::string& error) -> std::optional<WalBenchConfig> {
    const char* raw_root = std::getenv("STRATADB_WAL_RING_BENCH_DIR");
    if (!raw_root || *raw_root == '\0') {
        error = "set STRATADB_WAL_RING_BENCH_DIR to a scratch directory to enable this destructive SSD benchmark";
        return std::nullopt;
    }

    WalBenchConfig cfg;
    cfg.root = std::filesystem::path(raw_root);
    cfg.total_bytes = parse_u64_env("STRATADB_WAL_RING_BENCH_TOTAL_BYTES", kDefaultTotalBytes);
    cfg.file_bytes = parse_u64_env("STRATADB_WAL_RING_BENCH_FILE_BYTES", kDefaultFileBytes);
    cfg.chunk_bytes = parse_u64_env("STRATADB_WAL_RING_BENCH_CHUNK_BYTES", kDefaultChunkBytes);
    cfg.status_step_bytes = parse_u64_env("STRATADB_WAL_RING_BENCH_STATUS_STEP_BYTES", kDefaultStatusStepBytes);
    cfg.retained_segments = parse_size_env("STRATADB_WAL_RING_BENCH_RETAINED_SEGMENTS", kDefaultRetainedSegments);
    cfg.keep_files = env_flag_enabled("STRATADB_WAL_RING_BENCH_KEEP_FILES");

    const auto is_aligned = [](std::uint64_t value) noexcept -> bool {
        return (value % static_cast<std::uint64_t>(kDirectAlignment)) == 0;
    };

    if (!is_aligned(cfg.total_bytes) || !is_aligned(cfg.file_bytes) || !is_aligned(cfg.chunk_bytes)) {
        error = "total, file, and chunk byte counts must all be 4096-byte aligned for O_DIRECT";
        return std::nullopt;
    }
    if (cfg.chunk_bytes > cfg.file_bytes) {
        error = "STRATADB_WAL_RING_BENCH_CHUNK_BYTES must be <= STRATADB_WAL_RING_BENCH_FILE_BYTES";
        return std::nullopt;
    }
    if (cfg.chunk_bytes > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
        error = "STRATADB_WAL_RING_BENCH_CHUNK_BYTES is too large for this process";
        return std::nullopt;
    }

    return cfg;
}

[[nodiscard]] auto io_error_name(io::IOError error) noexcept -> std::string_view {
    switch (error) {
        case io::IOError::AlignmentViolation:
            return "alignment violation";
        case io::IOError::HardwareError:
            return "hardware error";
        case io::IOError::DeviceFull:
            return "device full";
        case io::IOError::PermissionDenied:
            return "permission denied";
        case io::IOError::UnknownError:
            return "unknown error";
    }
    return "unknown error";
}

[[nodiscard]] auto make_buffer(std::uint64_t bytes, std::string& error) -> DirectBuffer {
    auto* raw = static_cast<std::byte*>(std::aligned_alloc(kDirectAlignment, static_cast<std::size_t>(bytes)));
    if (!raw) {
        error = "failed to allocate aligned O_DIRECT buffer";
        return nullptr;
    }

    for (std::uint64_t i = 0; i < bytes; ++i) {
        raw[i] = static_cast<std::byte>((i * 131ULL) & 0xffULL);
    }
    return DirectBuffer(raw);
}

auto reset_workdir(const std::filesystem::path& path, std::string& error) -> bool {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    if (ec) {
        error = "failed to remove old workdir " + path.string() + ": " + ec.message();
        return false;
    }

    std::filesystem::create_directories(path, ec);
    if (ec) {
        error = "failed to create workdir " + path.string() + ": " + ec.message();
        return false;
    }
    return true;
}

void maybe_cleanup(const std::filesystem::path& path, bool keep_files) noexcept {
    if (keep_files) {
        return;
    }
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
}

class ProgressFile {
  public:
    ProgressFile(std::filesystem::path path, std::uint64_t total_bytes, std::uint64_t step_bytes)
        : path_(std::move(path))
        , total_bytes_(total_bytes)
        , step_bytes_(std::max<std::uint64_t>(step_bytes, kDefaultChunkBytes)) {}

    void
    update(std::string_view mode, std::uint64_t logical_bytes, std::uint64_t file_offset, std::uint64_t file_index) {
        if (logical_bytes < next_report_bytes_ && logical_bytes != total_bytes_) {
            return;
        }
        next_report_bytes_ = logical_bytes + step_bytes_;

        std::ofstream out(path_, std::ios::trunc);
        out << "mode=" << mode << '\n';
        out << "logical_bytes=" << logical_bytes << '\n';
        out << "logical_gib=" << (static_cast<double>(logical_bytes) / static_cast<double>(kGiB)) << '\n';
        out << "file_index=" << file_index << '\n';
        out << "file_offset=" << file_offset << '\n';
    }

  private:
    std::filesystem::path path_;
    std::uint64_t total_bytes_;
    std::uint64_t step_bytes_;
    std::uint64_t next_report_bytes_{0};
};

[[nodiscard]] auto create_preallocated_direct_file(const std::filesystem::path& path,
                                                   std::uint64_t bytes,
                                                   std::string& error) -> std::optional<io::UniqueFd> {
    auto buffered = utils::os::open_buffered(path, /*create=*/true);
    if (!buffered) {
        error = "failed to create buffered file " + path.string() + ": " + std::string(io_error_name(buffered.error()));
        return std::nullopt;
    }

    io::UniqueFd buffered_fd{*buffered};
    auto allocated = utils::os::allocate_file_space(buffered_fd.get(), bytes);
    if (!allocated) {
        error = "failed to preallocate " + path.string() + ": " + std::string(io_error_name(allocated.error()));
        return std::nullopt;
    }
    if (!utils::os::sync_data(buffered_fd.get())) {
        error = "failed to fdatasync preallocated file " + path.string();
        return std::nullopt;
    }
    buffered_fd.reset();

    auto direct = utils::os::open_direct(path);
    if (!direct) {
        error = "failed to reopen " + path.string() + " with O_DIRECT: " + std::string(io_error_name(direct.error()));
        return std::nullopt;
    }
    return io::UniqueFd{*direct};
}

[[nodiscard]] auto create_direct_segment_file(const std::filesystem::path& path, std::string& error)
    -> std::optional<io::UniqueFd> {
#if defined(__linux__) || defined(__APPLE__)
    int flags = O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC;
#if defined(__linux__)
    flags |= O_DIRECT;
#endif

    const int fd = ::open(path.c_str(), flags, 0644);
    if (fd < 0) {
        error = "failed to create direct segment " + path.string() + ": " + std::strerror(errno);
        return std::nullopt;
    }

#if defined(__APPLE__)
    if (::fcntl(fd, F_NOCACHE, 1) < 0) {
        error = "failed to enable F_NOCACHE on " + path.string() + ": " + std::strerror(errno);
        ::close(fd);
        return std::nullopt;
    }
#endif

    return io::UniqueFd{fd};
#else
    error = "direct file creation is only implemented for POSIX platforms";
    return std::nullopt;
#endif
}

auto record_write(
    int fd, const void* buffer, std::uint64_t bytes, std::uint64_t offset, WriteStats& stats, std::string& error)
    -> bool {
    const Clock::time_point t0 = Clock::now();
    auto write_result = utils::os::write_exact(fd, buffer, static_cast<std::size_t>(bytes), offset);
    const double ns = std::chrono::duration<double, std::nano>(Clock::now() - t0).count();

    stats.write_samples_ns.push_back(ns);
    stats.max_write_ns = std::max(stats.max_write_ns, ns);

    if (!write_result) {
        error = "pwrite failed: " + std::string(io_error_name(write_result.error()));
        return false;
    }

    stats.bytes_written += bytes;
    ++stats.writes;
    return true;
}

[[nodiscard]] auto run_circular_ring(const WalBenchConfig& cfg, std::string& error) -> std::optional<WriteStats> {
    const std::filesystem::path workdir = cfg.root / "circular_ring";
    if (!reset_workdir(workdir, error)) {
        return std::nullopt;
    }

    auto buffer = make_buffer(cfg.chunk_bytes, error);
    if (!buffer) {
        return std::nullopt;
    }

    auto fd = create_preallocated_direct_file(workdir / "wal-ring-000000.dat", cfg.file_bytes, error);
    if (!fd) {
        return std::nullopt;
    }

    WriteStats stats;
    stats.files_created = 1;
    stats.write_samples_ns.reserve(static_cast<std::size_t>(cfg.total_bytes / cfg.chunk_bytes));
    ProgressFile progress(workdir / "current_pointer.txt", cfg.total_bytes, cfg.status_step_bytes);
    progress.update("circular_ring", 0, 0, 0);

    while (stats.bytes_written < cfg.total_bytes) {
        const std::uint64_t remaining = cfg.total_bytes - stats.bytes_written;
        const std::uint64_t bytes = std::min(cfg.chunk_bytes, remaining);
        const std::uint64_t offset = stats.bytes_written % cfg.file_bytes;
        if (!record_write(fd->get(), buffer.get(), bytes, offset, stats, error)) {
            return std::nullopt;
        }
        progress.update("circular_ring", stats.bytes_written, offset + bytes, 0);
    }

    if (!utils::os::sync_data(fd->get())) {
        error = "failed to fdatasync circular ring file";
        return std::nullopt;
    }

    fd->reset();
    maybe_cleanup(workdir, cfg.keep_files);
    return stats;
}

[[nodiscard]] auto run_segment_append(const WalBenchConfig& cfg, std::string& error) -> std::optional<WriteStats> {
    const std::filesystem::path workdir = cfg.root / "segment_append";
    if (!reset_workdir(workdir, error)) {
        return std::nullopt;
    }

    auto buffer = make_buffer(cfg.chunk_bytes, error);
    if (!buffer) {
        return std::nullopt;
    }

    WriteStats stats;
    stats.write_samples_ns.reserve(static_cast<std::size_t>(cfg.total_bytes / cfg.chunk_bytes));
    std::vector<std::filesystem::path> live_segments;
    live_segments.reserve(cfg.retained_segments + 1);
    ProgressFile progress(workdir / "current_pointer.txt", cfg.total_bytes, cfg.status_step_bytes);
    progress.update("segment_append", 0, 0, 0);

    std::uint64_t segment_index = 0;
    while (stats.bytes_written < cfg.total_bytes) {
        const std::string filename = "wal-segment-" + std::to_string(segment_index) + ".dat";
        const std::filesystem::path segment_path = workdir / filename;
        auto fd = create_direct_segment_file(segment_path, error);
        if (!fd) {
            return std::nullopt;
        }

        ++stats.files_created;
        live_segments.push_back(segment_path);

        std::uint64_t file_offset = 0;
        while (file_offset < cfg.file_bytes && stats.bytes_written < cfg.total_bytes) {
            const std::uint64_t remaining_total = cfg.total_bytes - stats.bytes_written;
            const std::uint64_t remaining_file = cfg.file_bytes - file_offset;
            const std::uint64_t bytes = std::min({cfg.chunk_bytes, remaining_total, remaining_file});
            if (!record_write(fd->get(), buffer.get(), bytes, file_offset, stats, error)) {
                return std::nullopt;
            }
            file_offset += bytes;
            progress.update("segment_append", stats.bytes_written, file_offset, segment_index);
        }

        if (!utils::os::sync_data(fd->get())) {
            error = "failed to fdatasync segment " + segment_path.string();
            return std::nullopt;
        }
        fd->reset();

        while (live_segments.size() > cfg.retained_segments) {
            std::error_code ec;
            std::filesystem::remove(live_segments.front(), ec);
            if (ec) {
                error = "failed to delete old segment " + live_segments.front().string() + ": " + ec.message();
                return std::nullopt;
            }
            live_segments.erase(live_segments.begin());
            ++stats.files_deleted;
        }

        ++segment_index;
    }

    maybe_cleanup(workdir, cfg.keep_files);
    return stats;
}

void publish_counters(benchmark::State& state, const WalBenchConfig& cfg, const WriteStats& stats) {
    const LatencyExt latency = summarize_ext(stats.write_samples_ns);
    state.SetBytesProcessed(static_cast<std::int64_t>(stats.bytes_written));
    state.SetItemsProcessed(static_cast<std::int64_t>(stats.writes));
    state.counters["logical_gib"] = static_cast<double>(stats.bytes_written) / static_cast<double>(kGiB);
    state.counters["file_mib"] = static_cast<double>(cfg.file_bytes) / static_cast<double>(kMiB);
    state.counters["chunk_mib"] = static_cast<double>(cfg.chunk_bytes) / static_cast<double>(kMiB);
    state.counters["writes"] = static_cast<double>(stats.writes);
    state.counters["files_created"] = static_cast<double>(stats.files_created);
    state.counters["files_deleted"] = static_cast<double>(stats.files_deleted);
    state.counters["p50_write_us"] = latency.p50_ns / 1000.0;
    state.counters["p95_write_us"] = latency.p95_ns / 1000.0;
    state.counters["p99_write_us"] = latency.p99_ns / 1000.0;
    state.counters["max_write_us"] = stats.max_write_ns / 1000.0;
    state.counters["p99_to_p50"] = latency.p50_ns == 0.0 ? 0.0 : latency.p99_ns / latency.p50_ns;
    state.counters["max_to_p50"] = latency.p50_ns == 0.0 ? 0.0 : stats.max_write_ns / latency.p50_ns;
}

template <typename RunFn>
void run_wal_bench(benchmark::State& state, RunFn run) {
    std::string error;
    const auto cfg = config_from_env(error);
    if (!cfg) {
        state.SkipWithError(error.c_str());
        return;
    }

    for (auto _ : state) {
        auto stats = run(*cfg, error);
        if (!stats) {
            state.SkipWithError(error.c_str());
            break;
        }
        publish_counters(state, *cfg, *stats);
    }
}

static void BM_WalCircularRingDirect(benchmark::State& state) {
    run_wal_bench(state, run_circular_ring);
}

static void BM_WalSegmentAppendDirect(benchmark::State& state) {
    run_wal_bench(state, run_segment_append);
}

} // namespace
} // namespace stratadb::bench

BENCHMARK(stratadb::bench::BM_WalCircularRingDirect)->UseRealTime()->Iterations(1)->Repetitions(1);
BENCHMARK(stratadb::bench::BM_WalSegmentAppendDirect)->UseRealTime()->Iterations(1)->Repetitions(1);
