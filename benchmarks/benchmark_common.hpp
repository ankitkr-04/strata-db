#pragma once

// benchmark_common.hpp
//
// Shared utilities for all StrataDB benchmark binaries.
//
// What lives here:
//   - NodeProfile array and accessors
//   - Key / value payload generators
//   - LatencyExt + summarize_ext  (single definition — was duplicated across files)
//   - BenchmarkConfig + env-var parsing  (change arena size, ops count, page
//     strategy from the shell without recompiling)
//   - make_memory_cfg / make_memtable_cfg  (single definition — was triplicated)
//   - Thread count helpers
//
// What does NOT live here:
//   - File-local WorkerStats structs (field layouts differ per benchmark)
//   - File-local synchronisation primitives (SpinGate, std::barrier usage)
//   - Any Google Benchmark headers (include those in each .cpp)

#include "stratadb/config/memory_config.hpp"
#include "stratadb/config/memtable_config.hpp"
#include "stratadb/memtable/skiplist_node.hpp"
#include "stratadb/utils/hardware.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace stratadb::bench {

// ─────────────────────────────────────────────────────────────────────────────
// Node profiles  (allocator_bench, page_bench)
// ─────────────────────────────────────────────────────────────────────────────

struct NodeProfile {
    const char* name;
    std::size_t key_bytes;
    std::size_t value_bytes;
    std::uint8_t height;

    [[nodiscard]] auto allocation_size() const noexcept -> std::size_t {
        return memtable::SkipListNode::allocation_size(height, key_bytes, value_bytes);
    }
};

inline constexpr std::array<NodeProfile, 4> kNodeProfiles{{
    {.name = "node_128b", .key_bytes = 24, .value_bytes = 48, .height = 4},
    {.name = "node_256b", .key_bytes = 32, .value_bytes = 168, .height = 4},
    {.name = "node_512b", .key_bytes = 48, .value_bytes = 408, .height = 4},
    {.name = "node_1024b", .key_bytes = 64, .value_bytes = 904, .height = 4},
}};

[[nodiscard]] inline auto profile_from_index(int index) noexcept -> const NodeProfile& {
    return kNodeProfiles.at(static_cast<std::size_t>(index));
}

// ─────────────────────────────────────────────────────────────────────────────
// Payload / key generators
// ─────────────────────────────────────────────────────────────────────────────

[[nodiscard]] inline auto make_payload(std::size_t size, char seed) -> std::string {
    return std::string(size, seed);
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] inline auto make_ordered_key(std::uint64_t value, std::size_t width) -> std::string {
    std::string key(width, '0');
    std::array<char, 32> buffer{};
    const auto [ptr, ec] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
    if (ec != std::errc{}) {
        return key;
    }
    const auto digits = static_cast<std::size_t>(ptr - buffer.data());
    if (digits >= width) {
        std::copy(buffer.data() + static_cast<std::ptrdiff_t>(digits - width), ptr, key.begin());
    } else {
        const std::size_t pad = width - digits;
        std::copy(buffer.data(), ptr, key.begin() + static_cast<std::ptrdiff_t>(pad));
    }
    return key;
}

// ─────────────────────────────────────────────────────────────────────────────
// Latency summarization
//
// Previously duplicated as `LatencySummary` (p50/p99 only, unused) and a
// file-local `LatencyExt` in memtable_bench.cpp. Unified here with p50/p95/p99.
// ─────────────────────────────────────────────────────────────────────────────

struct LatencyExt {
    double p50_ns{0.0};
    double p95_ns{0.0};
    double p99_ns{0.0};
};

// Takes samples by value — nth_element mutates the vector in-place.
[[nodiscard]] inline auto summarize_ext(std::vector<double> samples) -> LatencyExt {
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

struct BenchmarkConfig {
    // 0 = use each benchmark file's built-in default.
    std::size_t ops_per_thread{0};

    // 0 = use each benchmark file's built-in headroom constant.
    std::size_t arena_headroom_bytes{0};

    // nullopt = use the benchmark's own default.
    std::optional<config::PageStrategy> page_strategy;
};

[[nodiscard]] inline auto bench_config_from_env() -> BenchmarkConfig {
    BenchmarkConfig cfg;

    // Helper: parse a positive decimal integer from an env var. Returns 0 on
    // parse failure, absent variable, or non-positive value.
    const auto parse_positive_env = [](const char* name) -> std::size_t {
        const char* raw = std::getenv(name);
        if (!raw)
            return 0;
        char* end = nullptr;
        errno = 0;
        const long val = std::strtol(raw, &end, 10);
        if (errno != 0 || end == raw || *end != '\0' || val <= 0)
            return 0;
        return static_cast<std::size_t>(val);
    };

    // ops_per_thread — enforce power-of-two so benchmarks can use (n-1) as a
    // cheap bitmask for latency sampling. Non-powers round up to next power.
    const std::size_t raw_ops = parse_positive_env("STRATADB_BENCH_OPS_PER_THREAD");
    if (raw_ops > 0) {
        cfg.ops_per_thread = std::has_single_bit(raw_ops) ? raw_ops : (std::size_t{1} << std::bit_width(raw_ops));
    }

    // arena_headroom — convert MiB → bytes
    const std::size_t raw_mib = parse_positive_env("STRATADB_BENCH_ARENA_HEADROOM_MIB");
    if (raw_mib > 0) {
        cfg.arena_headroom_bytes = raw_mib * (1ULL << 20);
    }

    // page_strategy
    if (const char* raw = std::getenv("STRATADB_BENCH_PAGE_STRATEGY")) {
        const std::string_view sv(raw);
        if (sv == "standard4k")
            cfg.page_strategy = config::PageStrategy::Standard4K;
        else if (sv == "huge2m")
            cfg.page_strategy = config::PageStrategy::Huge2M_Opportunistic;
        else if (sv == "huge2m_strict")
            cfg.page_strategy = config::PageStrategy::Huge2M_Strict;
        else if (sv == "huge1g")
            cfg.page_strategy = config::PageStrategy::Huge1G_Opportunistic;
        else if (sv == "huge1g_strict")
            cfg.page_strategy = config::PageStrategy::Huge1G_Strict;
        // Unknown value: leave nullopt; benchmark uses its own default.
    }

    return cfg;
}

// Initialised exactly once per process from env vars.
[[nodiscard]] inline auto global_bench_config() -> const BenchmarkConfig& {
    static const BenchmarkConfig cfg = bench_config_from_env();
    return cfg;
}

// Returns the effective ops_per_thread for a benchmark.
// Env-var override wins; falls back to `file_default` when unset.
[[nodiscard]] inline auto effective_ops(std::size_t file_default) noexcept -> std::size_t {
    const std::size_t env_val = global_bench_config().ops_per_thread;
    return env_val != 0 ? env_val : file_default;
}

// Returns the effective arena headroom in bytes.
// Env-var override wins; falls back to `file_default_bytes` when unset.
[[nodiscard]] inline auto effective_headroom(std::size_t file_default_bytes) noexcept -> std::size_t {
    const std::size_t env_val = global_bench_config().arena_headroom_bytes;
    return env_val != 0 ? env_val : file_default_bytes;
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared Arena / MemTable factory functions
//
// Previously each benchmark file had its own make_*_cfg with a slightly
// different name and signature. All three are collapsed here.
// ─────────────────────────────────────────────────────────────────────────────

[[nodiscard]] inline auto make_memory_cfg(std::size_t bytes,
                                          config::PageStrategy strategy = config::PageStrategy::Standard4K,
                                          bool prefault = false) noexcept -> config::MemoryConfig {
    config::MemoryConfig cfg;
    cfg.page_strategy = strategy;
    cfg.prefault_on_init = prefault;
    cfg.total_budget_bytes = bytes;
    cfg.tlab_size_bytes = config::MemoryConfig::DEFAULT_TLAB_SIZE;
    cfg.block_alignment_bytes = config::MemoryConfig::DEFAULT_BLOCK_ALIGNMENT;
    return cfg;
}

// flush_trigger / stall_trigger default to max (never auto-flush) which is
// correct for all benchmarks that own their own Arena lifetime.  page_bench's
// scan fixture passes budget_bytes for both to match its semantics.
[[nodiscard]] inline auto
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
make_memtable_cfg(std::size_t max_size,
                  std::size_t flush_trigger = std::numeric_limits<std::size_t>::max(),
                  std::size_t stall_trigger = std::numeric_limits<std::size_t>::max()) noexcept
    -> config::MemTableConfig {
    config::MemTableConfig cfg;
    cfg.max_size_bytes = max_size;
    cfg.flush_trigger_bytes = flush_trigger;
    cfg.stall_trigger_bytes = stall_trigger;
    return cfg;
}

// ─────────────────────────────────────────────────────────────────────────────
// Thread count helpers
// ─────────────────────────────────────────────────────────────────────────────

[[nodiscard]] inline auto benchmark_thread_cap() noexcept -> int {
    constexpr int kDefaultThreadCap = 16;
    int cap = kDefaultThreadCap;

    if (const char* raw_limit = std::getenv("STRATADB_BENCH_MAX_THREADS")) {
        char* end = nullptr;
        errno = 0;
        const long parsed = std::strtol(raw_limit, &end, 10);
        if (errno == 0 && end != raw_limit && *end == '\0' && parsed > 0 && parsed <= std::numeric_limits<int>::max()) {
            cap = static_cast<int>(parsed);
        }
    }

    const int logical_cores = static_cast<int>(utils::logical_core_count());
    if (logical_cores > 0) {
        cap = std::min(cap, logical_cores);
    }

    return std::max(1, cap);
}

template <typename Fn>
inline void for_each_supported_thread_count(Fn fn) {
    static constexpr std::array<int, 6> kCandidates{1, 2, 4, 8, 16, 32};
    const int cap = benchmark_thread_cap();
    for (const int candidate : kCandidates) {
        if (candidate <= cap) {
            fn(candidate);
        }
    }
}

} // namespace stratadb::bench