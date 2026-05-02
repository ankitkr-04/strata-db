#pragma once

#include "stratadb/memtable/skiplist_node.hpp"
#include "stratadb/utils/hardware.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <string>
#include <thread>
#include <vector>

namespace stratadb::bench {

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

struct LatencySummary {
    double p50_ns{0.0};
    double p99_ns{0.0};
};

[[nodiscard]] inline auto summarize_latencies(std::vector<double> samples) -> LatencySummary {
    if (samples.empty()) {
        return {};
    }

    const auto percentile = [&](double q) {
        const auto size = samples.size();
        const auto idx = static_cast<std::size_t>(q * static_cast<double>(size - 1));
        std::nth_element(samples.begin(), samples.begin() + static_cast<std::ptrdiff_t>(idx), samples.end());
        return samples[idx];
    };

    return {
        .p50_ns = percentile(0.50),
        .p99_ns = percentile(0.99),
    };
}

[[nodiscard]] inline auto benchmark_thread_cap() noexcept -> int {
    constexpr int kDefaultThreadCap = 16;

    int cap = kDefaultThreadCap;

    if (const char* raw_limit = std::getenv("STRATADB_BENCH_MAX_THREADS")) {
        char* end = nullptr;
        const long parsed = std::strtol(raw_limit, &end, 10);
        if (end != raw_limit && *end == '\0' && parsed > 0 && parsed <= std::numeric_limits<int>::max()) {
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

class OneShotStartGate {
  public:
    explicit OneShotStartGate(int participants) noexcept
        : participants_(participants) {}

    void arrive_and_wait() noexcept {
        const int prior = ready_.fetch_add(1, std::memory_order_acq_rel);
        if (prior + 1 == participants_) {
            start_.store(true, std::memory_order_release);
            return;
        }

        while (!start_.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }

  private:
    int participants_{0};
    std::atomic<int> ready_{0};
    std::atomic<bool> start_{false};
};

} // namespace stratadb::bench
