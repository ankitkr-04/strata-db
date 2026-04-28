#pragma once

#include "stratadb/memtable/skiplist_node.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
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

[[nodiscard]] inline auto make_ordered_key(std::uint64_t value, std::size_t width) -> std::string {
    std::string key(width, '0');
    auto digits = key.data();
    std::snprintf(digits, width + 1, "%0*llu", static_cast<int>(width), static_cast<unsigned long long>(value));
    if (key.size() > width) {
        key.resize(width);
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
