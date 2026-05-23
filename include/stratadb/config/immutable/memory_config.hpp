#pragma once

#include "stratadb/config/immutable/numa_config.hpp"
#include "stratadb/config/immutable/page_config.hpp"
#include "stratadb/utils/bytes.hpp"

#include <cstddef>

namespace stratadb::config {

struct MemoryConfig {

    static constexpr std::size_t DEFAULT_TOTAL_BUDGET_BYTES = 1UZ * stratadb::utils::bytes::GiB;
    static constexpr std::size_t DEFAULT_TLAB_SIZE_BYTES = 32UZ * stratadb::utils::bytes::MiB;
    static constexpr std::size_t DEFAULT_LARGE_ALLOC_TLAB_FRACTION = 2UZ;

    PageStrategy page_strategy{PageStrategy::Huge2M_Opportunistic};
    NumaPolicy numa_policy{NumaPolicy::UMA}; // UMA default: safe on embedded / non-NUMA hardware

    bool prefault_on_init{false};

    std::size_t total_budget_bytes{DEFAULT_TOTAL_BUDGET_BYTES};
    std::size_t tlab_size_bytes{DEFAULT_TLAB_SIZE_BYTES};

    // Allocator alignment for O_DIRECT compatibility.
    // 0 = auto: ConfigResolver fills this from system_page_size() before any
    // Arena is constructed. Downstream components may rely on the value being
    // non-zero after resolution.
    // Note: independent of ImmutableConfig::block_size_bytes (SSTable layout).
    std::size_t block_alignment_bytes{0};

    // Allocations >= (tlab_size_bytes / large_alloc_tlab_fraction) bypass the
    // TLAB and go directly to the Arena. Resolver enforces: must be > 0.
    std::size_t large_alloc_tlab_fraction{DEFAULT_LARGE_ALLOC_TLAB_FRACTION};
};

} // namespace stratadb::config