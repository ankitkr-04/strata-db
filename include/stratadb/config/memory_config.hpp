#pragma once

#include "stratadb/config/page_config.hpp"
#include "stratadb/utils/bytes.hpp"

#include <cstddef>

namespace stratadb::config {

struct MemoryConfig {

    static constexpr std::size_t DEFAULT_TOTAL_BUDGET = 1 * stratadb::utils::bytes::GiB;
    static constexpr std::size_t DEFAULT_TLAB_SIZE = 32 * stratadb::utils::bytes::MiB;
    // Intentionally independent from ImmutableConfig::DEFAULT_BLOCK_SIZE_BYTES.
    // This controls allocator alignment, not SSTable block layout.
    // 0 signifies autodetect, will query system page size at initialization and align to that if possible.
    static constexpr std::size_t ALIGNMENT_AUTODETECT = 0;

    PageStrategy page_strategy{PageStrategy::Huge2M_Opportunistic};
    NumaPolicy numa_policy{NumaPolicy::UMA}; // Default to UMA for embedded safety
    bool prefault_on_init{false};

    std::size_t total_budget_bytes{DEFAULT_TOTAL_BUDGET};
    std::size_t tlab_size_bytes{DEFAULT_TLAB_SIZE};
    std::size_t block_alignment_bytes{ALIGNMENT_AUTODETECT};
};

} // namespace stratadb::config
