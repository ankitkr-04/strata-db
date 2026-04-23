#pragma once

#include "stratadb/config/page_config.hpp"

#include <cstddef>

namespace stratadb::config {

struct MemoryConfig {
    static constexpr std::size_t DEFAULT_TOTAL_BUDGET = 1024ULL * 1024 * 1024; // 1GB
    static constexpr std::size_t DEFAULT_TLAB_SIZE = 2ULL * 1024 * 1024;       // 2MB
    // Intentionally independent from ImmutableConfig::DEFAULT_BLOCK_SIZE_BYTES.
    // This controls allocator alignment, not SSTable block layout.
    static constexpr std::size_t DEFAULT_BLOCK_ALIGNMENT = 4096;

    PageStrategy page_strategy{PageStrategy::Huge2M_Opportunistic};
    NumaPolicy numa_policy{NumaPolicy::UMA}; // Default to UMA for embedded safety

    std::size_t total_budget_bytes{DEFAULT_TOTAL_BUDGET};
    std::size_t tlab_size_bytes{DEFAULT_TLAB_SIZE};
    std::size_t block_alignment_bytes{DEFAULT_BLOCK_ALIGNMENT};

    bool prefault_on_init{false};
};

} // namespace stratadb::config
