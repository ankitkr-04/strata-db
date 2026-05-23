#pragma once

#include "stratadb/config/immutable/numa_config.hpp"
#include "stratadb/config/immutable/page_config.hpp"
#include "stratadb/utils/bytes.hpp"

#include <cstddef>
#include <optional>

namespace stratadb::config {

struct MemoryConfig {
    static constexpr std::size_t DEFAULT_TOTAL_BUDGET_BYTES = 1UZ * stratadb::utils::bytes::GiB;
    static constexpr std::size_t DEFAULT_TLAB_SIZE_BYTES = 32UZ * stratadb::utils::bytes::MiB;

    PageStrategy page_strategy{PageStrategy::Huge2M_Opportunistic};
    NumaPolicy numa_policy{NumaPolicy::UMA}; // Default UMA for embedded safety
    bool prefault_on_init{false};

    std::size_t total_budget_bytes{DEFAULT_TOTAL_BUDGET_BYTES};
    std::size_t tlab_size_bytes{DEFAULT_TLAB_SIZE_BYTES};

    // Allocator alignment for O_DIRECT compatibility.
    // nullopt = autodetect: queries system_page_size() at Arena initialization.
    // Note: independent of ImmutableConfig::block_size_bytes (SSTable layout).
    std::optional<std::size_t> block_alignment_bytes{std::nullopt};
};

} // namespace stratadb::config