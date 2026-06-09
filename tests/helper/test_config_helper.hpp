#pragma once

#include "stratadb/config/immutable/memory_config.hpp"
#include "stratadb/config/mutable/memtable_config.hpp"

#include <cstddef>
#include <limits>

namespace stratadb::helper {

[[nodiscard]] inline auto make_test_memory_config(std::size_t total = 32ULL << 20, std::size_t tlab_sz = 2ULL << 20)
    -> config::MemoryConfig {

    config::MemoryConfig cfg;
    cfg.total_budget_bytes = total;
    cfg.tlab_size_bytes = (tlab_sz == 0) ? 4096 : tlab_sz;
    cfg.page_strategy = config::PageStrategy::Standard4K;

    cfg.block_alignment_bytes = 4096;
    cfg.large_alloc_tlab_fraction = 8;

    return cfg;
}

[[nodiscard]] inline auto make_test_memtable_config(std::size_t max = std::numeric_limits<std::size_t>::max())
    -> config::MemTableConfig {

    config::MemTableConfig cfg;
    cfg.max_size_bytes = max;
    cfg.flush_trigger_bytes = max;
    cfg.stall_trigger_bytes = max;
    return cfg;
}

} // namespace stratadb::helper