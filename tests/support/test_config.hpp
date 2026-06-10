#pragma once

#include "stratadb/config/config_resolver.hpp"
#include "stratadb/config/immutable_config.hpp"
#include "stratadb/config/mutable_config.hpp"

#include <cstddef>
#include <limits>

namespace stratadb::test {

[[nodiscard]] inline auto test_memory_config(std::size_t total_budget = 32ULL << 20, // Default: 32 MiB
                                             std::size_t tlab_sz = 2ULL << 20        // Default: 2 MiB
                                             ) -> config::MemoryConfig {
    config::MemoryConfig cfg{};
    cfg.total_budget_bytes = total_budget;
    cfg.tlab_size_bytes = (tlab_sz == 0) ? 4096 : tlab_sz;

    cfg.page_strategy = config::PageStrategy::Standard4K;
    cfg.block_alignment_bytes = 4096;
    cfg.large_alloc_tlab_fraction = 8;
    return cfg;
}

[[nodiscard]] inline auto test_block_pool_config(std::size_t capacity = 16384, std::size_t block_sz = 4096)
    -> config::BlockPoolConfig {
    config::BlockPoolConfig cfg{};
    cfg.capacity = capacity;
    cfg.block_size_bytes = block_sz;
    cfg.payload_alignment_bytes = 4096;
    return cfg;
}

[[nodiscard]] inline auto test_memtable_config(std::size_t max_size = std::numeric_limits<std::size_t>::max())
    -> config::MemTableConfig {
    config::MemTableConfig cfg{};
    cfg.max_size_bytes = max_size;
    cfg.flush_trigger_bytes = max_size;
    cfg.stall_trigger_bytes = max_size;
    return cfg;
}

[[nodiscard]] inline auto test_wal_config(std::size_t slot_size = 256ULL * 1024) -> config::WalConfig {
    config::WalConfig cfg{};
    cfg.slot_size_bytes = slot_size;
    cfg.preallocated_pool_size = 1;
    cfg.spsc.mode = config::SpscMode::Disabled;
    return cfg;
}

[[nodiscard]] inline auto test_immutable_config() -> config::ImmutableConfig {
    config::ImmutableConfig cfg{};
    cfg.block_pool = test_block_pool_config(64, 32768); // Small test pool
    auto resolved = config::ConfigResolver::resolve_immutable(cfg);
    return resolved.value_or(cfg);
}

[[nodiscard]] inline auto test_mutable_config() -> config::MutableConfig {
    config::MutableConfig cfg{};
    cfg.memtable = test_memtable_config();
    cfg.background_compaction_threads = 1;
    return cfg;
}

} // namespace stratadb::test