#pragma once

#include "stratadb/config/memtable_config.hpp"
#include "stratadb/utils/hardware.hpp"

#include <algorithm>
#include <cstdint>

namespace stratadb::config {

struct MutableConfig {
    [[nodiscard]]
    static auto default_compaction_threads() -> std::uint32_t {
        return std::clamp(utils::logical_core_count() / 4u, 1u, 8u);
    }
    MemTableConfig memtable;

    std::uint32_t background_compaction_threads{default_compaction_threads()};
};

} // namespace stratadb::config
