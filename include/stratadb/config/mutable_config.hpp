#pragma once

#include "stratadb/config/memtable_config.hpp"
#include "stratadb/utils/hardware.hpp"

#include <algorithm>
#include <cstdint>

namespace stratadb::config {

struct MutableConfig {
    [[nodiscard]]
    static auto default_compaction_threads() -> std::uint32_t {
        static const std::uint32_t threads =
            std::clamp(utils::logical_core_count() / 4u, std::uint32_t{1}, std::uint32_t{8});
        return threads;
    }
    MemTableConfig memtable;

    std::uint32_t background_compaction_threads{default_compaction_threads()};
};

} // namespace stratadb::config
