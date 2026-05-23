#pragma once

#include "stratadb/config/mutable/memtable_config.hpp"
#include "stratadb/config/mutable/wal_tunable_config.hpp"
#include "stratadb/utils/probe.hpp"

#include <algorithm>
#include <cstdint>

namespace stratadb::config {

struct MutableConfig {
    [[nodiscard]]
    static auto default_compaction_threads() -> std::uint32_t {
        // Computed once; logical_core_count() does a sysconf syscall.
        static const std::uint32_t count =
            std::clamp(utils::logical_core_count() / 4u, std::uint32_t{1}, std::uint32_t{8});
        return count;
    }

    MemTableConfig memtable{};
    WalTuning wal_tuning{};

    std::uint32_t background_compaction_threads{default_compaction_threads()};
};

} // namespace stratadb::config