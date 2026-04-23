#pragma once

#include "stratadb/config/memtable_config.hpp"

#include <cstdint>

namespace stratadb::config {

struct MutableConfig {
    static constexpr std::uint32_t DEFAULT_BACKGROUND_COMPACTION_THREADS = 2;

    MemTableConfig memtable;

    std::uint32_t background_compaction_threads{DEFAULT_BACKGROUND_COMPACTION_THREADS};
};

} // namespace stratadb::config
