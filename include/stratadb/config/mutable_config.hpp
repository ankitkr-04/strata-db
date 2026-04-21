#pragma once

#include "stratadb/config/memtable_config.hpp"

namespace stratadb::config {

struct MutableConfig {
    MemTableConfig memtable;

    int background_compaction_threads{2};
};

} // namespace stratadb::config
