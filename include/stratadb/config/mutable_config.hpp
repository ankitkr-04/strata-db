#pragma once

#include "thresholds.hpp"
#include "wal_policy.hpp"

namespace stratadb::config {

struct MutableConfig {
    MemTableThresholds memtable;

    int background_compaction_threads{2};

    WalConfig wal;
};

} // namespace stratadb::config