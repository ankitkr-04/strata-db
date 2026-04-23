#pragma once

#include <cstdint>

namespace stratadb::memtable {

enum class PutResult : std::uint8_t {
    Ok,
    FlushNeeded,
    StallNeeded,
    OutOfMemory,
};

} // namespace stratadb::memtable
