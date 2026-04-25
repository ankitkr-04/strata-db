#pragma once
#include <cstddef>
#include <cstdint>
#include <span>

namespace stratadb::memory {
class EpochManager;
class Arena;
} // namespace stratadb::memory

namespace stratadb::wal {

struct StagedRecord {
    std::uint64_t sequence_id;
    std::uint64_t epoch;
    std::span<const std::byte> key;
    std::span<const std::byte> value;
};
} // namespace stratadb::wal
