#pragma once
#include "wal_block.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
namespace stratadb::memory {
class EpochManager;
class Arena;
} // namespace stratadb::memory

namespace stratadb::wal {
namespace SectorSize {
inline constexpr std::size_t LegacyHDD = 512;
inline constexpr std::size_t StandardNVMe = 4096;
inline constexpr std::size_t AdvancedFormat = 8192;
inline constexpr std::size_t EnterpriseNVMe = 16384;
} // namespace SectorSize

struct StagedRecord {
    std::uint64_t sequence_id;
    std::uint64_t epoch;
    std::span<const std::byte> key;
    std::span<const std::byte> value;
};

// WalStaging is the "Waiter". It takes the order from the frontend threads
// and puts it on a clipboard (TLAB) for the background disk thread to pick up
template <std::size_t BlockSize>
class WalStaging {
  public:
    WalStaging(memory::EpochManager& epoch_manager, memory::Arena& staging_arena) noexcept
        : epoch_manager_(epoch_manager)
        , staging_arena_(staging_arena) {}

    // Hot Path
    //  Called by users thread inside db->put()/db->delete() to stage the record for later flush to disk.

    auto stage_write(std::uint64_t sequence_id,
                     std::span<const std::byte> key,
                     std::span<const std::byte> value) noexcept -> bool;

    // Cold Path
    //  Called by internal WAL Orchestrator thread to retrieve the staged record for flushing to disk. This is a
    //  destructive read and will clear the staging area after reading.
    auto harvest_ready_blocks() noexcept -> void;

  private:
    memory::EpochManager& epoch_manager_;
    memory::Arena& staging_arena_;
    static thread_local WalBlock<BlockSize>* tls_current_block_;
};

} // namespace stratadb::wal
