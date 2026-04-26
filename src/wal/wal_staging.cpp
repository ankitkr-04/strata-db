
#include "stratadb/wal/wal_staging.hpp"

#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/epoch_manager.hpp"
#include "stratadb/memory/tlab.hpp"

#include <cstring>
#include <memory>
namespace stratadb::wal {
template <std::size_t BlockSize>
thread_local WalBlock<BlockSize>* WalStaging<BlockSize>::tls_current_block_ = nullptr;

// Thread local tlab
thread_local std::unique_ptr<memory::TLAB> tls_tlab = nullptr;

template <std::size_t BlockSize>
auto WalStaging<BlockSize>::stage_write(std::uint64_t sequence_id,
                                        // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
                                        std::span<const std::byte> key,
                                        std::span<const std::byte> value) noexcept -> bool {

    memory::EpochManager::ReadGuard guard(epoch_manager_);
    const std::size_t kv_size = key.size_bytes() + value.size_bytes();

    if (tls_tlab == nullptr) [[unlikely]] {
        tls_tlab = std::make_unique<memory::TLAB>(staging_arena_);
    }

    if (tls_current_block_ != nullptr) {
        std::size_t remaining_space =
            WalBlock<BlockSize>::PAYLOAD_BYTES - tls_current_block_->header.payload_bytes_written;
        if (kv_size > remaining_space) {
            // Not enough space in the current block, need to harvest it before staging the new record
            tls_current_block_ = nullptr; // Clear the current block to trigger harvesting in the next step
        }
    }

    if (tls_current_block_ == nullptr) {
        // Need to acquire a new block from the TLAB
        void* block_memory = tls_tlab->allocate(sizeof(WalBlock<BlockSize>), BlockSize);
        if (block_memory == nullptr) [[unlikely]] {
            return false; // Allocation failed, staging failed
        }
        // Placement new zeroes out padding and fields safely
        tls_current_block_ = new (block_memory) WalBlock<BlockSize>();
        // Stamp the epoch
        tls_current_block_->header.epoch_number = epoch_manager_.current_epoch();
    }

    // Memory Copy
    auto* payload_base = tls_current_block_->payload.data;
    auto current_offset = tls_current_block_->header.payload_bytes_written;
    auto current_record = tls_current_block_->header.num_records;

    // Copy key
    std::memcpy(payload_base + current_offset, key.data(), key.size_bytes());
    current_offset += key.size_bytes();

    // Copy value
    std::memcpy(payload_base + current_offset, value.data(), value.size_bytes());
    current_offset += value.size_bytes();

    // update header
    tls_current_block_->header.payload_bytes_written = current_offset;
    tls_current_block_->header.sequence_id = sequence_id;
    tls_current_block_->metadata.key_lengths[current_record] = static_cast<std::uint16_t>(key.size_bytes());
    tls_current_block_->header.num_records++;

    return true;
};

template <std::size_t BlockSize>
auto WalStaging<BlockSize>::harvest_ready_blocks() noexcept -> void {
    std::vector<WalBlock<BlockSize>*> blocks_to_flush;
    {
        std::lock_guard<std::mutex> lock(handoff_mutex_);
        blocks_to_flush.swap(ready_blocks_);
    }

    if (blocks_to_flush.empty()) {
        return; // No blocks to flush
    }

    // send to IO_URING
};

template class WalStaging<SectorSize::LegacyHDD>;
template class WalStaging<SectorSize::StandardNVMe>;
template class WalStaging<SectorSize::AdvancedFormat>;
template class WalStaging<SectorSize::EnterpriseNVMe>;

} // namespace stratadb::wal