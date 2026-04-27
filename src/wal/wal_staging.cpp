
#include "stratadb/wal/wal_staging.hpp"

#include "stratadb/memory/epoch_manager.hpp"
#include "stratadb/memory/tlab.hpp"

#include <cstdio>
#include <cstring>
#include <mutex>
#include <optional>
#include <vector>

namespace stratadb::wal {
namespace {
constexpr std::size_t MAX_DB_INSTANCES = utils::MAX_DB_INSTANCES;
template <std::size_t BlockSize>
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<std::size_t> staging_instance_counter{0};

template <std::size_t BlockSize>
struct ThreadState {
    WalBlock<BlockSize>* current_block{nullptr};
    std::optional<memory::TLAB> tlab{std::nullopt};
};

template <std::size_t BlockSize>
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
thread_local std::array<ThreadState<BlockSize>, MAX_DB_INSTANCES> tl_states;

} // namespace

// Constructor: Assign the instance ID
template <std::size_t BlockSize>
WalStaging<BlockSize>::WalStaging(memory::EpochManager& epoch_manager, memory::Arena& staging_arena) noexcept
    : epoch_manager_(&epoch_manager)
    , staging_arena_(&staging_arena)
    , instance_id_(staging_instance_counter<BlockSize>.fetch_add(1, std::memory_order_relaxed)) {

    if (instance_id_ >= MAX_DB_INSTANCES) {
        std::fputs("StrataDB: Exceeded max WAL instances\n", stderr);
        std::terminate();
    }
}

template <std::size_t BlockSize>
auto WalStaging<BlockSize>::stage_write(std::uint64_t sequence_id,
                                        // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
                                        std::span<const std::byte> key,
                                        std::span<const std::byte> value) noexcept -> bool {

    memory::EpochManager::ReadGuard guard(*epoch_manager_);
    const std::size_t kv_size = key.size_bytes() + value.size_bytes();

    auto& thread_state = tl_states<BlockSize>[instance_id_];

    if (thread_state.tlab == std::nullopt) [[unlikely]] {
        thread_state.tlab.emplace(*staging_arena_);
    }

    if (thread_state.current_block != nullptr) {
        std::size_t remaining_space =
            WalBlock<BlockSize>::PAYLOAD_BYTES - thread_state.current_block->header.payload_bytes_written;

        const bool payload_full = kv_size > remaining_space;
        const bool meta_full = thread_state.current_block->header.num_records >= WalBlock<BlockSize>::MAX_RECORDS;

        if (payload_full || meta_full) {
            {
                std::lock_guard<std::mutex> lock(handoff_mutex_);
                ready_blocks_.push_back(thread_state.current_block);
            }
            thread_state.current_block = nullptr; // Clear the current block to trigger harvesting in the next step
        }
    }

    if (thread_state.current_block == nullptr) {
        // Need to acquire a new block from the TLAB
        void* block_memory = thread_state.tlab->allocate(sizeof(WalBlock<BlockSize>), BlockSize);
        if (block_memory == nullptr) [[unlikely]] {
            return false; // Allocation failed, staging failed
        }
        thread_state.current_block = new (block_memory) WalBlock<BlockSize>();

        auto& header = thread_state.current_block->header;
        header.sequence_id = sequence_id;
        header.epoch_number = epoch_manager_->current_epoch();
        header.payload_bytes_written = 0;
        header.num_records = 0;
        header.physical_lba = 0;
        header.payload_crc32 = 0;
        header.header_crc32 = 0;
    }

    // Memory Copy
    auto* payload_base = thread_state.current_block->payload.data;
    auto current_offset = thread_state.current_block->header.payload_bytes_written;
    auto current_record = thread_state.current_block->header.num_records;

    // Copy key
    std::memcpy(payload_base + current_offset, key.data(), key.size_bytes());
    current_offset += key.size_bytes();

    // Copy value
    std::memcpy(payload_base + current_offset, value.data(), value.size_bytes());
    current_offset += value.size_bytes();

    // update header
    thread_state.current_block->header.payload_bytes_written = current_offset;
    thread_state.current_block->header.sequence_id = sequence_id;
    thread_state.current_block->metadata.key_lengths[current_record] = static_cast<std::uint16_t>(key.size_bytes());
    thread_state.current_block->header.num_records++;

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