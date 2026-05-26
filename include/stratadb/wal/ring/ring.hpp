
#pragma once

#include "stratadb/wal/reader/validator.hpp" // reader::BlockLayout
#include "stratadb/wal/ring/slot_types.hpp"

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <thread>

namespace stratadb::wal::ring {
class WalRing {
  public:
    static constexpr std::uint8_t MAX_RING_CAPACITY = 7U;
    WalRing(std::filesystem::path wal_dir,
            reader::BlockLayout layout,
            uint64_t slot_max_bytes,
            uint8_t ring_capacity,
            uint8_t target_pool_size,
            std::array<uint8_t, 16> db_instance_uuid);

    ~WalRing();

    WalRing(const WalRing&) = delete;
    auto operator=(const WalRing&) -> WalRing& = delete;
    WalRing(WalRing&&) = delete;
    auto operator=(WalRing&&) -> WalRing& = delete;

    // Block until an Active slot is available. On fresh start this waits for
    // the BG thread; on recovery active_index_ is already set.
    void ensure_active_slot() noexcept;

    [[nodiscard]] auto active_fd() const noexcept -> int;
    [[nodiscard]] auto active_write_offset() const noexcept -> uint64_t;
    [[nodiscard]] auto active_sequence() const noexcept -> uint64_t;

    // Fraction of the data region written: [0.0, 1.0].
    [[nodiscard]] auto active_fill_ratio() const noexcept -> float;

    // True when an incoming write would reach or exceed data_end_offset_.
    // Captures overflow AND exact-fit so the Flusher is always the sealer.
    [[nodiscard]] auto needs_rotation(size_t incoming_bytes) const noexcept -> bool;

    // Advance the write cursor after a successful pwritev.
    void advance_write_offset(size_t bytes_written) noexcept;

    // Record the LSN of a successfully written block for the active slot.
    // Updates min_lsn (first block only) and max_lsn (every block).
    // These values are stamped into the Footer at seal time, enabling the
    // GC thread to query the slot's LSN range in O(1) without block parsing.
    // LSN 0 is reserved/invalid — the lsn_generator_ starts at 1.
    void record_block_lsn(uint64_t lsn) noexcept;

    // Seal Active slot (Footer + mandatory fdatasync), then rotate to the
    // next Ready slot. Blocks only if the Ready pool is empty (should not
    // happen with correct target_pool_size, but handled safely).
    void seal_and_rotate() noexcept;

    // Seal Active slot without rotating. Used by flusher_loop exit path
    // (clean shutdown). Does not notify the BG thread.
    void seal_active_slot_only() noexcept;

    // Override the active slot's write_offset. Used by WAL recovery path
    // to set the correct valid-data extent before seal_active_slot_only().
    void set_active_write_offset(uint64_t offset) noexcept;

    // Wake the BG thread to replenish the Ready pool. Non-blocking.
    void notify_precreate() noexcept;

    // Block until at least one Ready slot exists or ring is stopping.
    void wait_for_ready_slot() noexcept;

    // Transitions Sealed → Recyclable for any slot with max_lsn <= checkpoint_lsn.
    // Safe to call from any thread.
    void release_wal_up_to(uint64_t checkpoint_lsn) noexcept;

    [[nodiscard]] auto ready_slot_count() const noexcept -> uint8_t;
    [[nodiscard]] auto slot_path(uint8_t i) const -> std::filesystem::path;

    // Index of the incomplete (unsealed) slot found at startup.
    // UINT8_MAX = no recovery slot (fresh start or clean shutdown).
    [[nodiscard]] auto recovery_slot_index() const noexcept -> uint8_t {
        return recovery_slot_index_;
    };

  private:
    std::filesystem::path wal_dir_;
    reader::BlockLayout layout_;
    uint64_t slot_max_bytes_;
    uint64_t data_end_offset_; // = slot_max_bytes_ - WalSlotFooter::SIZE
    uint64_t data_capacity_;   // = data_end_offset_ - WalSlotHeader::SIZE
    uint8_t ring_capacity_;
    uint8_t target_pool_size_;
    std::array<uint8_t, 16> db_instance_uuid_;

    std::atomic<uint64_t> next_sequence_{1};

    WalSlot slots_[MAX_RING_CAPACITY];
    uint8_t active_index_{UINT8_MAX};
    uint8_t recovery_slot_index_{UINT8_MAX};

    std::atomic<uint8_t> ready_count_{0};

    std::thread bg_thread_{};
    std::mutex mu_{};
    std::condition_variable bg_cv_{};
    std::condition_variable ready_cv_{};
    bool bg_stop_{false};
    std::atomic<bool> bg_needed_{false};

    void precreate_loop() noexcept;
    void create_slot(uint8_t index) noexcept;

    [[nodiscard]] auto find_slot_to_create() const noexcept -> uint8_t;
    [[nodiscard]] auto try_activate_next_ready() noexcept -> bool;
    [[nodiscard]] auto write_and_sync_footer() noexcept -> bool;

    void open_existing_slots() noexcept;
};
} // namespace stratadb::wal::ring
