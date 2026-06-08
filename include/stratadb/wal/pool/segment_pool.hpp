#pragma once

#include "stratadb/io/unique_file_descriptor.hpp"
#include "stratadb/wal/pool/segment_state.hpp"
#include "stratadb/wal/reader/validator.hpp"

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <thread>
#include <vector>

namespace stratadb::wal::pool {

// WalSegmentPool — append-only WAL segment pool.
//
// Not a ring in the overwrite sense. Segments are named by a monotonically
// increasing sequence (wal_NNNNNNNNNNNN.log) and physically deleted (unlinked)
// when checkpointed, issuing an SSD TRIM and freeing the LBAs cleanly.
//
// Three asynchronous actors, each with single responsibility:
//
//   Vanguard  (BG thread, precreate_loop)
//     Watches ready_count_ < target_pool_size_.
//     Calls create_segment(): open buffered → fallocate → header → fdatasync →
//     reopen O_DIRECT → sync_dir_entries(dir_fd_) → state = Ready.
//
//   Flusher   (manager's flusher thread)
//     Owns the Active segment exclusively.
//     Writes GammaBlock/DeltaBlock data, advances write_offset.
//     Calls seal_and_rotate() when the segment fills: footer + fdatasync,
//     fd closed, state = Sealed, then activates the next Ready segment.
//
//   GC caller (MemTableFlusher, any thread)
//     Calls release_wal_up_to(checkpoint_lsn): unlinks Sealed files whose
//     max_lsn <= checkpoint_lsn, syncs dir entries, transitions to Empty.
//     Wakes the Vanguard to replenish the pool.
//
// MAX_POOL_CAPACITY bounds the in-memory segment array (fixed, no heap).
// Size it to: 1 Active + 1 Creating + target_pool_size Ready +
//             max_outstanding_sealed Sealed.
// 16 handles any realistic checkpoint latency.
class WalSegmentPool {
  public:
    // Snapshot of one segment for the WAL reader during recovery (single-threaded).
    struct SegmentSnapshot {
        uint8_t pool_index{UINT8_MAX};
        uint64_t sequence{0};
        SegmentState state{SegmentState::Empty};
        uint64_t write_offset{0}; // upper bound of written user data
    };

    static constexpr uint8_t MAX_POOL_CAPACITY = 16U;

    WalSegmentPool(std::filesystem::path wal_dir,
                   reader::BlockLayout layout,
                   uint64_t slot_max_bytes,
                   uint8_t pool_capacity,
                   uint8_t target_pool_size,
                   std::array<uint8_t, 16> db_instance_uuid);

    ~WalSegmentPool();

    WalSegmentPool(const WalSegmentPool&) = delete;
    auto operator=(const WalSegmentPool&) -> WalSegmentPool& = delete;
    WalSegmentPool(WalSegmentPool&&) = delete;
    auto operator=(WalSegmentPool&&) -> WalSegmentPool& = delete;

    // Block until an Active segment is available.
    // On fresh start this waits for the Vanguard's first Ready segment.
    // On recovery, active_index_ is already set by discover_existing_segments().
    void ensure_active_segment() noexcept;

    [[nodiscard]] auto active_fd() const noexcept -> int;
    [[nodiscard]] auto active_write_offset() const noexcept -> uint64_t;
    [[nodiscard]] auto active_sequence() const noexcept -> uint64_t;

    // Fraction [0.0, 1.0] of the data region written in the Active segment.
    [[nodiscard]] auto active_fill_ratio() const noexcept -> float;

    // True when write_offset + incoming_bytes would reach or exceed data_end_offset_.
    // Captures both overflow and exact-fit so the Flusher is always the sealer.
    [[nodiscard]] auto needs_rotation(size_t incoming_bytes) const noexcept -> bool;

    // Advance the Active segment's write cursor after a successful pwritev.
    void advance_write_offset(size_t bytes_written) noexcept;

    // Record the LSN of a successfully written block.
    // Sets min_lsn on the first call; always updates max_lsn.
    // Stamped into the segment footer at seal time for O(1) GC decisions.
    void record_block_lsn(uint64_t lsn) noexcept;

    // Seal Active segment (footer + fdatasync, fd closed), then rotate to next Ready.
    // Blocks only if Ready pool is empty.
    void seal_and_rotate() noexcept;

    // Seal Active without rotating. Used by the flusher exit path (clean shutdown)
    // and immediately after crash-recovery before ensure_active_segment().
    void seal_active_segment_only() noexcept;

    // Override the Active segment's write_offset. Called by the WAL recovery path
    // after the reader determines the true valid-data extent.
    void set_active_write_offset(uint64_t offset) noexcept;

    // Signal the BG Vanguard thread to replenish the Ready pool.
    void notify_precreate() noexcept;

    // Block until at least one Ready segment exists or the pool is stopping.
    void wait_for_ready_segment() noexcept;

    // Unlink all Sealed segments with max_lsn <= checkpoint_lsn.
    // Triggers SSD TRIM on the freed LBAs. Syncs directory entries via dir_fd_.
    // Safe to call from any thread.
    void release_wal_up_to(uint64_t checkpoint_lsn) noexcept;

    // Snapshot of all pool segments. Acquire-load; safe from any thread.
    // Intended for the WAL reader during single-threaded recovery.
    [[nodiscard]] auto snapshot_segments() const noexcept -> std::vector<SegmentSnapshot>;

    [[nodiscard]] auto ready_segment_count() const noexcept -> uint8_t;

    // UINT8_MAX = no recovery segment (fresh start or clean shutdown).
    [[nodiscard]] auto recovery_segment_index() const noexcept -> uint8_t {
        return recovery_index_;
    }

    // Format: wal_NNNNNNNNNNNN.log (12 zero-padded digits).
    // Sequence is the authoritative key; pool_index is just a pool position.
    [[nodiscard]] auto format_path(uint64_t sequence) const -> std::filesystem::path;

  private:
    std::filesystem::path wal_dir_;
    reader::BlockLayout layout_;
    uint64_t slot_max_bytes_;
    uint64_t data_end_offset_; // = slot_max_bytes_ - WalSegmentFooter::SIZE
    uint64_t data_capacity_;   // = data_end_offset_ - WalSegmentHeader::SIZE
    uint8_t pool_capacity_;
    uint8_t target_pool_size_;
    std::array<uint8_t, 16> db_instance_uuid_;

    std::atomic<uint64_t> next_sequence_{1};

    Segment segments_[MAX_POOL_CAPACITY];
    uint8_t active_index_{UINT8_MAX};
    uint8_t recovery_index_{UINT8_MAX};

    // Opened once in the constructor; kept alive so create_segment() and
    // release_wal_up_to() can call sync_dir_entries(dir_fd_) without
    // re-opening the directory on every create/delete.
    io::UniqueFd dir_fd_{};

    std::atomic<uint8_t> ready_count_{0};

    std::thread bg_thread_{};
    std::mutex mu_{};
    std::condition_variable bg_cv_{};
    std::condition_variable ready_cv_{};
    bool bg_stop_{false};
    std::atomic<bool> bg_needed_{false};

    void precreate_loop() noexcept;
    void create_segment(uint8_t index) noexcept;

    // Returns index of first Empty segment, UINT8_MAX if pool is fully occupied.
    // No Recyclable pass — segments are deleted, not recycled.
    [[nodiscard]] auto find_empty_segment() const noexcept -> uint8_t;

    [[nodiscard]] auto try_activate_next_ready() noexcept -> bool;
    [[nodiscard]] auto write_and_sync_footer() noexcept -> bool;

    // Scan wal_dir_ for wal_*.log files and populate in-memory segments.
    // Replaces the old fixed-index loop: with monotonic filenames, the set of
    // on-disk files is discovered via directory_iterator, not by checking
    // a known set of pool-indexed filenames.
    void discover_existing_segments() noexcept;
};

} // namespace stratadb::wal::pool