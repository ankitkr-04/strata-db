
#include "stratadb/wal/ring/ring.hpp"

#include "stratadb/utils/os.hpp"
#include "stratadb/wal/slot/slot_footer.hpp"
#include "stratadb/wal/slot/slot_header.hpp"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <unistd.h>

namespace stratadb::wal::ring {

using slot::seal_footer_crc;
using slot::seal_header_crc;
using slot::validate_footer;
using slot::validate_header;
using slot::WalSlotFooter;
using slot::WalSlotHeader;

WalRing::WalRing(std::filesystem::path wal_dir,
                 reader::BlockLayout layout,
                 // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
                 uint64_t slot_max_bytes,
                 uint8_t ring_capacity,
                 uint8_t target_pool_size,
                 std::array<uint8_t, 16> db_instance_uuid)
    : wal_dir_(std::move(wal_dir))
    , layout_(layout)
    , slot_max_bytes_(slot_max_bytes)
    , data_end_offset_(slot_max_bytes - WalSlotFooter::SIZE)
    , data_capacity_(data_end_offset_ - WalSlotHeader::SIZE)
    , ring_capacity_(ring_capacity)
    , target_pool_size_(target_pool_size)
    , db_instance_uuid_(db_instance_uuid) {

    assert(ring_capacity_ >= 2 && ring_capacity_ <= MAX_RING_CAPACITY);
    assert(slot_max_bytes_ > WalSlotHeader::SIZE + WalSlotFooter::SIZE);
    assert(target_pool_size_ >= 1);

    for (uint8_t i = 0; i < ring_capacity_; ++i) {
        slots_[i].ring_index = i;
        slots_[i].path = slot_path(i);
    }

    open_existing_slots();

    bg_thread_ = std::thread([this] { precreate_loop(); });
    {
        std::lock_guard lk(mu_);
        bg_needed_.store(true, std::memory_order_relaxed);
    }
    bg_cv_.notify_one();
}

WalRing::~WalRing() {
    {
        std::lock_guard lk(mu_);
        bg_stop_ = true;
        bg_needed_.store(true, std::memory_order_relaxed);
    }
    bg_cv_.notify_one();
    ready_cv_.notify_all();
    if (bg_thread_.joinable()) {
        bg_thread_.join();
    }
}

void WalRing::ensure_active_slot() noexcept {
    if (active_index_ != UINT8_MAX) {
        return;
    }
    notify_precreate();
    while (!try_activate_next_ready()) {
        wait_for_ready_slot();
    }
}

auto WalRing::active_fd() const noexcept -> int {
    if (active_index_ == UINT8_MAX) {
        return -1;
    }
    return slots_[active_index_].fd.get();
}

auto WalRing::active_write_offset() const noexcept -> uint64_t {
    if (active_index_ == UINT8_MAX) {
        return 0;
    }
    return slots_[active_index_].write_offset;
}

auto WalRing::active_sequence() const noexcept -> uint64_t {
    if (active_index_ == UINT8_MAX) {
        return 0;
    }
    return slots_[active_index_].sequence;
}

auto WalRing::active_fill_ratio() const noexcept -> float {
    if (active_index_ == UINT8_MAX || data_capacity_ == 0) {
        return 0.0f;
    }
    const uint64_t written = slots_[active_index_].write_offset - WalSlotHeader::SIZE;
    return static_cast<float>(written) / static_cast<float>(data_capacity_);
}

auto WalRing::needs_rotation(size_t incoming_bytes) const noexcept -> bool {
    if (active_index_ == UINT8_MAX) {
        return false;
    }
    // Boundary math (final decision):
    //   new_off >= data_end_offset_ captures both overflow AND exact-fit.
    //   In both cases the Flusher is the sealer and the block is NOT written
    //   to the current slot — it retries in the next slot after rotation.
    return (slots_[active_index_].write_offset + static_cast<uint64_t>(incoming_bytes)) >= data_end_offset_;
}

void WalRing::advance_write_offset(size_t bytes_written) noexcept {
    assert(active_index_ != UINT8_MAX);
    slots_[active_index_].write_offset += static_cast<uint64_t>(bytes_written);
}

void WalRing::record_block_lsn(uint64_t lsn) noexcept {
    assert(active_index_ != UINT8_MAX);
    auto& slot = slots_[active_index_];
    // LSN 0 is reserved (lsn_generator_ starts at 1), so 0 is the "not yet set"
    // sentinel. The first real block write sets min_lsn and is never overwritten.
    if (slot.min_lsn == 0) {
        slot.min_lsn = lsn;
    }
    slot.max_lsn = lsn; // always updated — captures the last block written
}

void WalRing::seal_and_rotate() noexcept {
    assert(active_index_ != UINT8_MAX);

    if (!write_and_sync_footer()) {
        // I/O error: leave Active. Crash recovery handles this as an unsealed slot.
        return;
    }

    slots_[active_index_].state.store(WalSlotState::Sealed, std::memory_order_release);
    active_index_ = UINT8_MAX;

    notify_precreate();

    while (!try_activate_next_ready()) {
        wait_for_ready_slot();
    }
}

void WalRing::seal_active_slot_only() noexcept {
    if (active_index_ == UINT8_MAX) {
        return;
    }
    bool footer_written = write_and_sync_footer(); // best-effort on shutdown

    if (!footer_written) {
        // TODO: LOG THE CRASH
    }
    slots_[active_index_].state.store(WalSlotState::Sealed, std::memory_order_release);
    active_index_ = UINT8_MAX;
}

void WalRing::set_active_write_offset(uint64_t offset) noexcept {
    assert(active_index_ != UINT8_MAX);
    slots_[active_index_].write_offset = offset;
}

void WalRing::notify_precreate() noexcept {
    if (bg_needed_.load(std::memory_order_relaxed)) {
        return;
    }
    {
        std::lock_guard lk(mu_);
        bg_needed_.store(true, std::memory_order_relaxed);
    }
    bg_cv_.notify_one();
}

void WalRing::wait_for_ready_slot() noexcept {
    std::unique_lock lk(mu_);
    ready_cv_.wait(lk, [this] { return ready_count_.load(std::memory_order_acquire) > 0 || bg_stop_; });
}

auto WalRing::ready_slot_count() const noexcept -> uint8_t {
    return ready_count_.load(std::memory_order_acquire);
}

auto WalRing::slot_path(uint8_t i) const -> std::filesystem::path {
    char name[24];
    std::snprintf(name, sizeof(name), "wal_slot_%03u.wal", static_cast<unsigned>(i));
    return wal_dir_ / name;
}

auto WalRing::write_and_sync_footer() noexcept -> bool {
    auto& slot = slots_[active_index_];

    WalSlotFooter footer{};
    footer.magic = WalSlotFooter::MAGIC;
    footer.slot_sequence = slot.sequence;
    footer.sealed_write_offset = slot.write_offset;
    footer.min_lsn = slot.min_lsn; // enables O(1) GC decisions
    footer.max_lsn = slot.max_lsn; // enables O(1) GC decisions
    seal_footer_crc(footer);

    // Pre-fallocated region: pure in-place overwrite, no inode update.
    const off_t footer_pos = static_cast<off_t>(data_end_offset_);
    const ssize_t n = ::pwrite(slot.fd.get(), &footer, sizeof(footer), footer_pos);
    if (n != static_cast<ssize_t>(sizeof(footer))) {
        return false;
    }

    // Mandatory fdatasync before retiring this slot regardless of sync_on_commit.
    return utils::os::sync_data(slot.fd.get());
}

auto WalRing::try_activate_next_ready() noexcept -> bool {
    for (uint8_t i = 0; i < ring_capacity_; ++i) {
        if (slots_[i].state.load(std::memory_order_acquire) == WalSlotState::Ready) {
            // Reset Flusher-private fields before handing the slot over.
            slots_[i].write_offset = WalSlotHeader::SIZE;
            slots_[i].min_lsn = 0; // "not yet set" sentinel
            slots_[i].max_lsn = 0;
            slots_[i].state.store(WalSlotState::Active, std::memory_order_release);
            active_index_ = i;
            ready_count_.fetch_sub(1, std::memory_order_relaxed);
            return true;
        }
    }
    return false;
}

void WalRing::precreate_loop() noexcept {
    while (true) {
        {
            std::unique_lock lk(mu_);
            bg_cv_.wait(lk, [this] { return bg_needed_.load(std::memory_order_relaxed) || bg_stop_; });
            if (bg_stop_) {
                return;
            }
            bg_needed_.store(false, std::memory_order_relaxed);
        }

        while (ready_count_.load(std::memory_order_acquire) < target_pool_size_) {
            const uint8_t idx = find_slot_to_create();
            if (idx == UINT8_MAX) {
                break;
            }
            create_slot(idx);
        }
    }
}

auto WalRing::find_slot_to_create() const noexcept -> uint8_t {
    // Pass 1: prefer Empty (no file overwrite needed).
    for (uint8_t i = 0; i < ring_capacity_; ++i) {
        if (slots_[i].state.load(std::memory_order_acquire) == WalSlotState::Empty) {
            return i;
        }
    }
    // Pass 2: recycle the oldest Sealed slot (lowest sequence = processed first).
    uint8_t oldest_idx = UINT8_MAX;
    uint64_t oldest_seq = UINT64_MAX;
    for (uint8_t i = 0; i < ring_capacity_; ++i) {
        if (slots_[i].state.load(std::memory_order_acquire) == WalSlotState::Sealed
            && slots_[i].sequence < oldest_seq) {
            oldest_seq = slots_[i].sequence;
            oldest_idx = i;
        }
    }
    return oldest_idx;
}

void WalRing::create_slot(uint8_t index) noexcept {
    auto& slot = slots_[index];
    slot.state.store(WalSlotState::Creating, std::memory_order_release);

    const int raw_fd = ::open(slot.path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (raw_fd < 0) {
        slot.state.store(WalSlotState::Empty, std::memory_order_release);
        return;
    }

    const uint64_t seq = next_sequence_.fetch_add(1, std::memory_order_relaxed);

    // ── Write the immutable 4096-byte header ──────────────────────────────────
    // db_instance_uuid is stamped here so open_existing_slots() can reject
    // any slot that does not belong to this DB instance on the next startup.
    WalSlotHeader hdr{};
    hdr.magic = WalSlotHeader::MAGIC;
    hdr.version = WalSlotHeader::VERSION;
    hdr.block_layout = static_cast<uint8_t>(layout_);
    hdr.slot_sequence = seq;
    hdr.slot_max_bytes = slot_max_bytes_;
    hdr.created_at_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
    hdr.db_instance_uuid = db_instance_uuid_; // chain of custody

    seal_header_crc(hdr);

    const ssize_t n_hdr = ::pwrite(raw_fd, &hdr, sizeof(hdr), 0);
    if (n_hdr != static_cast<ssize_t>(sizeof(hdr))) {
        ::close(raw_fd);
        slot.state.store(WalSlotState::Empty, std::memory_order_release);
        return;
    }

    // ── posix_fallocate — extend EOF to slot_max_bytes_ ───────────────────────
    // Critical: must NOT use FALLOC_FL_KEEP_SIZE.
    // Without EOF extension, every Flusher write past offset 4096 forces the
    // filesystem to journal-commit on each fdatasync — destroying throughput.
    // With posix_fallocate(0, slot_max_bytes_) the inode EOF is set immediately;
    // all future Flusher writes are pure in-place overwrites that bypass the
    // FS journal entirely.
    if (::posix_fallocate(raw_fd, 0, static_cast<off_t>(slot_max_bytes_)) != 0) {
        ::close(raw_fd);
        slot.state.store(WalSlotState::Empty, std::memory_order_release);
        return;
    }

    // ── fdatasync — freeze the header; the Flusher may now write at offset 4096
    if (!utils::os::sync_data(raw_fd)) {
        ::close(raw_fd);
        slot.state.store(WalSlotState::Empty, std::memory_order_release);
        return;
    }

    slot.fd = io::UniqueFd{raw_fd};
    slot.sequence = seq;
    slot.write_offset = WalSlotHeader::SIZE;
    slot.min_lsn = 0;
    slot.max_lsn = 0;

    slot.state.store(WalSlotState::Ready, std::memory_order_release);
    ready_count_.fetch_add(1, std::memory_order_release);
    ready_cv_.notify_one();
}

void WalRing::open_existing_slots() noexcept {
    uint64_t max_seq = 0;
    uint8_t recovery_idx = UINT8_MAX;
    uint64_t recovery_seq = 0;

    for (uint8_t i = 0; i < ring_capacity_; ++i) {
        const auto& path = slots_[i].path;

        if (!std::filesystem::exists(path)) {
            slots_[i].state.store(WalSlotState::Empty, std::memory_order_relaxed);
            continue;
        }

        const int raw_fd = ::open(path.c_str(), O_RDWR);
        if (raw_fd < 0) {
            slots_[i].state.store(WalSlotState::Empty, std::memory_order_relaxed);
            continue;
        }

        WalSlotHeader hdr{};
        const ssize_t n_hdr = ::pread(raw_fd, &hdr, sizeof(hdr), 0);
        if (n_hdr != static_cast<ssize_t>(sizeof(hdr)) || !validate_header(hdr)) {
            ::close(raw_fd);
            slots_[i].state.store(WalSlotState::Empty, std::memory_order_relaxed);
            continue;
        }

        // ── UUID validation — chain of custody ────────────────────────────────
        // Reject any slot that does not belong to the current DB instance.
        // This fires when a DevOps engineer copies wal_slot_*.wal files from
        // DB-A's directory into DB-B's directory. Without this check, DB-B
        // would silently replay DB-A's WAL into its own MemTable, producing
        // data corruption that is invisible at the block-checksum level.
        // The same DB instance stopping/restarting keeps the same IDENTITY
        // file so this check never rejects valid crash-recovery slots.
        if (hdr.db_instance_uuid != db_instance_uuid_) {
            ::close(raw_fd);
            // Treat as Empty: the BG thread will safely overwrite this slot.
            slots_[i].state.store(WalSlotState::Empty, std::memory_order_relaxed);
            continue;
        }

        WalSlotFooter footer{};
        const auto footer_pos = static_cast<off_t>(hdr.slot_max_bytes - WalSlotFooter::SIZE);
        const ssize_t n_ftr = ::pread(raw_fd, &footer, sizeof(footer), footer_pos);

        const bool is_sealed =
            (n_ftr == static_cast<ssize_t>(sizeof(footer))) && validate_footer(footer, hdr.slot_sequence);

        slots_[i].fd = io::UniqueFd{raw_fd};
        slots_[i].sequence = hdr.slot_sequence;

        if (is_sealed) {
            slots_[i].write_offset = footer.sealed_write_offset;
            slots_[i].min_lsn = footer.min_lsn;
            slots_[i].max_lsn = footer.max_lsn;
            slots_[i].state.store(WalSlotState::Sealed, std::memory_order_relaxed);
        } else {
            // Unsealed: last active slot before crash (or interrupted creation).
            // Set write_offset conservatively to data_end_offset_; the WAL reader
            // determines the true last valid byte via block-level checksum walking.
            slots_[i].write_offset = data_end_offset_;
            slots_[i].min_lsn = 0; // unknown until reader scans blocks
            slots_[i].max_lsn = 0;
            slots_[i].state.store(WalSlotState::Active, std::memory_order_relaxed);

            if (hdr.slot_sequence > recovery_seq) {
                recovery_seq = hdr.slot_sequence;
                recovery_idx = i;
            }
        }

        if (hdr.slot_sequence > max_seq) {
            max_seq = hdr.slot_sequence;
        }
    }

    next_sequence_.store(max_seq + 1, std::memory_order_relaxed);
    recovery_slot_index_ = recovery_idx;

    if (recovery_idx != UINT8_MAX) {
        active_index_ = recovery_idx;
    }
}

} // namespace stratadb::wal::ring