#include "stratadb/wal/pool/segment_pool.hpp"

#include "stratadb/utils/os.hpp"
#include "stratadb/wal/slot/slot_footer.hpp"
#include "stratadb/wal/slot/slot_header.hpp"

#include <algorithm>
#include <cassert>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <string>
#include <system_error>
#include <thread>

namespace stratadb::wal::pool {

using slot::seal_footer_crc;
using slot::seal_header_crc;
using slot::validate_footer;
using slot::validate_header;
using slot::WalSlotFooter;
using slot::WalSlotHeader;

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
//     Writes block data, advances write_offset.
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

WalSegmentPool::WalSegmentPool(std::filesystem::path wal_dir,
                               reader::BlockLayout layout,
                               uint64_t segment_max_bytes,
                               uint8_t pool_capacity,
                               uint8_t target_pool_size,
                               std::array<uint8_t, 16> db_instance_uuid)
    : wal_dir_(std::move(wal_dir))
    , layout_(layout)
    , slot_max_bytes_(segment_max_bytes)
    , data_end_offset_(segment_max_bytes - WalSlotFooter::SIZE)
    , data_capacity_(data_end_offset_ - WalSlotHeader::SIZE)
    , pool_capacity_(pool_capacity)
    , target_pool_size_(target_pool_size)
    , db_instance_uuid_(db_instance_uuid) {
    assert(pool_capacity_ >= 2 && pool_capacity_ <= MAX_POOL_CAPACITY);
    for (uint8_t i = 0; i < pool_capacity_; ++i) {
        segments_[i].pool_index = i;
        // path is NOT set here — it is assigned in create_segment() from the
        // monotonic sequence, or discovered by discover_existing_segments().
    }

    // Open the WAL directory once; the fd stays alive for sync_dir_entries()
    // calls after every create and every unlink, avoiding a re-open per I/O.
    // If the directory is inaccessible the DB cannot start — terminate.
    auto dir_result = utils::os::open_directory(wal_dir_);
    assert(dir_result.has_value() && "WAL directory inaccessible");
    dir_fd_ = io::UniqueFd{*dir_result};

    discover_existing_segments();

    bg_thread_ = std::thread([this] { precreate_loop(); });
    {
        std::lock_guard lk(mu_);
        bg_needed_.store(true, std::memory_order_relaxed);
    }
    bg_cv_.notify_one();
}

WalSegmentPool::~WalSegmentPool() {
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
    // dir_fd_, all segment fds: closed by UniqueFd RAII
}

// Block until an Active segment is available.
// On fresh start this waits for the Vanguard's first Ready segment.
// On recovery, active_index_ is already set by discover_existing_segments().
void WalSegmentPool::ensure_active_segment() noexcept {
    if (active_index_ != UINT8_MAX)
        return;
    notify_precreate();
    while (!try_activate_next_ready()) {
        wait_for_ready_segment();
    }
}

auto WalSegmentPool::active_fd() const noexcept -> int {
    return active_index_ == UINT8_MAX ? -1 : segments_[active_index_].fd.get();
}

auto WalSegmentPool::active_write_offset() const noexcept -> uint64_t {
    return active_index_ == UINT8_MAX ? 0 : segments_[active_index_].write_offset;
}

auto WalSegmentPool::active_sequence() const noexcept -> uint64_t {
    return active_index_ == UINT8_MAX ? 0 : segments_[active_index_].sequence;
}

// Fraction [0.0, 1.0] of the data region written in the Active segment.
auto WalSegmentPool::active_fill_ratio() const noexcept -> float {
    if (active_index_ == UINT8_MAX || data_capacity_ == 0)
        return 0.0f;
    const uint64_t written = segments_[active_index_].write_offset - WalSlotHeader::SIZE;
    return static_cast<float>(written) / static_cast<float>(data_capacity_);
}

// True when write_offset + incoming_bytes would reach or exceed data_end_offset_.
// Captures both overflow and exact-fit so the Flusher is always the sealer.
auto WalSegmentPool::needs_rotation(size_t incoming_bytes) const noexcept -> bool {
    if (active_index_ == UINT8_MAX)
        return false;
    return (segments_[active_index_].write_offset + static_cast<uint64_t>(incoming_bytes)) >= data_end_offset_;
}

// Advance the Active segment's write cursor after a successful pwritev.
void WalSegmentPool::advance_write_offset(size_t bytes_written) noexcept {
    assert(active_index_ != UINT8_MAX);
    segments_[active_index_].write_offset += static_cast<uint64_t>(bytes_written);
}

// Record the LSN of a successfully written block.
// Sets min_lsn on the first call; always updates max_lsn.
// Stamped into the segment footer at seal time for O(1) GC decisions.
void WalSegmentPool::record_block_lsn(uint64_t lsn) noexcept {
    assert(active_index_ != UINT8_MAX);
    auto& seg = segments_[active_index_];
    if (seg.min_lsn == 0)
        seg.min_lsn = lsn;
    seg.max_lsn = lsn;
}

// Seal Active segment (footer + fdatasync, fd closed), then rotate to next Ready.
// Blocks only if Ready pool is empty.
void WalSegmentPool::seal_and_rotate() noexcept {
    assert(active_index_ != UINT8_MAX);
    if (!write_and_sync_footer())
        return;

    segments_[active_index_].fd.reset();
    segments_[active_index_].state.store(SegmentState::Sealed, std::memory_order_release);
    active_index_ = UINT8_MAX;

    notify_precreate();
    while (!try_activate_next_ready())
        wait_for_ready_segment();
}

// Seal Active without rotating. Used by the flusher exit path (clean shutdown)
// and immediately after crash-recovery before ensure_active_segment().
void WalSegmentPool::seal_active_segment_only() noexcept {
    if (active_index_ == UINT8_MAX)
        return;
    auto sync = write_and_sync_footer();
    if (!sync) {
        // TODO: handle this error case — the file is created but the directory metadata isn't flushed, risking
    }
    segments_[active_index_].fd.reset();
    segments_[active_index_].state.store(SegmentState::Sealed, std::memory_order_release);
    active_index_ = UINT8_MAX;
}

// Override the Active segment's write_offset. Called by the WAL recovery path
// after the reader determines the true valid-data extent.
void WalSegmentPool::set_active_write_offset(uint64_t offset) noexcept {
    assert(active_index_ != UINT8_MAX);
    segments_[active_index_].write_offset = offset;
}

// Unlink all Sealed segments with max_lsn <= checkpoint_lsn.
// Triggers SSD TRIM on the freed LBAs. Syncs directory entries via dir_fd_.
// Safe to call from any thread.
void WalSegmentPool::release_wal_up_to(uint64_t checkpoint_lsn) noexcept {
    bool any_deleted = false;
    for (uint8_t i = 0; i < pool_capacity_; ++i) {
        auto& seg = segments_[i];
        if (seg.state.load(std::memory_order_acquire) != SegmentState::Sealed)
            continue;
        if (seg.max_lsn > checkpoint_lsn)
            continue;

        std::error_code ec;
        std::filesystem::remove(format_path(seg.sequence), ec);

        seg.sequence = 0;
        seg.write_offset = 0;
        seg.min_lsn = 0;
        seg.max_lsn = 0;
        seg.state.store(SegmentState::Empty, std::memory_order_release);
        any_deleted = true;
    }
    if (any_deleted) {
        auto sync = utils::os::sync_dir_entries(dir_fd_.get());
        if (!sync) {
            // TODO: handle this error case — the file is created but the directory metadata isn't flushed, risking
            // invisibility after a crash. Options:
        }
        notify_precreate();
    }
}

// Signal the BG Vanguard thread to replenish the Ready pool.
void WalSegmentPool::notify_precreate() noexcept {
    if (bg_needed_.load(std::memory_order_relaxed))
        return;
    {
        std::lock_guard lk(mu_);
        bg_needed_.store(true, std::memory_order_relaxed);
    }
    bg_cv_.notify_one();
}

// Block until at least one Ready segment exists or the pool is stopping.
void WalSegmentPool::wait_for_ready_segment() noexcept {
    std::unique_lock lk(mu_);
    ready_cv_.wait(lk, [this] { return ready_count_.load(std::memory_order_acquire) > 0 || bg_stop_; });
}

auto WalSegmentPool::ready_segment_count() const noexcept -> uint8_t {
    return ready_count_.load(std::memory_order_acquire);
}

// Snapshot of all pool segments. Acquire-load; safe from any thread.
// Intended for the WAL reader during single-threaded recovery.
auto WalSegmentPool::snapshot_segments() const noexcept -> std::vector<SegmentSnapshot> {
    std::vector<SegmentSnapshot> out;
    out.reserve(pool_capacity_);
    for (uint8_t i = 0; i < pool_capacity_; ++i) {
        out.push_back({
            .pool_index = i,
            .sequence = segments_[i].sequence,
            .state = segments_[i].state.load(std::memory_order_acquire),
            .write_offset = segments_[i].write_offset,
        });
    }
    return out;
}

// Format: wal_NNNNNNNNNNNN.log (12 zero-padded digits).
// Sequence is the authoritative key; pool_index is just a pool position.
auto WalSegmentPool::format_path(uint64_t sequence) const -> std::filesystem::path {
    char name[32];
    std::snprintf(name, sizeof(name), "wal_%012llu.log", static_cast<unsigned long long>(sequence));
    return wal_dir_ / name;
}




auto WalSegmentPool::write_and_sync_footer() noexcept -> bool {
    auto& seg = segments_[active_index_];

    WalSlotFooter footer{};
    footer.magic = WalSlotFooter::MAGIC;
    footer.slot_sequence = seg.sequence;
    footer.sealed_write_offset = seg.write_offset;
    footer.min_lsn = seg.min_lsn;
    footer.max_lsn = seg.max_lsn;
    seal_footer_crc(footer);

    return utils::os::write_exact(seg.fd.get(), &footer, sizeof(footer), data_end_offset_)
           && utils::os::sync_data(seg.fd.get());
}

auto WalSegmentPool::try_activate_next_ready() noexcept -> bool {
    for (uint8_t i = 0; i < pool_capacity_; ++i) {
        if (segments_[i].state.load(std::memory_order_acquire) != SegmentState::Ready)
            continue;
        segments_[i].write_offset = WalSlotHeader::SIZE;
        segments_[i].min_lsn = 0;
        segments_[i].max_lsn = 0;
        segments_[i].state.store(SegmentState::Active, std::memory_order_release);
        active_index_ = i;
        ready_count_.fetch_sub(1, std::memory_order_relaxed);
        return true;
    }
    return false;
}

// Returns index of first Empty segment, UINT8_MAX if pool is fully occupied.
// No recycle pass — segments are deleted, not reused.
auto WalSegmentPool::find_empty_segment() const noexcept -> uint8_t {
    for (uint8_t i = 0; i < pool_capacity_; ++i) {
        if (segments_[i].state.load(std::memory_order_acquire) == SegmentState::Empty)
            return i;
    }
    return UINT8_MAX;
}

void WalSegmentPool::create_segment(uint8_t index) noexcept {
    auto& seg = segments_[index];
    seg.state.store(SegmentState::Creating, std::memory_order_release);
    const uint64_t seq = next_sequence_.fetch_add(1, std::memory_order_relaxed);

    auto path = format_path(seq);
    auto buf = utils::os::open_buffered(path, true);
    if (!buf) {
        seg.state.store(SegmentState::Empty, std::memory_order_release);
        return;
    }
    io::UniqueFd fd{*buf};

    WalSlotHeader hdr{.magic = WalSlotHeader::MAGIC,
                      .version = WalSlotHeader::VERSION,
                      .block_layout = static_cast<uint8_t>(layout_),
                      .slot_sequence = seq,
                      .slot_max_bytes = slot_max_bytes_,
                      .db_instance_uuid = db_instance_uuid_};
    seal_header_crc(hdr);

    if (!utils::os::write_exact(fd.get(), &hdr, sizeof(hdr), 0)
        || !utils::os::allocate_file_space(fd.get(), slot_max_bytes_) || !utils::os::sync_data(fd.get())) {
        std::filesystem::remove(path);
        seg.state.store(SegmentState::Empty, std::memory_order_release);
        return;
    }

    fd.reset();

    auto direct = utils::os::open_direct(path);
    if (!direct) {
        std::filesystem::remove(path);
        seg.state.store(SegmentState::Empty, std::memory_order_release);
        return;
    }

    auto sync = utils::os::sync_dir_entries(dir_fd_.get());
    if (!sync) {
        // TODO: handle this error case — the file is created but the directory metadata isn't flushed, risking
        // invisibility after a crash. Options:
    }
    seg.fd = io::UniqueFd{*direct};
    seg.sequence = seq;
    seg.write_offset = WalSlotHeader::SIZE;
    seg.state.store(SegmentState::Ready, std::memory_order_release);
    ready_count_.fetch_add(1, std::memory_order_release);
    ready_cv_.notify_one();
}

void WalSegmentPool::precreate_loop() noexcept {
    while (true) {
        {
            std::unique_lock lk(mu_);
            bg_cv_.wait(lk, [this] { return bg_needed_.load(std::memory_order_relaxed) || bg_stop_; });
            if (bg_stop_)
                return;
            bg_needed_.store(false, std::memory_order_relaxed);
        }

        while (ready_count_.load(std::memory_order_acquire) < target_pool_size_) {
            uint8_t idx = find_empty_segment();
            if (idx == UINT8_MAX)
                break;
            create_segment(idx);
        }
    }
}

// Scan wal_dir_ for wal_*.log files and populate in-memory segments.
// Monotonic filenames are discovered via directory_iterator, not by
// checking a fixed set of pool-indexed filenames.
void WalSegmentPool::discover_existing_segments() noexcept {
    struct F {
        uint64_t seq;
        std::filesystem::path p;
    };
    std::vector<F> found;

    for (auto& e : std::filesystem::directory_iterator(wal_dir_)) {
        if (e.path().extension() != ".log")
            continue;
        std::string stem = e.path().stem().string();
        uint64_t seq = 0;
        if (stem.size() > 4 && std::from_chars(stem.data() + 4, stem.data() + stem.size(), seq).ec == std::errc{})
            found.push_back({.seq = seq, .p = e.path()});
    }

    std::ranges::sort(found, {}, &F::seq);
    if (found.size() > pool_capacity_)
        found.erase(found.begin(),
                    found.begin() + static_cast<std::vector<F>::difference_type>(found.size() - pool_capacity_));

    uint8_t idx = 0;
    uint64_t max_seq = 0;
    for (auto& [seq, p] : found) {
        auto buf = utils::os::open_buffered(p, false);
        if (!buf)
            continue;
        io::UniqueFd fd{*buf};
        WalSlotHeader h{};
        if (!utils::os::read_exact(fd.get(), &h, sizeof(h), 0) || !validate_header(h)
            || h.db_instance_uuid != db_instance_uuid_)
            continue;

        WalSlotFooter f{};
        bool sealed = utils::os::read_exact(fd.get(), &f, sizeof(f), slot_max_bytes_ - WalSlotFooter::SIZE)
                      && validate_footer(f, seq);

        segments_[idx].sequence = seq;
        if (sealed) {
            segments_[idx].state.store(SegmentState::Sealed, std::memory_order_relaxed);
            segments_[idx].write_offset = f.sealed_write_offset;
        } else {
            fd.reset();
            auto direct = utils::os::open_direct(p);
            if (!direct)
                continue;
            segments_[idx].fd = io::UniqueFd{*direct};
            segments_[idx].write_offset = data_end_offset_;
            segments_[idx].state.store(SegmentState::Active, std::memory_order_relaxed);
            active_index_ = idx;
            recovery_index_ = idx;
        }
        max_seq = std::max(max_seq, seq);
        ++idx;
    }
    next_sequence_.store(max_seq + 1, std::memory_order_relaxed);
}

} // namespace stratadb::wal::pool