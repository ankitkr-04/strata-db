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
    }

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
}

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
    return active_index_ == UINT8_MAX ? 0ULL : segments_[active_index_].write_offset.load(std::memory_order_relaxed);
}

auto WalSegmentPool::active_sequence() const noexcept -> uint64_t {
    return active_index_ == UINT8_MAX ? 0ULL : segments_[active_index_].sequence.load(std::memory_order_relaxed);
}

auto WalSegmentPool::active_fill_ratio() const noexcept -> float {
    if (active_index_ == UINT8_MAX || data_capacity_ == 0)
        return 0.0f;
    const uint64_t current_offset = segments_[active_index_].write_offset.load(std::memory_order_relaxed);
    const uint64_t written = current_offset - WalSlotHeader::SIZE;
    return static_cast<float>(written) / static_cast<float>(data_capacity_);
}

auto WalSegmentPool::needs_rotation(size_t incoming_bytes) const noexcept -> bool {
    if (active_index_ == UINT8_MAX)
        return false;
    const uint64_t current_offset = segments_[active_index_].write_offset.load(std::memory_order_relaxed);
    return (current_offset + static_cast<uint64_t>(incoming_bytes)) >= data_end_offset_;
}

void WalSegmentPool::advance_write_offset(size_t bytes_written) noexcept {
    assert(active_index_ != UINT8_MAX);
    uint64_t current = segments_[active_index_].write_offset.load(std::memory_order_relaxed);
    segments_[active_index_].write_offset.store(current + static_cast<uint64_t>(bytes_written),
                                                std::memory_order_relaxed);
}

void WalSegmentPool::record_block_lsn(uint64_t lsn) noexcept {
    assert(active_index_ != UINT8_MAX);
    auto& seg = segments_[active_index_];
    if (seg.min_lsn == 0)
        seg.min_lsn = lsn;
    seg.max_lsn = lsn;
}

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

void WalSegmentPool::seal_active_segment_only() noexcept {
    if (active_index_ == UINT8_MAX)
        return;
    auto sync = write_and_sync_footer();
    if (!sync) {
        // Handle error appropriately
    }
    segments_[active_index_].fd.reset();
    segments_[active_index_].state.store(SegmentState::Sealed, std::memory_order_release);
    active_index_ = UINT8_MAX;
}

void WalSegmentPool::set_active_write_offset(uint64_t offset) noexcept {
    assert(active_index_ != UINT8_MAX);
    segments_[active_index_].write_offset.store(offset, std::memory_order_relaxed);
}

void WalSegmentPool::release_wal_up_to(uint64_t checkpoint_lsn) noexcept {
    bool any_deleted = false;
    for (uint8_t i = 0; i < pool_capacity_; ++i) {
        auto& seg = segments_[i];
        if (seg.state.load(std::memory_order_acquire) != SegmentState::Sealed)
            continue;
        if (seg.max_lsn > checkpoint_lsn)
            continue;

        std::error_code ec;
        std::filesystem::remove(format_path(seg.sequence.load(std::memory_order_relaxed)), ec);

        seg.sequence.store(0, std::memory_order_relaxed);
        seg.write_offset.store(0, std::memory_order_relaxed);
        seg.min_lsn = 0;
        seg.max_lsn = 0;
        seg.state.store(SegmentState::Empty, std::memory_order_release);
        any_deleted = true;
    }
    if (any_deleted) {
        auto sync = utils::os::sync_dir_entries(dir_fd_.get());
        if (!sync) {
            // Handle error appropriately
        }
        notify_precreate();
    }
}

void WalSegmentPool::notify_precreate() noexcept {
    if (bg_needed_.load(std::memory_order_relaxed))
        return;
    {
        std::lock_guard lk(mu_);
        bg_needed_.store(true, std::memory_order_relaxed);
    }
    bg_cv_.notify_one();
}

void WalSegmentPool::wait_for_ready_segment() noexcept {
    std::unique_lock lk(mu_);
    ready_cv_.wait(lk, [this] { return ready_count_.load(std::memory_order_acquire) > 0 || bg_stop_; });
}

auto WalSegmentPool::ready_segment_count() const noexcept -> uint8_t {
    return ready_count_.load(std::memory_order_acquire);
}

auto WalSegmentPool::snapshot_segments() const noexcept -> std::vector<SegmentSnapshot> {
    std::vector<SegmentSnapshot> out;
    out.reserve(pool_capacity_);
    for (uint8_t i = 0; i < pool_capacity_; ++i) {
        SegmentState st = segments_[i].state.load(std::memory_order_acquire);
        uint64_t seq = 0;
        uint64_t off = 0;

        // Only read non-atomic fields if Vanguard is finished creating them
        if (st == SegmentState::Ready || st == SegmentState::Active || st == SegmentState::Sealed) {
            seq = segments_[i].sequence.load(std::memory_order_relaxed);
            off = segments_[i].write_offset.load(std::memory_order_relaxed);
        }

        out.push_back({
            .pool_index = i,
            .sequence = seq,
            .state = st,
            .write_offset = off,
        });
    }
    return out;
}

auto WalSegmentPool::format_path(uint64_t sequence) const -> std::filesystem::path {
    char name[32];
    std::snprintf(name, sizeof(name), "wal_%012llu.log", static_cast<unsigned long long>(sequence));
    return wal_dir_ / name;
}

auto WalSegmentPool::write_and_sync_footer() noexcept -> bool {
    auto& seg = segments_[active_index_];

    WalSlotFooter footer{};
    footer.magic = WalSlotFooter::MAGIC;
    footer.slot_sequence = seg.sequence.load(std::memory_order_relaxed);
    footer.sealed_write_offset = seg.write_offset.load(std::memory_order_relaxed);
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
        segments_[i].write_offset.store(WalSlotHeader::SIZE, std::memory_order_relaxed);
        segments_[i].min_lsn = 0;
        segments_[i].max_lsn = 0;
        segments_[i].state.store(SegmentState::Active, std::memory_order_release);
        active_index_ = i;
        ready_count_.fetch_sub(1, std::memory_order_relaxed);
        return true;
    }
    return false;
}

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
        // Handle error appropriately
    }
    seg.fd = io::UniqueFd{*direct};

    // Write atomic values with relaxed memory ordering
    seg.sequence.store(seq, std::memory_order_relaxed);
    seg.write_offset.store(WalSlotHeader::SIZE, std::memory_order_relaxed);

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

        segments_[idx].sequence.store(seq, std::memory_order_relaxed);
        if (sealed) {
            segments_[idx].state.store(SegmentState::Sealed, std::memory_order_relaxed);
            segments_[idx].write_offset.store(f.sealed_write_offset, std::memory_order_relaxed);
        } else {
            fd.reset();
            auto direct = utils::os::open_direct(p);
            if (!direct)
                continue;
            segments_[idx].fd = io::UniqueFd{*direct};
            segments_[idx].write_offset.store(data_end_offset_, std::memory_order_relaxed);
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