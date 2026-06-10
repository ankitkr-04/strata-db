#include "../support/wal_test_helpers.hpp"
#include "stratadb/wal/pool/segment_pool.hpp"
#include "stratadb/wal/pool/segment_state.hpp"
#include "stratadb/wal/reader/validator.hpp"
#include "stratadb/wal/slot/slot_footer.hpp"
#include "stratadb/wal/slot/slot_header.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <thread>
#include <vector>

using namespace stratadb::wal::test;
using namespace stratadb::wal;
using stratadb::wal::pool::WalSegmentPool;

static constexpr std::uint64_t kSlotSize = 64LL * 1024;
static constexpr std::uint8_t kPoolCapacity = 5;
static constexpr std::uint8_t kTargetPoolSize = 2;

static constexpr std::uint64_t kDataEndOffset = kSlotSize - stratadb::wal::slot::WalSlotFooter::SIZE;
static constexpr std::uint64_t kDataCapacity = kDataEndOffset - stratadb::wal::slot::WalSlotHeader::SIZE;

static bool
wait_ready(WalSegmentPool& pool, uint8_t target, std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pool.ready_segment_count() >= target)
            return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

TEST(SegmentPool, VanguardPreallocates) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid = make_test_uuid();

    WalSegmentPool pool(dir.path, reader::BlockLayout::Gamma4K, kSlotSize, kPoolCapacity, kTargetPoolSize, uuid);

    ASSERT_TRUE(wait_ready(pool, kTargetPoolSize))
        << "Vanguard did not pre-allocate " << static_cast<int>(kTargetPoolSize)
        << " Ready segments within the timeout";

    std::size_t log_count = 0;
    for (auto& entry : std::filesystem::directory_iterator(dir.path)) {
        if (entry.path().extension() != ".log")
            continue;
        ++log_count;
        const auto file_sz = std::filesystem::file_size(entry.path());
        EXPECT_EQ(file_sz, kSlotSize) << "File " << entry.path() << " has unexpected size " << file_sz;
    }
    EXPECT_GE(log_count, kTargetPoolSize);
}

TEST(SegmentPool, RotationOnFull) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid = make_test_uuid();

    WalSegmentPool pool(dir.path, reader::BlockLayout::Gamma4K, kSlotSize, kPoolCapacity, kTargetPoolSize, uuid);

    ASSERT_TRUE(wait_ready(pool, 1));

    pool.ensure_active_segment();
    ASSERT_NE(pool.active_fd(), -1);

    const uint64_t seq_before = pool.active_sequence();
    EXPECT_GT(seq_before, 0u);

    pool.advance_write_offset(kDataCapacity);
    EXPECT_TRUE(pool.needs_rotation(1));

    ASSERT_TRUE(wait_ready(pool, 1));

    pool.seal_and_rotate();

    EXPECT_NE(pool.active_sequence(), seq_before);
    EXPECT_GT(pool.active_sequence(), seq_before);
}

TEST(SegmentPool, GCUnlinksFiles) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid = make_test_uuid();

    WalSegmentPool pool(dir.path, reader::BlockLayout::Gamma4K, kSlotSize, kPoolCapacity, kTargetPoolSize, uuid);

    ASSERT_TRUE(wait_ready(pool, 1));
    pool.ensure_active_segment();
    ASSERT_NE(pool.active_fd(), -1);

    const uint64_t seq1 = pool.active_sequence();
    const auto path1 = pool.format_path(seq1);

    pool.advance_write_offset(kDataCapacity);
    pool.record_block_lsn(42);
    ASSERT_TRUE(wait_ready(pool, 1));
    pool.seal_and_rotate();

    EXPECT_TRUE(std::filesystem::exists(path1)) << "Sealed segment " << path1 << " missing from disk";

    pool.release_wal_up_to(42);

    EXPECT_FALSE(std::filesystem::exists(path1)) << "GC failed to unlink " << path1;
}

TEST(SegmentPool, CrashRecoveryLoadsMostRecent) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid = make_test_uuid();
    constexpr uint8_t kCrashPoolCapacity = 16;
    constexpr uint8_t kCrashTarget = 1;

    for (uint64_t seq = 1; seq <= 20; ++seq) {
        ASSERT_NO_FATAL_FAILURE(create_sealed_wal_file(dir.path,
                                                       seq,
                                                       kSlotSize,
                                                       uuid,
                                                       reader::BlockLayout::Gamma4K,
                                                       /*max_lsn=*/seq * 100));
    }

    WalSegmentPool pool(dir.path, reader::BlockLayout::Gamma4K, kSlotSize, kCrashPoolCapacity, kCrashTarget, uuid);

    auto snaps = pool.snapshot_segments();

    uint64_t min_seq = UINT64_MAX, max_seq = 0;
    int sealed_count = 0;
    for (auto& s : snaps) {
        if (s.state == pool::SegmentState::Sealed && s.sequence > 0) {
            ++sealed_count;
            min_seq = std::min(min_seq, s.sequence);
            max_seq = std::max(max_seq, s.sequence);
        }
    }

    EXPECT_EQ(sealed_count, kCrashPoolCapacity) << "Expected exactly 16 Sealed segments after recovery";
    EXPECT_EQ(min_seq, 5u) << "Oldest loaded segment should be seq 5";
    EXPECT_EQ(max_seq, 20u) << "Newest loaded segment should be seq 20";

    for (auto& s : snaps) {
        EXPECT_GE(s.sequence == 0 ? 5u : s.sequence, 5u)
            << "Stale sequence " << s.sequence << " should have been dropped";
    }
}

TEST(SegmentPool, RejectsForeignInstanceFiles) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid_a = make_test_uuid();
    std::array<uint8_t, 16> uuid_b = uuid_a;
    uuid_b[0] ^= 0xFF;

    ASSERT_NO_FATAL_FAILURE(create_sealed_wal_file(dir.path, 1, kSlotSize, uuid_b, reader::BlockLayout::Gamma4K));

    WalSegmentPool pool(dir.path, reader::BlockLayout::Gamma4K, kPoolCapacity, kTargetPoolSize, 2, uuid_a);

    auto snaps = pool.snapshot_segments();
    int non_empty = 0;
    for (auto& s : snaps) {
        if (s.state != pool::SegmentState::Empty)
            ++non_empty;
    }
    EXPECT_EQ(non_empty, 0) << "Foreign-instance file should have been rejected";
}
