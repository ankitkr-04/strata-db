// tests/wal/segment_pool_test.cpp
//
// Unit tests for WalSegmentPool.
//
// Coverage:
//   VanguardPreallocates      — BG thread creates target_pool_size files on disk
//                               with the exact expected byte size
//   RotationOnFull            — seal_and_rotate() increments active_sequence()
//                               and leaves a sealed file behind
//   GCUnlinksFiles            — release_wal_up_to() unlinks the right files
//   CrashRecoveryOverflow     — booting with 20 on-disk files (pool_capacity=16)
//                               loads the 16 most-recent, drops the 4 oldest

#include "stratadb/wal/pool/segment_pool.hpp"
#include "stratadb/wal/pool/segment_state.hpp"
#include "stratadb/wal/reader/validator.hpp"
#include "stratadb/wal/slot/slot_footer.hpp"
#include "stratadb/wal/slot/slot_header.hpp"
#include "wal_test_helpers.hpp"

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

// ─────────────────────────────────────────────────────────────────────────────
// Constants used across tests
// ─────────────────────────────────────────────────────────────────────────────
static constexpr std::uint64_t kSlotSize = 64LL * 1024; // 64 KiB per segment
static constexpr std::uint8_t kPoolCapacity = 5;
static constexpr std::uint8_t kTargetPoolSize = 2;

// data_end_offset = slot - footer; data_capacity = data_end - header
static constexpr std::uint64_t kDataEndOffset = kSlotSize - stratadb::wal::slot::WalSlotFooter::SIZE;
static constexpr std::uint64_t kDataCapacity = kDataEndOffset - stratadb::wal::slot::WalSlotHeader::SIZE;

// ─────────────────────────────────────────────────────────────────────────────
// Helper: wait until ready_count >= target with a timeout
// ─────────────────────────────────────────────────────────────────────────────
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

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: Vanguard Pre-allocation
// ─────────────────────────────────────────────────────────────────────────────
TEST(SegmentPool, VanguardPreallocates) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid = make_test_uuid();

    WalSegmentPool pool(dir.path, reader::BlockLayout::Gamma4K, kSlotSize, kPoolCapacity, kTargetPoolSize, uuid);

    // Wait for the Vanguard to create at least kTargetPoolSize Ready segments.
    ASSERT_TRUE(wait_ready(pool, kTargetPoolSize))
        << "Vanguard did not pre-allocate " << static_cast<int>(kTargetPoolSize)
        << " Ready segments within the timeout";

    // Each file on disk must be exactly kSlotSize bytes.
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

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: Rotation on full
// ─────────────────────────────────────────────────────────────────────────────
TEST(SegmentPool, RotationOnFull) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid = make_test_uuid();

    WalSegmentPool pool(dir.path, reader::BlockLayout::Gamma4K, kSlotSize, kPoolCapacity, kTargetPoolSize, uuid);

    // Wait for at least one Ready segment so ensure_active_segment won't hang.
    ASSERT_TRUE(wait_ready(pool, 1));

    pool.ensure_active_segment();
    ASSERT_NE(pool.active_fd(), -1);

    const uint64_t seq_before = pool.active_sequence();
    EXPECT_GT(seq_before, 0u);

    // Simulate filling the segment by advancing the write cursor to the end.
    pool.advance_write_offset(kDataCapacity);
    EXPECT_TRUE(pool.needs_rotation(1));

    // Wait for a second Ready segment so seal_and_rotate doesn't block.
    ASSERT_TRUE(wait_ready(pool, 1)); // at least 1 Ready after one was activated

    pool.seal_and_rotate();

    // active_sequence() must have advanced to a new segment.
    EXPECT_NE(pool.active_sequence(), seq_before);
    EXPECT_GT(pool.active_sequence(), seq_before);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: GC unlinks files
// ─────────────────────────────────────────────────────────────────────────────
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

    // Advance to trigger rotation, then seal.
    pool.advance_write_offset(kDataCapacity);
    pool.record_block_lsn(42); // max_lsn = 42
    ASSERT_TRUE(wait_ready(pool, 1));
    pool.seal_and_rotate();

    // The old segment file must exist on disk now (it's Sealed).
    EXPECT_TRUE(std::filesystem::exists(path1)) << "Sealed segment " << path1 << " missing from disk";

    // GC: release anything with max_lsn <= 42.
    pool.release_wal_up_to(42);

    // File must be gone.
    EXPECT_FALSE(std::filesystem::exists(path1)) << "GC failed to unlink " << path1;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4: Crash recovery — overflow (20 files, pool_capacity=16)
//         The pool should load the 16 most-recent files, silently dropping
//         the 4 oldest.
// ─────────────────────────────────────────────────────────────────────────────
TEST(SegmentPool, CrashRecoveryLoadsMostRecent) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid = make_test_uuid();
    constexpr uint8_t kCrashPoolCapacity = 16;
    constexpr uint8_t kCrashTarget = 1; // leave capacity for Active/Creating

    // Create 20 sealed WAL files with sequences 1..20.
    for (uint64_t seq = 1; seq <= 20; ++seq) {
        ASSERT_NO_FATAL_FAILURE(create_sealed_wal_file(dir.path,
                                                       seq,
                                                       kSlotSize,
                                                       uuid,
                                                       reader::BlockLayout::Gamma4K,
                                                       /*max_lsn=*/seq * 100));
    }

    // Boot the pool.  discover_existing_segments() must keep only the 16 most
    // recent (sequences 5–20) and drop sequences 1–4.
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

    // Sequences 1–4 must not be present in any slot.
    for (auto& s : snaps) {
        EXPECT_GE(s.sequence == 0 ? 5u : s.sequence, 5u)
            << "Stale sequence " << s.sequence << " should have been dropped";
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5: Files belonging to a different DB instance are rejected.
// ─────────────────────────────────────────────────────────────────────────────
TEST(SegmentPool, RejectsForeignInstanceFiles) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid_a = make_test_uuid();
    std::array<uint8_t, 16> uuid_b = uuid_a;
    uuid_b[0] ^= 0xFF; // different instance

    // Write a file belonging to instance B.
    ASSERT_NO_FATAL_FAILURE(create_sealed_wal_file(dir.path, 1, kSlotSize, uuid_b, reader::BlockLayout::Gamma4K));

    // Boot pool as instance A — the foreign file must be ignored.
    WalSegmentPool pool(dir.path, reader::BlockLayout::Gamma4K, kPoolCapacity, kTargetPoolSize, 2, uuid_a);

    auto snaps = pool.snapshot_segments();
    int non_empty = 0;
    for (auto& s : snaps) {
        if (s.state != pool::SegmentState::Empty)
            ++non_empty;
    }
    EXPECT_EQ(non_empty, 0) << "Foreign-instance file should have been rejected";
}