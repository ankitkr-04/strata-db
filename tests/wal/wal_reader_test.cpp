// tests/wal/reader_test.cpp
//
// Tests for WalReader crash-recovery path.
//
// Coverage:
//   CleanReplay          — two sealed segments, 5 records total, all recovered
//                          in the correct order
//   TornWriteDetected    — flip a bit in GammaBlock payload → reader stops at
//                          that block and returns TornWrite status
//   ZeroPaddedTailStops  — active segment whose tail is all-zeros → reader
//                          terminates cleanly at seq==0 (not IOError)
//   MultiSegmentOrder    — records from three segments recovered in sequence order

#include "stratadb/wal/pool/segment_pool.hpp"
#include "stratadb/wal/pool/segment_state.hpp"
#include "stratadb/wal/reader/reader.hpp"
#include "stratadb/wal/reader/validator.hpp"
#include "stratadb/wal/slot/slot_footer.hpp"
#include "stratadb/wal/slot/slot_header.hpp"
#include "wal_test_helpers.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace stratadb::wal::test;
using namespace stratadb::wal;
using stratadb::wal::pool::WalSegmentPool;
using stratadb::wal::reader::RecoveredRecord;
using stratadb::wal::reader::RecoveryStatus;
using stratadb::wal::reader::WalReader;

static constexpr uint64_t kSlotSize = 64LL * 1024;
static constexpr uint8_t kPoolCap = 8;
static constexpr uint8_t kTargetPool = 1;

// ─────────────────────────────────────────────────────────────────────────────
// File-level helper: create a sealed segment with specific blocks
// ─────────────────────────────────────────────────────────────────────────────
// Creates a single .log file with N GammaBlock<4096> instances, then seals it.
// Returns the file write offset after all blocks (used as sealed_write_offset).
static auto make_sealed_segment(const std::filesystem::path& dir,
                                uint64_t seq,
                                const std::array<uint8_t, 16>& uuid,
                                // Each entry is one block's worth of records.
                                const std::vector<std::vector<std::pair<std::string, std::string>>>& blocks_data)
    -> uint64_t {

    using slot::WalSlotFooter;
    using slot::WalSlotHeader;

    auto path = segment_path(dir, seq);
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
        return 0;

    // Pre-allocate
    ::posix_fallocate(fd, 0, static_cast<off_t>(kSlotSize));

    // Header
    WalSlotHeader hdr{};
    hdr.magic = WalSlotHeader::MAGIC;
    hdr.version = WalSlotHeader::VERSION;
    hdr.block_layout = static_cast<uint8_t>(reader::BlockLayout::Gamma4K);
    hdr.slot_sequence = seq;
    hdr.slot_max_bytes = kSlotSize;
    hdr.db_instance_uuid = uuid;
    slot::seal_header_crc(hdr);
    ::pwrite(fd, &hdr, sizeof(hdr), 0);

    // Blocks
    uint64_t offset = WalSlotHeader::SIZE;
    uint64_t lsn_base = seq * 1000; // give each segment a distinct LSN range
    for (auto& recs : blocks_data) {
        offset = write_gamma_block_to_fd(fd, offset, lsn_base++, recs);
    }

    // Footer
    WalSlotFooter footer{};
    footer.magic = WalSlotFooter::MAGIC;
    footer.slot_sequence = seq;
    footer.sealed_write_offset = offset;
    footer.min_lsn = seq * 1000;
    footer.max_lsn = lsn_base - 1;
    slot::seal_footer_crc(footer);
    ::pwrite(fd, &footer, sizeof(footer), static_cast<off_t>(kSlotSize - WalSlotFooter::SIZE));

    ::fdatasync(fd);
    ::close(fd);
    return offset;
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: Clean replay across two segments
// ─────────────────────────────────────────────────────────────────────────────
TEST(WalReader, CleanReplay) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid = make_test_uuid();

    // Segment 1: one block with 2 records.
    make_sealed_segment(dir.path, 1, uuid, {{{"key1", "val1"}, {"key2", "val2"}}});

    // Segment 2: one block with 3 records.
    make_sealed_segment(dir.path, 2, uuid, {{{"key3", "val3"}, {"key4", "val4"}, {"key5", "val5"}}});

    WalSegmentPool pool(dir.path, reader::BlockLayout::Gamma4K, kSlotSize, kPoolCap, kTargetPool, uuid);

    WalReader reader(pool, reader::BlockLayout::Gamma4K);

    std::vector<std::string> keys;
    auto result = reader.recover([&](const RecoveredRecord& rec) {
        keys.emplace_back(reinterpret_cast<const char*>(rec.key.data()), rec.key.size());
    });

    EXPECT_EQ(result.status, RecoveryStatus::Clean);
    EXPECT_EQ(result.records_recovered, 5u);
    ASSERT_EQ(keys.size(), 5u);
    EXPECT_EQ(keys[0], "key1");
    EXPECT_EQ(keys[1], "key2");
    EXPECT_EQ(keys[2], "key3");
    EXPECT_EQ(keys[3], "key4");
    EXPECT_EQ(keys[4], "key5");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: Torn write detected — corrupt payload byte → TornWrite status
// ─────────────────────────────────────────────────────────────────────────────
TEST(WalReader, TornWriteDetected) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid = make_test_uuid();

    // Valid segment 1.
    make_sealed_segment(dir.path, 1, uuid, {{{"before_tear", "ok"}}});

    // Segment 2: write valid, then flip a byte in the payload to corrupt checksum.
    const uint64_t seg2_offset = make_sealed_segment(dir.path, 2, uuid, {{{"torn", "data"}}});
    (void)seg2_offset;

    // Corrupt one byte in the block payload region (past the 32-byte header,
    // past the WALR marker and length fields, deep in the value bytes).
    auto corrupt_path = segment_path(dir.path, 2);
    int cfd = ::open(corrupt_path.c_str(), O_RDWR, 0);
    ASSERT_GE(cfd, 0);
    // Offset: WalSlotHeader(4096) + GammaBlock.Header(32) + record_header(12) + key("torn" = 4 bytes) + 2 bytes into
    // value
    const off_t corrupt_off = 4096 + 32 + 12 + 4 + 2;
    char bad = 0xFF;
    ::pwrite(cfd, &bad, 1, corrupt_off);
    ::fdatasync(cfd);
    ::close(cfd);

    WalSegmentPool pool(dir.path, reader::BlockLayout::Gamma4K, kSlotSize, kPoolCap, kTargetPool, uuid);

    WalReader reader(pool, reader::BlockLayout::Gamma4K);

    std::vector<std::string> keys;
    auto result = reader.recover([&](const RecoveredRecord& rec) {
        keys.emplace_back(reinterpret_cast<const char*>(rec.key.data()), rec.key.size());
    });

    EXPECT_EQ(result.status, RecoveryStatus::TornWrite);
    // Records from the valid segment 1 should have been recovered.
    EXPECT_GE(result.records_recovered, 1u);
    EXPECT_EQ(keys[0], "before_tear");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: Zero-padded tail stops cleanly
// ─────────────────────────────────────────────────────────────────────────────
TEST(WalReader, ZeroPaddedTailStopsClean) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid = make_test_uuid();

    // Create an "Active"-style segment: valid header + 1 block of data,
    // rest is zeros (fallocate initialises to zero on most filesystems),
    // no sealed footer.
    using slot::WalSlotHeader;
    auto path = segment_path(dir.path, 1);
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);
    ::posix_fallocate(fd, 0, static_cast<off_t>(kSlotSize));

    WalSlotHeader hdr{};
    hdr.magic = WalSlotHeader::MAGIC;
    hdr.version = WalSlotHeader::VERSION;
    hdr.block_layout = static_cast<uint8_t>(reader::BlockLayout::Gamma4K);
    hdr.slot_sequence = 1;
    hdr.slot_max_bytes = kSlotSize;
    hdr.db_instance_uuid = uuid;
    slot::seal_header_crc(hdr);
    ::pwrite(fd, &hdr, sizeof(hdr), 0);

    // Write one valid block at the data start.
    write_gamma_block_to_fd(fd, WalSlotHeader::SIZE, 1001 /*lsn*/, {{"k_active", "v_active"}});

    ::fdatasync(fd);
    ::close(fd);

    // No footer → discovered as Active during recovery.
    WalSegmentPool pool(dir.path, reader::BlockLayout::Gamma4K, kSlotSize, kPoolCap, kTargetPool, uuid);

    WalReader reader(pool, reader::BlockLayout::Gamma4K);
    uint64_t records = 0;
    auto result = reader.recover([&](const RecoveredRecord&) { ++records; });

    EXPECT_EQ(result.status, RecoveryStatus::Clean) << "Zero-padded tail should produce a Clean (not IOError) result";
    EXPECT_GE(records, 1u) << "The one valid block should have been recovered";
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4: Multiple segments recovered in sequence order
// ─────────────────────────────────────────────────────────────────────────────
TEST(WalReader, MultiSegmentOrderPreserved) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid = make_test_uuid();

    // Write three segments with one unique key each.
    make_sealed_segment(dir.path, 1, uuid, {{{"seq1_k", "v"}}});
    make_sealed_segment(dir.path, 2, uuid, {{{"seq2_k", "v"}}});
    make_sealed_segment(dir.path, 3, uuid, {{{"seq3_k", "v"}}});

    WalSegmentPool pool(dir.path, reader::BlockLayout::Gamma4K, kSlotSize, kPoolCap, kTargetPool, uuid);

    WalReader reader(pool, reader::BlockLayout::Gamma4K);

    std::vector<std::string> keys;
    auto result = reader.recover([&](const RecoveredRecord& rec) {
        keys.emplace_back(reinterpret_cast<const char*>(rec.key.data()), rec.key.size());
    });

    EXPECT_EQ(result.status, RecoveryStatus::Clean);
    ASSERT_EQ(keys.size(), 3u);
    // Segments recovered in monotonic sequence order.
    EXPECT_EQ(keys[0], "seq1_k");
    EXPECT_EQ(keys[1], "seq2_k");
    EXPECT_EQ(keys[2], "seq3_k");
}