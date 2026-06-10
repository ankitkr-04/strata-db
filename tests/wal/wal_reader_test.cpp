#include "../support/wal_test_helpers.hpp"
#include "stratadb/wal/pool/segment_pool.hpp"
#include "stratadb/wal/reader/reader.hpp"
#include "stratadb/wal/slot/slot_header.hpp"

#include <cstdint>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <string>
#include <unistd.h>
#include <vector>

using namespace stratadb::wal::test;
using namespace stratadb::wal;
using stratadb::wal::pool::WalSegmentPool;
using stratadb::wal::reader::RecoveredRecord;
using stratadb::wal::reader::RecoveryStatus;
using stratadb::wal::reader::WalReader;

static constexpr std::uint64_t kSlotSize = 64LL * 1024;
static constexpr std::uint8_t kPoolCap = 8;
static constexpr std::uint8_t kTargetPool = 1;

TEST(WalReader, CleanReplay) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid = make_test_uuid();

    make_sealed_segment(dir.path, 1, kSlotSize, uuid, {{{"key1", "val1"}, {"key2", "val2"}}});
    make_sealed_segment(dir.path, 2, kSlotSize, uuid, {{{"key3", "val3"}, {"key4", "val4"}, {"key5", "val5"}}});

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

TEST(WalReader, TornWriteDetected) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid = make_test_uuid();

    make_sealed_segment(dir.path, 1, kSlotSize, uuid, {{{"before_tear", "ok"}}});

    make_sealed_segment(dir.path, 2, kSlotSize, uuid, {{{"torn", "data"}}});

    auto corrupt_path = segment_path(dir.path, 2);
    int cfd = ::open(corrupt_path.c_str(), O_RDWR, 0);
    ASSERT_GE(cfd, 0);
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
    EXPECT_GE(result.records_recovered, 1u);
    EXPECT_EQ(keys[0], "before_tear");
}

TEST(WalReader, ZeroPaddedTailStopsClean) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid = make_test_uuid();

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

    write_gamma_block_to_fd(fd, WalSlotHeader::SIZE, /*lsn=*/1001, {{"k_active", "v_active"}});

    ::fdatasync(fd);
    ::close(fd);

    WalSegmentPool pool(dir.path, reader::BlockLayout::Gamma4K, kSlotSize, kPoolCap, kTargetPool, uuid);

    WalReader reader(pool, reader::BlockLayout::Gamma4K);
    uint64_t records = 0;
    auto result = reader.recover([&](const RecoveredRecord&) { ++records; });

    EXPECT_EQ(result.status, RecoveryStatus::Clean) << "Zero-padded tail should produce a Clean (not IOError) result";
    EXPECT_GE(records, 1u) << "The one valid block should have been recovered";
}

TEST(WalReader, MultiSegmentOrderPreserved) {
    TempDir dir;
    WAL_SKIP_IF_NO_ODIRECT(dir.path);

    const auto uuid = make_test_uuid();

    make_sealed_segment(dir.path, 1, kSlotSize, uuid, {{{"seq1_k", "v"}}});
    make_sealed_segment(dir.path, 2, kSlotSize, uuid, {{{"seq2_k", "v"}}});
    make_sealed_segment(dir.path, 3, kSlotSize, uuid, {{{"seq3_k", "v"}}});

    WalSegmentPool pool(dir.path, reader::BlockLayout::Gamma4K, kSlotSize, kPoolCap, kTargetPool, uuid);

    WalReader reader(pool, reader::BlockLayout::Gamma4K);

    std::vector<std::string> keys;
    auto result = reader.recover([&](const RecoveredRecord& rec) {
        keys.emplace_back(reinterpret_cast<const char*>(rec.key.data()), rec.key.size());
    });

    EXPECT_EQ(result.status, RecoveryStatus::Clean);
    ASSERT_EQ(keys.size(), 3u);
    EXPECT_EQ(keys[0], "seq1_k");
    EXPECT_EQ(keys[1], "seq2_k");
    EXPECT_EQ(keys[2], "seq3_k");
}
