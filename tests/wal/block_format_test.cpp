
#include "stratadb/wal/block/delta_block.hpp"
#include "stratadb/wal/block/gamma_block.hpp"
#include "stratadb/wal/reader/validator.hpp"
#include "stratadb/wal/types.hpp"

#include <array>
#include <cstddef>
#include <cstring>
#include <gtest/gtest.h>
#include <span>
#include <string>
#include <vector>

using namespace stratadb::wal;
using namespace stratadb::wal::reader;

// Helpers
namespace {

template <std::size_t N>
std::span<const std::byte> as_bytes(const char (&lit)[N]) {
    return {reinterpret_cast<const std::byte*>(lit), N - 1};
}

std::span<const std::byte> sv_bytes(std::string_view sv) {
    return {reinterpret_cast<const std::byte*>(sv.data()), sv.size()};
}

// Reinterpret the block's raw storage as a uint8_t* for the validator helpers.
template <typename Block>
const std::uint8_t* raw(const Block& b) {
    return reinterpret_cast<const std::uint8_t*>(&b);
}

} // namespace

// GammaBlock tests

TEST(GammaBlock, AppendAndValidate) {
    alignas(4096) GammaBlock<4096> block{};
    block.init(42);

    EXPECT_TRUE(block.append(as_bytes("hello"), as_bytes("world")));
    EXPECT_TRUE(block.append(as_bytes("foo"), as_bytes("bar")));

    auto result = block.finalize(42);
    EXPECT_FALSE(result.memory_to_write.empty());

    // valid_data_end_offset stamped in the header
    std::uint32_t vde = 0;
    std::memcpy(&vde, raw(block) + 28 /*kGammaVdeOffset*/, sizeof(vde));
    EXPECT_GT(vde, static_cast<std::uint32_t>(sizeof(GammaBlock<4096>::Header)));

    auto val = validate_gamma_block(raw(block), 4096, vde);
    EXPECT_TRUE(val.checksum_ok);
    EXPECT_EQ(val.valid_data_end_offset, vde);
}

TEST(GammaBlock, CRCDetectsPayloadCorrupt) {
    alignas(4096) GammaBlock<4096> block{};
    block.init(1);
    ASSERT_TRUE(block.append(as_bytes("key"), as_bytes("value")));
    auto result = block.finalize(1);
    EXPECT_FALSE(result.memory_to_write.empty());
    std::uint32_t vde = 0;
    std::memcpy(&vde, raw(block) + 28, sizeof(vde));

    // Flip a byte deep in the payload (past header + record header).
    auto* mutable_raw = const_cast<std::uint8_t*>(raw(block));
    mutable_raw[sizeof(GammaBlock<4096>::Header) + 20] ^= 0xFF;

    auto val = validate_gamma_block(raw(block), 4096, vde);
    EXPECT_FALSE(val.checksum_ok) << "Checksum should fail after payload corruption";
}

TEST(GammaBlock, FullBlockRejectsFurtherAppends) {
    alignas(4096) GammaBlock<4096> block{};
    block.init(7);

    // Fill until append returns false.
    std::string big_val(200, 'X');
    int appended = 0;
    while (block.append(sv_bytes("k"), sv_bytes(big_val)))
        ++appended;

    EXPECT_GT(appended, 0) << "Expected at least one successful append";

    // One more must fail.
    EXPECT_FALSE(block.append(sv_bytes("overflow_key"), sv_bytes(big_val)));
}

TEST(GammaBlock, EmptyBlockFinalizes) {
    alignas(4096) GammaBlock<4096> block{};
    block.init(99);

    auto result = block.finalize(99);
    // An empty block still produces a valid memory span (the header alone).
    EXPECT_FALSE(result.memory_to_write.empty());

    std::uint32_t vde = 0;
    std::memcpy(&vde, raw(block) + 28, sizeof(vde));
    // vde should at least cover the header.
    EXPECT_GE(vde, static_cast<std::uint32_t>(sizeof(GammaBlock<4096>::Header)));

    auto val = validate_gamma_block(raw(block), 4096, vde);
    EXPECT_TRUE(val.checksum_ok);
}

TEST(GammaBlock, PartialFlushThenFinalize) {
    alignas(4096) GammaBlock<4096> block{};
    block.init(5);

    ASSERT_TRUE(block.append(as_bytes("partial"), as_bytes("flush")));
    auto pf = block.partial_flush();
    EXPECT_FALSE(pf.memory_to_write.empty());

    ASSERT_TRUE(block.append(as_bytes("second"), as_bytes("record")));
    auto fin = block.finalize(5);
    EXPECT_FALSE(fin.memory_to_write.empty());

    // finalize writes from offset 0 (header changed) — verify checksum.
    std::uint32_t vde = 0;
    std::memcpy(&vde, raw(block) + 28, sizeof(vde));
    EXPECT_TRUE(validate_gamma_block(raw(block), 4096, vde).checksum_ok);
}

TEST(GammaBlock, ValidDataEndOffsetIsStamped) {
    alignas(4096) GammaBlock<4096> block{};
    block.init(3);
    ASSERT_TRUE(block.append(as_bytes("x"), as_bytes("y")));
    auto result = block.finalize(3);

    if (result.memory_to_write.empty()) {
        FAIL() << "Expected finalize to produce a non-empty memory span";
    }

    std::uint32_t vde = 0;
    std::memcpy(&vde, raw(block) + 28, sizeof(vde));
    EXPECT_GT(vde, 0u);
    EXPECT_LE(vde, 4096u);
}

TEST(GammaBlock, SequenceNumberIsStoredInHeader) {
    alignas(4096) GammaBlock<4096> block{};
    block.init(12345);

    std::uint64_t seq = 0;
    std::memcpy(&seq, raw(block), sizeof(seq));
    EXPECT_EQ(seq, 12345u);
}

// DeltaBlock tests

TEST(DeltaBlock, AppendAndValidate) {
    alignas(4096) DeltaBlock<4096> block{};
    block.init(77);

    EXPECT_TRUE(block.append(as_bytes("delta_key"), as_bytes("delta_val")));
    auto result = block.finalize(77);
    EXPECT_FALSE(result.memory_to_write.empty());

    auto val = validate_delta_block(raw(block), 4096, 4096);
    EXPECT_TRUE(val.checksum_ok);
}

TEST(DeltaBlock, CRCDetectsPayloadCorrupt) {
    alignas(4096) DeltaBlock<4096> block{};
    block.init(10);
    ASSERT_TRUE(block.append(as_bytes("d_key"), as_bytes("d_val")));
    auto result = block.finalize(10);
    EXPECT_FALSE(result.memory_to_write.empty());

    // Corrupt a byte in the payload region (past the 16-byte header).
    auto* mutable_raw = const_cast<std::uint8_t*>(raw(block));
    mutable_raw[sizeof(DeltaBlock<4096>::Header) + 15] ^= 0xAA;

    auto val = validate_delta_block(raw(block), 4096, 4096);
    EXPECT_FALSE(val.checksum_ok);
    EXPECT_NE(val.first_failed_sector, UINT8_MAX);
}

TEST(DeltaBlock, SectorCRCSlotNeverContainsPayload) {
    // DeltaBlock stores a CRC at bytes [4092, 4096) of each sector.
    // After finalize, we verify the stored CRC at that offset validates the
    // preceding 4092 bytes.
    alignas(4096) DeltaBlock<4096> block{};
    block.init(20);
    ASSERT_TRUE(block.append(as_bytes("k"), as_bytes("v")));
    auto result = block.finalize(20);
    EXPECT_FALSE(result.memory_to_write.empty());

    // Manually verify sector 0 CRC.
    std::uint32_t stored_crc = 0;
    std::memcpy(&stored_crc, raw(block) + 4092, sizeof(stored_crc));
    const std::uint32_t computed = stratadb::utils::crc32c(raw(block), 4092);
    EXPECT_EQ(stored_crc, computed) << "Sector 0 CRC mismatch — records may have spilled into CRC slot";
}

TEST(DeltaBlock, EmptyBlockFinalizes) {
    alignas(4096) DeltaBlock<4096> block{};
    block.init(0);

    auto result = block.finalize(0);
    EXPECT_FALSE(result.memory_to_write.empty());

    auto val = validate_delta_block(raw(block), 4096, 4096);
    EXPECT_TRUE(val.checksum_ok);
}

TEST(DeltaBlock, FullBlockRejectsFurtherAppends) {
    alignas(4096) DeltaBlock<4096> block{};
    block.init(1);

    std::string big_val(200, 'D');
    int appended = 0;
    while (block.append(sv_bytes("kk"), sv_bytes(big_val)))
        ++appended;

    EXPECT_GT(appended, 0);
    EXPECT_FALSE(block.append(sv_bytes("overflow"), sv_bytes(big_val)));
}

// Validator dispatcher

TEST(BlockValidator, DispatcherRoutesByLayout) {
    alignas(4096) GammaBlock<4096> g{};
    g.init(1);
    ASSERT_TRUE(g.append(as_bytes("a"), as_bytes("b")));
    auto g_result = g.finalize(1);
    EXPECT_FALSE(g_result.memory_to_write.empty());

    std::uint32_t vde = 0;
    std::memcpy(&vde, raw(g) + 28, sizeof(vde));

    auto gamma_result = validate_block(BlockLayout::Gamma4K, raw(g), 4096, vde);
    EXPECT_TRUE(gamma_result.checksum_ok);

    alignas(4096) DeltaBlock<4096> d{};
    d.init(2);
    ASSERT_TRUE(d.append(as_bytes("c"), as_bytes("d")));
    auto d_result = d.finalize(2);
    EXPECT_FALSE(d_result.memory_to_write.empty());

    auto delta_result = validate_block(BlockLayout::Delta4K, raw(d), 4096, 4096);
    EXPECT_TRUE(delta_result.checksum_ok);
}

TEST(BlockValidator, TornWriteDefenseLayers) {
    // Layer 1: missing marker.
    auto tear = check_for_tear(/*marker_found=*/false, {true, UINT8_MAX, 100}, 0, 1);
    EXPECT_FALSE(tear.is_valid);
    EXPECT_EQ(tear.defense, TearDefense::MarkerMissing);

    // Layer 2: checksum failed.
    BlockValidationResult bad_crc{false, 0, 0};
    tear = check_for_tear(true, bad_crc, 0, 1);
    EXPECT_FALSE(tear.is_valid);
    EXPECT_EQ(tear.defense, TearDefense::ChecksumFailed);

    // Layer 3: LSN regression.
    BlockValidationResult good{true, UINT8_MAX, 100};
    tear = check_for_tear(true, good, /*lsn_previous=*/5, /*lsn_current=*/5); // not strictly greater
    EXPECT_FALSE(tear.is_valid);
    EXPECT_EQ(tear.defense, TearDefense::LsnRegression);

    // All layers pass.
    tear = check_for_tear(true, good, 0, 1);
    EXPECT_TRUE(tear.is_valid);
    EXPECT_EQ(tear.defense, TearDefense::None);
}