#include "stratadb/wal/reader/reader.hpp"

#include "stratadb/io/unique_file_descriptor.hpp"
#include "stratadb/utils/os.hpp"
#include "stratadb/wal/block/delta_block.hpp"
#include "stratadb/wal/block/gamma_block.hpp"
#include "stratadb/wal/pool/segment_pool.hpp"
#include "stratadb/wal/slot/slot_header.hpp"

#include <algorithm>
#include <cstdlib>
#include <cstring>

namespace stratadb::wal::reader {

#pragma pack(push, 1)
struct SerializedRecordHeader {
    std::uint32_t marker;
    std::uint32_t key_len;
    std::uint32_t val_len;
};
#pragma pack(pop)

namespace {

// Byte offset of sequence_number in GammaBlock::Header and DeltaBlock::Header.
constexpr std::size_t kSeqFieldOffset = 0;

// Byte offset of valid_data_end_offset in GammaBlock::Header.
// Layout: [seq:8][hash:16][record_count:2][flags:2][valid_data_end_offset:4]
constexpr std::size_t kGammaVdeOffset = 28;

// DeltaBlock sector / CRC geometry.
constexpr std::size_t kDeltaSectorSize = 4096;
constexpr std::size_t kDeltaCrcOffset = 4092; // CRC at [4092, 4096) within each sector

// Record framing: WALR_MARKER(4) + key_len(4) + val_len(4).
constexpr std::size_t kRecordHeaderSize = sizeof(SerializedRecordHeader);

} // namespace

WalReader::WalReader(pool::WalSegmentPool& wal_segment_pool, BlockLayout layout)
    : wal_segment_pool_(wal_segment_pool)
    , layout_(layout)
    , read_buf_raw_(std::aligned_alloc(kReadBufAlign, kReadBufSize))
    , read_buf_(static_cast<std::uint8_t*>(read_buf_raw_)) {}

WalReader::~WalReader() {
    std::free(read_buf_raw_);
}

auto WalReader::block_size() const noexcept -> std::size_t {
    switch (layout_) {
        case BlockLayout::Gamma16K:
            return 16384;
        case BlockLayout::Gamma4K:
            [[fallthrough]];
        case BlockLayout::Delta4K:
            return 4096;
    }
    return 4096;
}

auto WalReader::header_size() const noexcept -> std::size_t {
    // GammaBlock::Header = 32 bytes (static_assert'd in gamma_block.hpp).
    // DeltaBlock::Header = 16 bytes (static_assert'd in delta_block.hpp).
    return (layout_ == BlockLayout::Delta4K) ? sizeof(DeltaBlock<>::Header) : sizeof(GammaBlock<>::Header);
}

auto WalReader::recover(RecordCallback callback) noexcept -> RecoveryResult {
    RecoveryResult result{};
    auto snapshots = wal_segment_pool_.snapshot_segments();

    // Only Sealed and Active slots contain data.
    const auto [first, last] = std::ranges::remove_if(snapshots, [](const auto& snap) -> auto {
        return snap.state != pool::SegmentState::Sealed && snap.state != pool::SegmentState::Active;
    });
    snapshots.erase(first, last);

    // Process sequentially by sequence number.
    std::ranges::sort(snapshots, [](const auto& a, const auto& b) -> bool { return a.sequence < b.sequence; });

    std::uint64_t prev_lsn = 0; // track the last seen LSN to detect gaps in the WAL

    for (const auto& snap : snapshots) {
        auto [status, valid_end] = recover_slot(snap, callback, prev_lsn, result.records_recovered);

        if (status != RecoveryStatus::Clean) {
            result.status = status;
            result.last_valid_lsn = prev_lsn;

            result.torn_slot_index = snap.pool_index;
            result.torn_slot_valid_end = valid_end;

            return result; // stop recovery on first sign of trouble
        }
    }

    result.status = RecoveryStatus::Clean;
    result.last_valid_lsn = prev_lsn;
    return result;
}

auto WalReader::recover_slot(const pool::WalSegmentPool::SegmentSnapshot& snap,
                             RecordCallback& cb,
                             std::uint64_t& prev_lsn,
                             std::uint64_t& records_out) noexcept -> std::pair<RecoveryStatus, std::uint64_t> {
    // Open with O_DIRECT: our buffer (4096-aligned) and offsets (multiples of
    // 4096) satisfy kernel alignment requirements on both ext4 and XFS.
    // open_direct uses O_RDWR; semantically we only read, but WAL files are
    // always writable by the process that owns them.
    auto fd_result = utils::os::open_direct(wal_segment_pool_.format_path(snap.sequence));
    if (!fd_result) {
        return {RecoveryStatus::IOError, snap.write_offset};
    }
    io::UniqueFd fd{*fd_result};

    const std::size_t blk_size = block_size();
    const std::size_t hdr_sz = header_size();

    // scan_limit: the first byte we must NOT read past.
    //   Sealed slot : footer.sealed_write_offset (exact last byte of user data).
    //   Active slot : data_end_offset_ (conservative; zero-block check stops early).
    const std::uint64_t scan_limit = snap.write_offset;

    // Blocks begin immediately after the 4096-byte WalSlotHeader.
    std::uint64_t offset = slot::WalSlotHeader::SIZE;
    std::uint64_t last_good_offset = offset;

    while (offset + blk_size <= scan_limit) {
        auto io_res = utils::os::read_exact(fd.get(), read_buf_, blk_size, offset);
        if (!io_res) {
            return {RecoveryStatus::IOError, last_good_offset};
        }

        // lsn_generator_ starts at 1, so 0 is the "never written" sentinel.
        // This fires for the active recovery slot's unwritten tail.
        std::uint64_t seq = 0;
        std::memcpy(&seq, read_buf_ + kSeqFieldOffset, sizeof(seq));
        if (seq == 0) {
            return {RecoveryStatus::Clean, last_good_offset};
        }

        // Records are always packed starting immediately after the block header.
        // A missing marker means either: no records were written (impossible for
        // a sealed block), or the data region is corrupt / torn.
        bool marker_found = false;
        if (blk_size >= hdr_sz + sizeof(std::uint32_t)) {
            std::uint32_t first_word = 0;
            std::memcpy(&first_word, read_buf_ + hdr_sz, sizeof(first_word));
            marker_found = (first_word == WALR_MARKER);
        }

        // validate the checksum
        BlockValidationResult validation{};

        std::uint32_t vde = 0; // valid data end offset, only for gamma blocks
        if (layout_ == BlockLayout::Gamma4K || layout_ == BlockLayout::Gamma16K) {
            std::memcpy(&vde, read_buf_ + kGammaVdeOffset, sizeof(vde));
            validation = validate_gamma_block(read_buf_, blk_size, static_cast<std::size_t>(vde));
        } else {
            validation = validate_delta_block(read_buf_, blk_size, blk_size);
        }

        // Torn write detection:
        auto tear = check_for_tear(marker_found, validation, prev_lsn, seq);
        if (!tear.is_valid) {
            return {RecoveryStatus::TornWrite, last_good_offset};
        }

        // parse the records in the block:
        bool parse_ok = false;
        if (layout_ == BlockLayout::Gamma4K || layout_ == BlockLayout::Gamma16K) {
            parse_ok = parse_gamma_block(read_buf_, static_cast<std::size_t>(vde), seq, cb, records_out);
        } else {
            parse_ok = parse_delta_block(read_buf_, blk_size, seq, cb, records_out);
        }

        prev_lsn = seq;
        last_good_offset = offset + blk_size;
        offset += blk_size;

        if (!parse_ok) {
            // Truncated record inside a checksum-valid block: shouldn't happen in
            // normal operation but treat conservatively as a partial write.
            return {RecoveryStatus::TornWrite, last_good_offset};
        }
    }

    return {RecoveryStatus::Clean, last_good_offset};
}

// Parse Gamma Block
auto WalReader::parse_gamma_block(const std::uint8_t* block_buf,
                                  std::size_t valid_data_end,
                                  std::uint64_t block_seq,
                                  RecordCallback& cb,
                                  std::uint64_t& records_out) noexcept -> bool {
    // GammaBlock::Header = 32 bytes; records begin immediately after.
    constexpr std::size_t kHdrSize = sizeof(GammaBlock<>::Header);

    std::size_t cursor = kHdrSize;

    while (cursor + kRecordHeaderSize <= valid_data_end) {
        // Parse record header.
        SerializedRecordHeader rec_hdr{};
        std::memcpy(&rec_hdr, block_buf + cursor, sizeof(rec_hdr));

        if (rec_hdr.marker != WALR_MARKER) {
            // Zero-padding written by sector_aligned_end() zero-fill — clean stop.
            break;
        }

        const std::size_t record_end = cursor + kRecordHeaderSize + rec_hdr.key_len + rec_hdr.val_len;
        if (record_end > valid_data_end) {
            // Truncated payload inside a hash-valid block: data integrity failure.
            return false;
        }

        cb(RecoveredRecord{
            .lsn = block_seq,
            .key = {reinterpret_cast<const std::byte*>(block_buf + cursor + kRecordHeaderSize), rec_hdr.key_len},
            .value = {reinterpret_cast<const std::byte*>(block_buf + cursor + kRecordHeaderSize + rec_hdr.key_len),
                      rec_hdr.val_len},
        });
        ++records_out;

        cursor = (record_end + GammaBlock<>::ALIGNMENT_MASK) & ~static_cast<std::size_t>(GammaBlock<>::ALIGNMENT_MASK);
    }

    return true;
}

auto WalReader::linearize_delta(const std::uint8_t* raw, std::size_t raw_size) noexcept -> std::size_t {
    constexpr std::size_t kDeltaHdrSize = sizeof(DeltaBlock<>::Header);

    std::size_t dst = 0;
    std::size_t src = kDeltaHdrSize;

    while (src < raw_size && dst < kScratchSize) {
        const std::size_t intra = src & (kDeltaSectorSize - 1);

        if (intra >= kDeltaCrcOffset) {
            src = (src & ~(kDeltaSectorSize - 1)) + kDeltaSectorSize; // skip CRC sector
            continue;
        }

        std::uint32_t marker = 0;
        std::memcpy(&marker, raw + src, sizeof(marker));
        if (marker != WALR_MARKER) {
            break; // No more records
        }

        // How many payload bytes remain before the next CRC slot.
        const std::size_t to_crc = kDeltaCrcOffset - intra;
        const std::size_t to_src_end = raw_size - src;
        const std::size_t to_dst_end = kScratchSize - dst;
        const std::size_t chunk = std::min({to_crc, to_src_end, to_dst_end});

        std::memcpy(scratch_ + dst, raw + src, chunk);
        dst += chunk;
        src += chunk;
    }

    return dst;
}

auto WalReader::parse_delta_block(const std::uint8_t* block,
                                  std::size_t raw_size,
                                  std::uint64_t block_seq,
                                  RecordCallback& cb,
                                  std::uint64_t& records_out) noexcept -> bool {
    // Linearize: copy payload bytes (header-skipped, CRC-stripped) into scratch_.
    // Records in scratch_ are packed tightly — DeltaBlock::append() does NOT
    // apply 8-byte alignment.
    const std::size_t logical_bytes = linearize_delta(block, raw_size);
    const auto* data = reinterpret_cast<const std::uint8_t*>(scratch_);

    std::size_t cursor = 0;
    while (cursor + kRecordHeaderSize <= logical_bytes) {
        std::uint32_t marker = 0;
        std::memcpy(&marker, data + cursor, sizeof(marker));

        if (marker != WALR_MARKER) {
            // Zero-padding written by DeltaBlock::partial_flush() — clean stop.
            break;
        }

        std::uint32_t k_len = 0;
        std::uint32_t v_len = 0;
        std::memcpy(&k_len, data + cursor + 4, sizeof(k_len));
        std::memcpy(&v_len, data + cursor + 8, sizeof(v_len));

        const std::size_t record_end = cursor + kRecordHeaderSize + k_len + v_len;
        if (record_end > logical_bytes) {
            return false; // truncated record inside a CRC-valid block
        }

        cb(RecoveredRecord{
            .lsn = block_seq,
            .key = {reinterpret_cast<const std::byte*>(data + cursor + kRecordHeaderSize), k_len},
            .value = {reinterpret_cast<const std::byte*>(data + cursor + kRecordHeaderSize + k_len), v_len},
        });
        ++records_out;

        cursor = record_end; // tight packing — no alignment
    }

    return true;
}

} // namespace stratadb::wal::reader