#pragma once

#include "stratadb/utils/xxhash.hpp"
#include "stratadb/wal/concepts.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>

namespace stratadb::wal {

// GammaBlock — NVMe / SATA SSD-optimised write-buffer format.
//
// A single XXH3-128 digest covers the entire sealed block (stored in the
// header).  Partial flushes overwrite the same sector range in place; NVMe
// AWUPF guarantees atomic overwrites up to the physical sector size so there
// is no torn-write risk and no per-sector overhead.
//
// Partial flush: the write cursor is rounded up to the next 4 KiB boundary
// and the gap is zeroed.  The flush_offset_ is rewound to the current sector
// start so the NEXT partial flush overlaps and overwrites the zero-padded tail
// — this is the key SSD physics difference from DeltaBlock.
//
// finalize(): computes the XXH3 digest over the entire written region,
// stores it in header.block_hash, and returns a span starting at offset 0
// (because the header in sector 0 changed).
//
// Usage: prefer DeltaBlock for spinning disks where per-sector CRCs give
//        meaningful seek-avoiding recovery properties.
template <std::size_t BlockSize = 16384>
struct alignas(4096) GammaBlock {
    static_assert(BlockSize % 4096 == 0, "BlockSize must be a multiple of the OS page size (4 KiB)");

    struct Header {
        std::uint64_t sequence_number{0};
        XXH128_hash_t block_hash{.low64 = 0, .high64 = 0}; // 16 bytes — XXH3-128
        std::uint16_t record_count{0};
        std::uint16_t flags{0};
        std::uint32_t _padding{0}; // pad to 32 bytes total
    } header;

    static_assert(sizeof(Header) == 32, "GammaBlock::Header must be exactly 32 bytes");

    alignas(8) std::array<std::byte, BlockSize - sizeof(Header)> arena{};

    std::size_t append_offset_{sizeof(Header)};
    std::size_t flush_offset_{0};

    void init(std::uint64_t seq) noexcept {
        header.sequence_number = seq;
        header.block_hash = {0, 0};
        header.record_count = 0;
        header.flags = 0;
        append_offset_ = sizeof(Header);
        flush_offset_ = 0;
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    [[nodiscard]] auto append(std::span<const std::byte> key, std::span<const std::byte> value) noexcept -> bool {
        const auto k_len = static_cast<std::uint32_t>(key.size());
        const auto v_len = static_cast<std::uint32_t>(value.size());
        const std::size_t total = sizeof(k_len) + sizeof(v_len) + k_len + v_len;

        if (append_offset_ + total > BlockSize) {
            return false;
        }

        auto* base = reinterpret_cast<std::byte*>(this);

        std::memcpy(base + append_offset_, &WALR_MARKER, sizeof(WALR_MARKER));
        append_offset_ += sizeof(WALR_MARKER);

        std::memcpy(base + append_offset_, &k_len, sizeof(k_len));
        append_offset_ += sizeof(k_len);

        std::memcpy(base + append_offset_, &v_len, sizeof(v_len));
        append_offset_ += sizeof(v_len);

        std::memcpy(base + append_offset_, key.data(), k_len);
        append_offset_ += k_len;

        std::memcpy(base + append_offset_, value.data(), v_len);
        append_offset_ += v_len;

        ++header.record_count;

        // Align to 8 bytes so the next record header is naturally aligned.
        append_offset_ = (append_offset_ + 7U) & ~static_cast<std::size_t>(7U);
        if (append_offset_ > BlockSize) {
            append_offset_ = BlockSize; // safety clamp (should never fire)
        }

        return true;
    }

    [[nodiscard]] auto partial_flush() noexcept -> FlushResult {
        const std::size_t start_offset = flush_offset_;
        const std::size_t end_offset = sector_aligned_end();

        // Zero-pad the tail of the current sector so the kernel sees a full
        // sector-aligned write — required for O_DIRECT.
        if (append_offset_ < end_offset) {
            std::memset(reinterpret_cast<std::byte*>(this) + append_offset_, 0, end_offset - append_offset_);
        }

        const auto span = std::span<const std::byte>(reinterpret_cast<const std::byte*>(this) + start_offset,
                                                     end_offset - start_offset);

        // SSD PHYSICS: rewind to the current sector boundary so the next
        // partial flush overlaps and overwrites — NVMe AWUPF handles this
        // atomically without a read-modify-write penalty.
        flush_offset_ = append_offset_ & ~static_cast<std::size_t>(4095U);

        return FlushResult{
            .memory_to_write = span,
            .block_internal_offset = start_offset,
            .max_lsn = header.sequence_number,
        };
    }

    [[nodiscard]] auto finalize(std::uint64_t /*seq*/) noexcept -> FlushResult {
        const std::size_t end_offset = sector_aligned_end();

        // Zero-pad the tail sector.
        if (append_offset_ < end_offset) {
            std::memset(reinterpret_cast<std::byte*>(this) + append_offset_, 0, end_offset - append_offset_);
        }

        // Stamp the whole-block XXH3-128 digest into the header.
        // Because we zero the hash field before computing, the digest is
        // deterministic regardless of previous partial flushes.
        header.block_hash = {0, 0};
        header.block_hash = utils::xxhash3_128(this, end_offset);

        // The header lives in sector 0, so we must rewrite from offset 0 to
        // include the freshly stamped digest.  NVMe handles large overwrites
        // atomically up to AWUPF without torn-write risk.
        const auto span = std::span<const std::byte>(reinterpret_cast<const std::byte*>(this), end_offset);

        flush_offset_ = end_offset;

        return FlushResult{
            .memory_to_write = span,
            .block_internal_offset = 0,
            .max_lsn = header.sequence_number,
        };
    }

  private:
    [[nodiscard]] auto sector_aligned_end() const noexcept -> std::size_t {
        const std::size_t aligned = (append_offset_ + 4095U) & ~static_cast<std::size_t>(4095U);
        return std::min(aligned, BlockSize);
    }
};

static_assert(WALBlockLayout<GammaBlock<4096>>, "GammaBlock<4096> does not satisfy WALBlockLayout");
static_assert(WALBlockLayout<GammaBlock<16384>>, "GammaBlock<16384> does not satisfy WALBlockLayout");

} // namespace stratadb::wal