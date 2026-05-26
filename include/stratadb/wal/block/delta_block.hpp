#pragma once

#include "stratadb/utils/hash.hpp"
#include "stratadb/wal/concepts.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>

namespace stratadb::wal {

// DeltaBlock — HDD-optimised write-buffer format.
//
// Each 4 KiB sector carries its own CRC32c (Castagnoli) in the final 4 bytes,
// leaving 4092 bytes of payload per sector.  This means a single torn write
// at the sector boundary is detectable during WAL recovery without reading the
// entire block — critical for rotational media where random I/O is expensive.
//
// Partial flush: the current incomplete sector is zeroed to its CRC slot,
// sealed, and the write cursor advances to the NEXT sector boundary.
// This avoids later RMW read-modify-write cycles on spinning disks.
//
// Usage: prefer GammaBlock for NVMe / SATA SSD where AWUPF guarantees
//        atomic 4 KiB / 16 KiB writes and large-block XXH3 is cheaper.
template <std::size_t BlockSize = 4096>
struct alignas(4096) DeltaBlock {
    static_assert(BlockSize % 4096 == 0, "BlockSize must be a multiple of the 4 KiB sector size");

    static constexpr std::size_t SECTOR_SIZE = 4096;
    static constexpr std::size_t SECTOR_MASK = SECTOR_SIZE - 1;
    static constexpr std::size_t CRC_FIELD_SIZE = sizeof(std::uint32_t);
    static constexpr std::size_t CRC_OFFSET = SECTOR_SIZE - CRC_FIELD_SIZE; // 4092

    struct Header {
        std::uint64_t sequence_number{0};
        std::uint16_t record_count{0};
        std::uint16_t flags{0};
        std::uint32_t _padding{0};
    } header;

    static_assert(sizeof(Header) == 16, "Header must be exactly 16 bytes");

    alignas(8) std::array<std::byte, BlockSize - sizeof(Header)> arena{};

    std::size_t append_offset_{sizeof(Header)};
    std::size_t flush_offset_{0};

    void init(std::uint64_t seq) noexcept {
        header.sequence_number = seq;
        header.record_count = 0;
        header.flags = 0;
        append_offset_ = sizeof(Header);
        flush_offset_ = 0;
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    [[nodiscard]] auto append(std::span<const std::byte> key, std::span<const std::byte> value) noexcept -> bool {
        const auto k_len = static_cast<std::uint32_t>(key.size());
        const auto v_len = static_cast<std::uint32_t>(value.size());
        const std::size_t total_required = sizeof(WALR_MARKER) + sizeof(k_len) + sizeof(v_len) + k_len + v_len;

        // Simulate the append to check capacity before committing any bytes.
        std::size_t simulated = append_offset_;
        std::size_t remaining = total_required;
        while (remaining > 0) {
            std::size_t intra = simulated & SECTOR_MASK;
            if (intra == CRC_OFFSET) {
                simulated += CRC_FIELD_SIZE;
                intra = 0;
            }
            std::size_t chunk = std::min(remaining, CRC_OFFSET - intra);
            simulated += chunk;
            remaining -= chunk;
        }
        if (simulated > BlockSize) {
            return false;
        }

        const std::array<std::span<const std::byte>, 5> parts = {
            std::span<const std::byte>(reinterpret_cast<const std::byte*>(&WALR_MARKER), sizeof(WALR_MARKER)),
            std::span<const std::byte>(reinterpret_cast<const std::byte*>(&k_len), sizeof(k_len)),
            std::span<const std::byte>(reinterpret_cast<const std::byte*>(&v_len), sizeof(v_len)),
            key,
            value,
        };

        auto* block_start = reinterpret_cast<std::byte*>(this);

        for (auto part : parts) {
            while (!part.empty()) {
                const std::size_t intra = append_offset_ & SECTOR_MASK;
                if (intra == CRC_OFFSET) {
                    // Seal the completed sector before crossing its boundary.
                    const std::uint32_t crc = utils::crc32c(block_start + (append_offset_ & ~SECTOR_MASK), CRC_OFFSET);
                    std::memcpy(block_start + append_offset_, &crc, sizeof(crc));
                    append_offset_ += CRC_FIELD_SIZE;
                }
                const std::size_t avail = CRC_OFFSET - (append_offset_ & SECTOR_MASK);
                const std::size_t chunk = std::min(part.size(), avail);
                std::memcpy(block_start + append_offset_, part.data(), chunk);
                append_offset_ += chunk;
                part = part.subspan(chunk);
            }
        }

        ++header.record_count;
        return true;
    }

    [[nodiscard]] auto partial_flush() noexcept -> FlushResult {
        const std::size_t start_offset = flush_offset_;
        auto* block_start = reinterpret_cast<std::byte*>(this);

        if ((append_offset_ & SECTOR_MASK) != 0) {
            const std::size_t sector_start = append_offset_ & ~SECTOR_MASK;
            const std::size_t crc_abs_offset = sector_start + CRC_OFFSET;

            // Zero-pad from the current write cursor up to the CRC slot.
            if (append_offset_ < crc_abs_offset) {
                std::memset(block_start + append_offset_, 0, crc_abs_offset - append_offset_);
            }

            const std::uint32_t tail_crc = utils::crc32c(block_start + sector_start, CRC_OFFSET);
            std::memcpy(block_start + crc_abs_offset, &tail_crc, sizeof(tail_crc));

            // HDD PHYSICS: seal the entire sector so the next append starts
            // on a fresh boundary — eliminates read-modify-write on spinning media.
            append_offset_ = sector_start + SECTOR_SIZE;
        }

        const auto span = std::span<const std::byte>(reinterpret_cast<const std::byte*>(this) + start_offset,
                                                     append_offset_ - start_offset);

        flush_offset_ = append_offset_;

        return FlushResult{
            .memory_to_write = span,
            .block_internal_offset = start_offset,
            .max_lsn = header.sequence_number,
        };
    }

    // DeltaBlock sealing is identical to a partial flush of the remaining data:
    // per-sector CRCs are already written incrementally by append().
    [[nodiscard]] auto finalize(std::uint64_t /*seq*/) noexcept -> FlushResult {
        return partial_flush();
    }
};

static_assert(WALBlockLayout<DeltaBlock<4096>>, "DeltaBlock<4096> does not satisfy WALBlockLayout");

} // namespace stratadb::wal