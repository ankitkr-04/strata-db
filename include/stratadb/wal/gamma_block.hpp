#pragma once

#include "wal_concept.hpp"
#include "xxhash.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>

namespace stratadb::wal {
[[nodiscard]] auto compute_wal_block_hash(const void* data, std::size_t length) noexcept -> XXH128_hash_t;

template <size_t BlockSize = 16384> // 16KiB default
struct alignas(4096) GammaBlock {
    static_assert(BlockSize % 4096 == 0, "BlockSize must be a multiple of the OS page size.");

    struct Header {
        uint64_t sequence_number{0};
        XXH128_hash_t block_hash{.low64 = 0, .high64 = 0}; // 16 bytes: xxHash3 128-bit
        uint16_t record_count{0};
        uint16_t flags{0};
        uint32_t _padding{0}; // Padding to make Header size a multiple of 8 bytes (total 32 bytes)
    } header;

    static_assert(sizeof(Header) == 32, "Header size must be exactly 32 bytes for proper alignment.");

    alignas(8) std::array<std::byte, BlockSize - sizeof(Header)> arena;

    std::size_t append_offset_{sizeof(Header)};
    std::size_t flush_offset_{0};

    void init(uint64_t seq) noexcept {
        header.sequence_number = seq;
        header.block_hash = {0, 0};
        header.record_count = 0;
        header.flags = 0;
        append_offset_ = sizeof(Header);
        flush_offset_ = 0;
    }

    // Implements WalBlockLayout
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    [[nodiscard]] auto append(std::span<const std::byte> key, std::span<const std::byte> value) noexcept -> bool {
        const auto k_len = static_cast<uint32_t>(key.size());
        const auto v_len = static_cast<uint32_t>(value.size());

        const size_t total_required = sizeof(k_len) + sizeof(v_len) + k_len + v_len;

        // Check capacity
        if (append_offset_ + total_required > BlockSize) {
            return false;
        }

        auto* block_start = reinterpret_cast<std::byte*>(this);

        //  Write lengths and data sequentially
        std::memcpy(block_start + append_offset_, &k_len, sizeof(k_len));
        append_offset_ += sizeof(k_len);

        std::memcpy(block_start + append_offset_, &v_len, sizeof(v_len));
        append_offset_ += sizeof(v_len);

        std::memcpy(block_start + append_offset_, key.data(), k_len);
        append_offset_ += k_len;

        std::memcpy(block_start + append_offset_, value.data(), v_len);
        append_offset_ += v_len;

        header.record_count++;

        append_offset_ = (append_offset_ + 7U) & ~static_cast<std::size_t>(7U); // Align to 8 bytes

        if (append_offset_ > BlockSize)
            append_offset_ = BlockSize; // Safety check to prevent overflow

        return true;
    }

    [[nodiscard]] auto partial_flush() noexcept -> FlushResult {
        size_t start_offset = flush_offset_;
        size_t end_offset = (append_offset_ + 4095) & ~4095ULL; // Align up to 4KiB
        if (end_offset > BlockSize)
            end_offset = BlockSize;

        if (append_offset_ < end_offset) {
            std::memset(reinterpret_cast<std::byte*>(this) + append_offset_, 0, end_offset - append_offset_);
        }

        auto span = std::span<const std::byte>(reinterpret_cast<const std::byte*>(this) + start_offset,
                                               end_offset - start_offset);

        // SSD PHYSICS: Overlapping overwrites! The next append stays in the same sector.
        // The hardware AWUPF guarantees it won't tear.
        flush_offset_ = append_offset_ & ~4095ULL;

        return FlushResult{.memory_to_write = span,
                           .block_internal_offset = start_offset,
                           .max_lsn = header.sequence_number};
    }

    // called by the producer thread when the block is full or needs to be sealed for I/O handoff
    [[nodiscard]] auto finalize(uint64_t /*seq_num*/) noexcept -> FlushResult {
        std::size_t end_offset = (append_offset_ + 4095) & ~4095ULL;
        if (end_offset > BlockSize)
            end_offset = BlockSize;

        if (append_offset_ < end_offset) {
            std::memset(reinterpret_cast<std::byte*>(this) + append_offset_, 0, end_offset - append_offset_);
        }

        // Compute hash over the entire valid block
        header.block_hash = compute_wal_block_hash(this, end_offset);

        // Because we modified the header (Sector 0), we MUST rewrite from offset 0
        // to persist the final hash. NVMe handles large overwrites perfectly.
        std::size_t start_offset = 0;
        auto span = std::span<const std::byte>(reinterpret_cast<const std::byte*>(this) + start_offset,
                                               end_offset - start_offset);

        flush_offset_ = end_offset;
        return FlushResult{.memory_to_write = span,
                           .block_internal_offset = start_offset,
                           .max_lsn = header.sequence_number};
    }
};

static_assert(WALBlockLayout<GammaBlock<16384>>, "GammaBlock does not satisfy WALBlockLayout requirements.");

} // namespace stratadb::wal