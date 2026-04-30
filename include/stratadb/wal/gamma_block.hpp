#pragma once

#include "stratadb/wal/wal_concept.hpp"

#define XXH_INLINE_ALL
#include "xxhash.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>

namespace stratadb::wal {

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

    size_t current_offset{0};

    // Implements WalBlockLayout
    [[nodiscard]] auto append(std::span<const std::byte> key, std::span<const std::byte> value) -> bool {
        const auto k_len = static_cast<uint32_t>(key.size());
        const auto v_len = static_cast<uint32_t>(value.size());

        const size_t total_required = sizeof(k_len) + sizeof(v_len) + k_len + v_len;

        // Check capacity
        if (current_offset + total_required > arena.size()) {
            return false;
        }

        // Write lengths (memcpy is safe for implicit lifetime creation of trivially copyable types)
        std::memcpy(arena.data() + current_offset, &k_len, sizeof(k_len));
        current_offset += sizeof(k_len);

        std::memcpy(arena.data() + current_offset, &v_len, sizeof(v_len));
        current_offset += sizeof(v_len);

        // Write payload
        std::memcpy(arena.data() + current_offset, key.data(), k_len);
        current_offset += k_len;

        std::memcpy(arena.data() + current_offset, value.data(), v_len);
        current_offset += v_len;

        header.record_count++;

        // HARDWARE SYMPATHY: Dynamic alignment padding to the next 8-byte boundary.
        // Bitwise trick: (offset + 7) & ~7 rounds up to the nearest multiple of 8.
        current_offset = (current_offset + 7) & ~7ULL;

        // Ensure padding didn't push us out of bounds (edge case handling)
        if (current_offset > arena.size()) {
            current_offset = arena.size();
        }

        return true;
    }
    // called by the producer thread when the block is full or needs to be sealed for I/O handoff
    [[nodiscard]] auto finalize(uint64_t seq_num) -> std::span<const std::byte> {
        header.sequence_number = seq_num;
        // Zero the hash field before computing the hash to avoid circular dependency
        header.block_hash = {0, 0};

        // We return the total written size (Header + active arena)
        const size_t total_written = sizeof(Header) + current_offset;

        // compute hash, use avx2/avx512 if available, otherwise fallback to scalar
        header.block_hash = XXH3_128bits(this, current_offset);

        // Safely cast `this` to byte pointer for the I/O engine
        const auto* block_start = reinterpret_cast<const std::byte*>(this);
        return {block_start, total_written};
    }
};

static_assert(WALBlockLayout<GammaBlock<16384>>);

} // namespace stratadb::wal