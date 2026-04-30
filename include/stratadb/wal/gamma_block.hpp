#pragma once

#include "stratadb/wal/wal_concept.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>

namespace stratadb::wal {

template <size_t BlockSize = 16384>
struct alignas(4096) GammaBlock {
    static_assert(BlockSize % 4096 == 0, "Block size must be a multiple of OS page size");

    struct Header {
        uint64_t sequence_number{0};
        uint32_t block_crc32{0};
        uint16_t record_count{0};
        uint16_t flags{0};
    } header;

    alignas(8) std::array<std::byte, BlockSize - sizeof(Header)> arena{};

    size_t current_offset{0};

    [[nodiscard]] auto append(std::span<const std::byte> key, std::span<const std::byte> value) -> bool {
        const auto k_len = static_cast<uint32_t>(key.size());
        const auto v_len = static_cast<uint32_t>(value.size());

        const size_t total_required = sizeof(k_len) + sizeof(v_len) + k_len + v_len;

        // Check if we have enough space for the new record
        if (current_offset + total_required > arena.size()) {
            return false; // Not enough space, caller should finalize and start a new block
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

        // ensure padding didn't push us over the block size
        if (current_offset > arena.size()) {
            current_offset = arena.size(); // clamp to block size, finalize will handle the rest
        }

        return true;
    }
    auto finalize(uint64_t seq) -> std::span<const std::byte> {
        header.sequence_number = seq;

        // Note: CRC32 is intentionally left to 0 here.
        // The *Flusher Thread* computes it over this returned span before O_DIRECT.

        // We return the total written size (Header + active arena)
        const size_t total_written = sizeof(Header) + current_offset;

        // Safely cast `this` to byte pointer for the I/O engine
        const auto* block_start = reinterpret_cast<const std::byte*>(this);
        return {block_start, total_written};
    }

    static_assert(WALBlockLayout<GammaBlock<16384>>, "GammaBlock must satisfy WALBlockLayout concept");
};
} // namespace stratadb::wal