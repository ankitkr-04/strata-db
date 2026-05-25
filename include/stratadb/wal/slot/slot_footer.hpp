#pragma once

#include "stratadb/utils/hash.hpp"

#include <cstddef>
#include <cstdint>

namespace stratadb::wal::slot {
struct alignas(512) WalSlotFooter {
    static constexpr uint32_t MAGIC = 0x57414C46U; // "WALF" little-endian
    static constexpr size_t SIZE = 512UZ;
    static constexpr size_t HASH_OFFSET = SIZE - sizeof(uint32_t); // 508

    uint32_t magic{MAGIC};            //   0
    uint32_t _reserved0{0};           //   4
    uint64_t slot_sequence{0};        //   8
    uint64_t sealed_write_offset{0};  //  16  first byte NOT written by user data
    uint64_t min_lsn{0};              //  24  LSN of the first block in this slot
    uint64_t max_lsn{0};              //  32  LSN of the last block in this slot
    uint8_t _pad[HASH_OFFSET - 40]{}; //  40..507
    uint32_t crc32c{0};
};

static_assert(sizeof(WalSlotFooter) == WalSlotFooter::SIZE, "WalSlotFooter must be exactly 512 bytes");
static_assert(alignof(WalSlotFooter) == 512, "WalSlotFooter must be 512-byte aligned for O_DIRECT pwrite");
static_assert(offsetof(WalSlotFooter, min_lsn) == 24, "min_lsn must be at byte offset 24");
static_assert(offsetof(WalSlotFooter, max_lsn) == 32, "max_lsn must be at byte offset 32");
static_assert(offsetof(WalSlotFooter, crc32c) == WalSlotFooter::HASH_OFFSET, "crc32c must be at byte offset 508");

inline void seal_footer_crc(WalSlotFooter& f) noexcept {
    f.crc32c = 0;
    f.crc32c = utils::crc32c(&f, WalSlotFooter::HASH_OFFSET);
}

[[nodiscard]] inline auto validate_footer(const WalSlotFooter& f, uint64_t expected_sequence) noexcept -> bool {
    if (f.magic != WalSlotFooter::MAGIC) {
        return false;
    }
    if (f.slot_sequence != expected_sequence) {
        return false;
    }
    return utils::crc32c(&f, WalSlotFooter::HASH_OFFSET) == f.crc32c;
}

[[nodiscard]] inline auto validate_footer_magic_and_crc(const WalSlotFooter& f) noexcept -> bool {
    if (f.magic != WalSlotFooter::MAGIC) {
        return false;
    }
    return utils::crc32c(&f, WalSlotFooter::HASH_OFFSET) == f.crc32c;
}

} // namespace stratadb::wal::slot