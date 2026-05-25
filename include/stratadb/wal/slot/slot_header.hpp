#pragma once

#include "stratadb/utils/hash.hpp"

#include <array>
#include <cstddef>
#include <cstdint>

namespace stratadb::wal::slot {
struct alignas(4096) WalSlotHeader {
    static constexpr std::uint32_t MAGIC = 0x57414C53U; // "WALS" in little-endian
    static constexpr std::uint8_t VERSION = 1U;
    static constexpr std::size_t SIZE = 4096UZ;
    static constexpr std::size_t HASH_OFFSET = SIZE - sizeof(std::uint32_t);

    uint32_t magic{MAGIC};                      //    0
    uint8_t version{VERSION};                   //    4
    uint8_t block_layout{0};                    //    5
    uint16_t _reserved0{0};                     //    6
    uint64_t slot_sequence{0};                  //    8
    uint64_t slot_max_bytes{0};                 //   16
    uint64_t created_at_ns{0};                  //   24
    std::array<uint8_t, 16> db_instance_uuid{}; //   32  ← 16 bytes
    uint8_t _pad[HASH_OFFSET - 48]{};             // 48..4091
    uint32_t crc32c{0};                         // 4092
};
static_assert(sizeof(WalSlotHeader) == WalSlotHeader::SIZE, "WalSlotHeader must be exactly 4096 bytes");
static_assert(alignof(WalSlotHeader) == 4096, "WalSlotHeader must be 4096-byte aligned for O_DIRECT pwrite");
static_assert(offsetof(WalSlotHeader, db_instance_uuid) == 32, "db_instance_uuid must be at byte offset 32");
static_assert(offsetof(WalSlotHeader, crc32c) == WalSlotHeader::HASH_OFFSET, "crc32c must be at byte offset 4092");

inline void seal_header_crc(WalSlotHeader& h) noexcept {
    h.crc32c = 0;
    h.crc32c = utils::crc32c(&h, WalSlotHeader::HASH_OFFSET);
}

[[nodiscard]] inline auto validate_header(const WalSlotHeader& h) noexcept -> bool {
    if (h.magic != WalSlotHeader::MAGIC) {
        return false;
    }
    if (h.version != WalSlotHeader::VERSION) {
        return false;
    }
    if (h.slot_max_bytes == 0) {
        return false;
    }
    return utils::crc32c(&h, WalSlotHeader::HASH_OFFSET) == h.crc32c;
}

} // namespace stratadb::wal::slot