#pragma once

#include "stratadb/utils/hardware.hpp"

#include <cstddef>
#include <cstdint>

namespace stratadb::wal {
// The physical representation of data on disk.
// Templated to allow the WalManager to instantiate the exact size
// matching the hardware's AWUPF (Atomic Write Unit Power Fail) boundary.
template <std::size_t BlockSize>
struct alignas(BlockSize) WalBlock {
    static constexpr std::size_t HEADER_BYTES = utils::CACHE_LINE_SIZE;
    static constexpr std::size_t CACHE_LINES = BlockSize / HEADER_BYTES;

    static constexpr std::size_t TEARING_MATRIX_BYTES = CACHE_LINES;
    static constexpr std::size_t TEARING_MATRIX_PADDED =
        (TEARING_MATRIX_BYTES + HEADER_BYTES - 1) & ~(HEADER_BYTES - 1);

    static constexpr std::size_t HEADER_DATA_BYTES = 38;
    static constexpr std::size_t METADATA_BYTES = 128;

    // Unified capacity constant to prevent array mismatch
    static constexpr std::size_t MAX_RECORDS = 32;

    // Protect against unsigned underflow for non-standard BlockSizes
    static constexpr std::size_t OVERHEAD_BYTES = HEADER_BYTES + TEARING_MATRIX_PADDED + METADATA_BYTES;
    static_assert(BlockSize > OVERHEAD_BYTES, "BlockSize too small to contain WAL metadata");

    static constexpr std::size_t PAYLOAD_BYTES = BlockSize - OVERHEAD_BYTES;

    struct alignas(utils::CACHE_LINE_SIZE) Header {
        std::uint64_t sequence_id;           // Global logical sequence number
        std::uint64_t physical_lba;          // Detects misdirected I/O
        std::uint32_t payload_crc32;         // AVX-512 folded checksum
        std::uint32_t header_crc32;          // Checksum of this header
        std::uint64_t epoch_number;          // Ties back to EpochManager
        std::uint32_t payload_bytes_written; // Actual bytes of payload data, used for partial writes and recovery
        std::uint16_t num_records;           // 2B: DB ops in this block, used for recovery and compaction

        std::uint8_t
            padding[HEADER_BYTES - HEADER_DATA_BYTES]; // Pad the header to fill one cache line, ensuring the tearing
                                                       // matrix starts on a new cache line for atomicity detection.
    } header;
    static_assert(sizeof(Header) == HEADER_BYTES, "Header size mismatch. Update HEADER_DATA_BYTES.");

    // Cache line 1 to N: Scalable Tearing Matrix for atomicity detection. Each byte corresponds to a cache line in the
    // payload.
    struct alignas(utils::CACHE_LINE_SIZE) TearingMatrix {
        std::uint8_t generation_counters[TEARING_MATRIX_PADDED];
    } tearing_matrix;

    static_assert(sizeof(TearingMatrix) == TEARING_MATRIX_PADDED, "TearingMatrix size mismatch.");
    static_assert(offsetof(WalBlock, tearing_matrix) == HEADER_BYTES, "TearingMatrix must follow Header.");

    // -cache line N+1: Metadata for the payload, such as key lengths and operation types. This is separate from the
    // header SoA Metadata(128 bytes) is designed to fit within 2 cache lines, allowing for efficient access without
    // interfering with the tearing matrix.
    struct alignas(utils::CACHE_LINE_SIZE) VectorMetadata {
        std::uint8_t opcodes[MAX_RECORDS];      // Insert/Delete/Commit flags
        std::uint16_t key_lengths[MAX_RECORDS]; // Length of keys in payload
        std::uint8_t _pad[METADATA_BYTES
                          - 96]; // Pad to fill the metadata block, ensuring the payload starts on a new cache line.
    } metadata;
    static_assert(sizeof(VectorMetadata) == METADATA_BYTES, "VectorMetadata size mismatch.");

    // Remaining Cache Lines: Payload data, such as keys and values. The payload is designed to be as large as possible
    // while still fitting within the block size after accounting for the header, tearing matrix, and metadata.
    struct alignas(utils::CACHE_LINE_SIZE) Payload {
        std::uint8_t data[PAYLOAD_BYTES];
    } payload;

    static_assert((BlockSize & (BlockSize - 1)) == 0, "BlockSize must be a power of two");
    static_assert(sizeof(WalBlock) == BlockSize, "WalBlock size mismatch with BlockSize template parameter");
};

} // namespace stratadb::wal