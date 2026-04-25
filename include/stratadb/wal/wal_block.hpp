#pragma once

#include "stratadb/utils/hardware.hpp"

#include <cstddef>
#include <cstdint>

namespace stratadb::wal {
// The physical representation of data on disk.
// Templated to allow the WalManager to instantiate the exact size
// matching the hardware's AWUPF (Atomic Write Unit Power Fail) boundary.
<template <std::size_t BlockSize>> struct alignas(BlockSize) WalBlock {
    static constexpr std::size_t CACHE_LINES = BlockSize / utils::CACHE_LINE_SIZE;
    // Tearing Matrix: 1 byte per cache line
    static constexpr std::size_t TEARING_MATRIX_BYTES = CACHE_LINES;
    static constexpr std::size_t TEARING_MATRIX_PADDED =
        (TEARING_MATRIX_BYTES + utils::CACHE_LINE_SIZE - 1) & ~(utils::CACHE_LINE_SIZE - 1);

    static constexpr std::size_t HEADER_BYTES = 64;
    static constexpr std::size_t METADATA_BYTES = 128;

    static constexpr std::size_t PAYLOAD_BYTES = BlockSize - HEADER_BYTES - TEARING_MATRIX_PADDED - METADATA_BYTES;

    struct alignas(utils::CACHE_LINE_SIZE) Header {
        std::uint64_t sequence_id;   // Global logical sequence number
        std::uint64_t physical_lba;  // Detects misdirected I/O
        std::uint32_t payload_crc32; // AVX-512 folded checksum
        std::uint32_t header_crc32;  // Checksum of this header
        std::uint64_t epoch_number;  // Ties back to EpochManager
        std::uint8_t padding[32];    // Pad strictly to 64 bytes
    } header;

    // Cache line 1 to N: Scalable Tearing Matrix for atomicity detection. Each byte corresponds to a cache line in the
    // payload.
    struct alignas(utils::CACHE_LINE_SIZE) TearingMatrix {
        std::uint8_t generation_counters[TEARING_MATRIX_PADDED];
    } tearing_matrix;

    // -cahce line N+1: Metadata for the payload, such as key lengths and operation types. This is separate from the
    // header SoA Metadata(128 bytes) is designed to fit within 2 cache lines, allowing for efficient access without
    // interfering with the tearing matrix.
    struct alignas(utils::CACHE_LINE_SIZE) VectorMetadata {
        std::uint8_t opcodes[64];      // Insert/Delete/Commit flags
        std::uint16_t key_lengths[32]; // Length of keys in payload
    } metadata;

    // Remaining Cache Lines: Payload data, such as keys and values. The payload is designed to be as large as possible
    // while still fitting within the block size after accounting for the header, tearing matrix, and metadata.
    struct alignas(utils::CACHE_LINE_SIZE) Payload {
        std::uint8_t data[PAYLOAD_BYTES];
    } payload;

    static_assert((BlockSize & (BlockSize - 1)) == 0, "BlockSize must be a power of two");
};

} // namespace stratadb::wal