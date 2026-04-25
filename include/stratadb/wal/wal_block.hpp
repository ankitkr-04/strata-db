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
};

} // namespace stratadb::wal