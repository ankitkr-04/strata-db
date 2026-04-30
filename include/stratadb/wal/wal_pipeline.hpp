#pragma once

#include "stratadb/wal/wal_concept.hpp"

#include <cstddef>
#include <span>

namespace stratadb::wal {
template <WALBlockLayout Layout>
class WalPipeline {
  public:
    WalPipeline() = default;

    // The Hot Path
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    auto stage_write(std::span<const std::byte> key, std::span<const std::byte> value) noexcept -> bool {
        // Attempt to append to the current block. If it returns false, we need to seal and flush.
        return true;
    }

    void flush_pipeline() noexcept {
        // Notify flusher thread
    }
};
} // namespace stratadb::wal