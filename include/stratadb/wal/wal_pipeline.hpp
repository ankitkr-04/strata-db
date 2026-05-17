#pragma once

#include "stratadb/wal/wal_concept.hpp"

#include <cstddef>
#include <span>

namespace stratadb::wal {

template <typename T>
concept ConcurrencyQueue = requires(T q) {
    { q.enqueue(nullptr) } -> std::same_as<void>; // we will work later
};

template <WALBlockLayout Layout, ConcurrencyQueue Queue>
class WalPipeline {
  public:
    WalPipeline() = default;

    // The Hot Path: Inlined, zero virtual dispatch.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    auto stage_write(std::span<const std::byte> key, std::span<const std::byte> value) noexcept -> bool {
        // 1. Thread-local layout append
        // 2. If full, queue.push() to flusher
        return true;
    }

    void flush_pipeline() noexcept {
        // Force push to queue
    }

  private:
    Queue handoff_queue_;
};
};
} // namespace stratadb::wal