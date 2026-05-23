#pragma once

#include "stratadb/config/immutable/memory_config.hpp"
#include "stratadb/utils/cache.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>

namespace stratadb::memory {

enum class ArenaError : std::uint8_t {
    MmapFailed,
    OutOfMemory,
    MbindFailed,
    MunmapFailed,
};

class Arena {
  public:
    // ConfigResolver must be called before Arena::create so that all sentinel
    // fields in MemoryConfig are populated. Passing an unresolved config is a
    // programming error and will be caught by internal debug assertions.
    [[nodiscard]] static auto create(const config::MemoryConfig& cfg) noexcept -> std::expected<Arena, ArenaError>;

    ~Arena() noexcept;
    Arena(const Arena&) = delete;
    auto operator=(const Arena&) -> Arena& = delete;
    Arena(Arena&& other) noexcept;
    auto operator=(Arena&& other) noexcept -> Arena&;

    [[nodiscard]] auto allocate_block(std::size_t min_size) noexcept -> std::span<std::byte>;
    [[nodiscard]] auto allocate_aligned(std::size_t size, std::size_t alignment) noexcept -> void*;

    // WARNING: caller must ensure no live TLAB still references this Arena.
    // Any string_view previously returned from memtable get()/scan() into this
    // Arena becomes invalid after reset().
    void reset() noexcept;

    [[nodiscard]] auto capacity() const noexcept -> std::size_t {
        return config_.total_budget_bytes;
    }
    [[nodiscard]] auto tlab_size() const noexcept -> std::size_t {
        return config_.tlab_size_bytes;
    }
    [[nodiscard]] auto large_alloc_fraction() const noexcept -> std::size_t {
        return config_.large_alloc_tlab_fraction;
    }
    [[nodiscard]] auto large_alloc_threshold() const noexcept -> std::size_t {
        return large_alloc_threshold_bytes_;
    }
    [[nodiscard]] auto memory_used() const noexcept -> std::size_t {
        return offset_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] auto remaining() const noexcept -> std::size_t {
        return capacity() - memory_used();
    }

  private:
    explicit Arena(std::byte* base, const config::MemoryConfig& cfg) noexcept;

    [[nodiscard]] auto bump_allocate(std::size_t size, std::size_t alignment) noexcept -> std::size_t;

  private:
    std::byte* base_{nullptr};
    config::MemoryConfig config_{};
    std::size_t large_alloc_threshold_bytes_{0};
    alignas(stratadb::utils::CACHE_LINE_SIZE) std::atomic<std::size_t> offset_{0};
};

} // namespace stratadb::memory