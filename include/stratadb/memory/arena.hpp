#pragma once
#include "stratadb/config/memory_config.hpp"
#include "stratadb/utils/hardware.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <new>
#include <span>

namespace stratadb::memory {
enum class ArenaError : std::uint8_t {
    MmapFailed,
    OutOfMemory, // ENOMEM
    MbindFailed,
    MunmapFailed,
};

class Arena {
  public:
    [[nodiscard]] static auto create(const config::MemoryConfig& config) noexcept -> std::expected<Arena, ArenaError>;

    ~Arena() noexcept;
    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;
    Arena(Arena&& other) noexcept;
    Arena& operator=(Arena&& other) noexcept;

    [[nodiscard]] auto allocate_block(std::size_t min_size) noexcept -> std::span<std::byte>;
    [[nodiscard]] auto allocate_aligned(std::size_t size, std::size_t alignment) noexcept -> void*;

    // WARNING: caller must ensure no live TLAB still references this Arena when reset() is called.
    // reset() rewinds offset_ to zero and may reuse memory ranges handed out earlier.
    // Any std::string_view previously returned from memtable get()/scan() into this Arena becomes invalid.
    void reset() noexcept;

    [[nodiscard]] auto capacity() const noexcept -> std::size_t {
        return config_.total_budget_bytes;
    };
    [[nodiscard]] auto tlab_size() const noexcept -> std::size_t {
        return config_.tlab_size_bytes;
    };

    [[nodiscard]] auto memory_used() const noexcept -> std::size_t {
      // allocate_block/allocate_aligned maintain offset_ <= total_budget_bytes.
      return offset_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] auto remaining() const noexcept -> std::size_t {
        return capacity() - memory_used();
    }

  private:
    // base must be page-aligned and size >= config.total_budget_bytes
    explicit Arena(std::byte* base, const config::MemoryConfig& config) noexcept;

  private:
    std::byte* base_{nullptr};
    config::MemoryConfig config_{};

    alignas(stratadb::utils::CACHE_LINE_SIZE) std::atomic<std::size_t> offset_{0};
};

} // namespace stratadb::memory
