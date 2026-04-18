#pragma once

#include <cstddef>

namespace stratadb::memory {
class Arena;


class TLAB {
  public:
    explicit TLAB(Arena& arena) noexcept;

    TLAB(const TLAB&) = delete;
    TLAB& operator=(const TLAB&) = delete;
    TLAB(TLAB&&) = delete;
    TLAB& operator=(TLAB&&) = delete;
    ~TLAB() noexcept;

    [[nodiscard]] auto allocate(std::size_t size, std::size_t alignment = alignof(std::max_align_t)) noexcept -> void*;

  private:
    auto refill(std::size_t min_size) noexcept -> bool;

  private:
    std::byte* current_block_{nullptr};
    std::byte* block_end_{nullptr};
    Arena* arena_{nullptr};
};
} // namespace stratadb::memory