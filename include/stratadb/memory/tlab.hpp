#pragma once

#include "stratadb/utils/math.hpp"

#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>

namespace stratadb::memory {
class Arena;

class TLAB {
  public:
    explicit TLAB(Arena& arena) noexcept;

    TLAB(const TLAB&) = delete;
    auto operator=(const TLAB&) -> TLAB& = delete;
    TLAB(TLAB&&) = delete;
    auto operator=(TLAB&&) -> TLAB& = delete;
    ~TLAB() noexcept;

    // Must be called before the backing Arena is retired.
    // After detach(), all allocations return nullptr.
    void detach() noexcept {
        current_block_ = nullptr;
        block_end_ = nullptr;
        arena_ = nullptr;
    }

    [[nodiscard]] auto is_attached() const noexcept -> bool {
        return arena_ != nullptr;
    }

    [[nodiscard]] inline auto allocate(std::size_t size, std::size_t alignment = alignof(std::max_align_t)) noexcept
        -> void* {
        assert(std::has_single_bit(alignment));
        if (!std::has_single_bit(alignment)) [[unlikely]] {
            return nullptr;
        }

        if (current_block_ != nullptr) {
            const auto current_ptr = reinterpret_cast<std::uintptr_t>(current_block_);
            const auto end_ptr = reinterpret_cast<std::uintptr_t>(block_end_);
            const auto aligned_ptr = utils::align_up_pow2(current_ptr, alignment);

            // Overflow here would imply a mapping at the edge of virtual address space.
            if (aligned_ptr >= current_ptr && aligned_ptr <= end_ptr) [[likely]] {
                const auto remaining = end_ptr - aligned_ptr;
                if (remaining >= size) [[likely]] {
                    const auto new_current = aligned_ptr + size;

                    current_block_ = reinterpret_cast<std::byte*>(new_current);
                    return reinterpret_cast<void*>(aligned_ptr);
                }
            }

            return allocate_slow(size, alignment);
        }

        return allocate_slow(size, alignment);
    }

  private:
    auto allocate_slow(std::size_t size, std::size_t alignment) noexcept -> void*;
    auto refill(std::size_t min_size) noexcept -> bool;

  private:
    std::byte* current_block_{nullptr};
    std::byte* block_end_{nullptr};
    Arena* arena_{nullptr};
};
} // namespace stratadb::memory
