#pragma once

#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>

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

    [[nodiscard]] inline auto allocate(std::size_t size, std::size_t alignment = alignof(std::max_align_t)) noexcept
        -> void* {
        assert(std::has_single_bit(alignment));
        if (!std::has_single_bit(alignment)) [[unlikely]] {
            return nullptr;
        }

        if (current_block_ != nullptr) {
            const auto current_ptr = reinterpret_cast<std::uintptr_t>(current_block_);
            const auto end_ptr = reinterpret_cast<std::uintptr_t>(block_end_);
            const auto mask = static_cast<std::uintptr_t>(alignment - 1);

            if (current_ptr <= std::numeric_limits<std::uintptr_t>::max() - mask) [[likely]] {
                const auto aligned_ptr = (current_ptr + mask) & ~mask;

                if (size <= std::numeric_limits<std::uintptr_t>::max() - aligned_ptr) [[likely]] {
                    const auto new_current = aligned_ptr + size;

                    if (new_current <= end_ptr) [[likely]] {
                        current_block_ = reinterpret_cast<std::byte*>(new_current);
                        return reinterpret_cast<void*>(aligned_ptr);
                    }
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
