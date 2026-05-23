#pragma once

#include "stratadb/utils/align.hpp"

#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>

namespace stratadb::memory {

class Arena;

// Thread-Local Allocation Buffer.
//
// Fast-path allocations bump a pointer within the current block; slow-path
// requests a fresh block from the Arena.
//
// The threshold below which an allocation goes through the TLAB (rather than
// directly to the Arena) is determined by Arena::large_alloc_fraction(), which
// in turn comes from MemoryConfig::large_alloc_tlab_fraction (resolved by
// ConfigResolver). TLAB itself owns no policy constants.
class TLAB {
  public:
    explicit TLAB(Arena& arena) noexcept;

    TLAB(const TLAB&) = delete;
    TLAB(TLAB&&) = delete;
    auto operator=(const TLAB&) -> TLAB& = delete;
    auto operator=(TLAB&&) -> TLAB& = delete;
    ~TLAB() noexcept = default;

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
            const auto aligned_ptr = utils::align_up(current_ptr, alignment);

            if (aligned_ptr >= current_ptr && aligned_ptr <= end_ptr) [[likely]] {
                const auto remaining = end_ptr - aligned_ptr;
                if (remaining >= size) [[likely]] {
                    // NOLINTBEGIN(performance-no-int-to-ptr)
                    current_block_ = reinterpret_cast<std::byte*>(aligned_ptr + size);
                    return reinterpret_cast<void*>(aligned_ptr);
                    // NOLINTEND(performance-no-int-to-ptr)
                }
            }
        }

        return allocate_slow(size, alignment);
    }

  private:
    [[nodiscard]] auto allocate_slow(std::size_t size, std::size_t alignment) noexcept -> void*;
    [[nodiscard]] auto refill(std::size_t min_size) noexcept -> bool;

  private:
    std::byte* current_block_{nullptr};
    std::byte* block_end_{nullptr};
    Arena* arena_{nullptr};
};

} // namespace stratadb::memory