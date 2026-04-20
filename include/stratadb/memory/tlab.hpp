#pragma once

#include <cstddef>
#include <cstdint>

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

        while (true) {

            if (current_block_ != nullptr) {
                auto current_ptr = reinterpret_cast<std::uintptr_t>(current_block_);
                auto aligned_ptr = (current_ptr + alignment - 1) & ~(alignment - 1);
                auto new_current = aligned_ptr + size;
                auto end_ptr = reinterpret_cast<std::uintptr_t>(block_end_);

                if (new_current <= end_ptr) [[likely]] {
                    current_block_ = reinterpret_cast<std::byte*>(new_current);
                    return reinterpret_cast<void*>(aligned_ptr);
                }
            }

            // slow path
            if (!refill(size + alignment)) [[unlikely]] {
                return nullptr;
            }
        }
    }

  private:
    auto refill(std::size_t min_size) noexcept -> bool;

  private:
    std::byte* current_block_{nullptr};
    std::byte* block_end_{nullptr};
    Arena* arena_{nullptr};
};
} // namespace stratadb::memory