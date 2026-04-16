#pragma once

#include <atomic>
#include <cstddef>
#include <memory>

namespace stratadb::memory {

namespace defaults {
inline constexpr std::size_t ARENA_BLOCK_SIZE = 2ULL * 1024 * 1024;
}

class Arena {
  public:
    Arena();
    ~Arena();

    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;
    Arena(Arena&&) = delete;
    Arena& operator=(Arena&&) = delete;

    void* allocate(std::size_t bytes, std::size_t alignment = alignof(std::max_align_t));

    [[nodiscard]] std::size_t memory_used() const noexcept {
        return memory_used_.load(std::memory_order_relaxed);
    }

  private:
    struct Block {
        std::unique_ptr<std::byte[]> data;
        std::atomic<std::size_t> offset{0};
        std::atomic<Block*> next{nullptr};

        Block()
            : data(std::make_unique<std::byte[]>(defaults::ARENA_BLOCK_SIZE)) {}
    };

    std::atomic<Block*> current_block_;
    std::atomic<std::size_t> memory_used_{0};
};

} // namespace stratadb::memory