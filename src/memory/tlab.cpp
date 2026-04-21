#include "stratadb/memory/tlab.hpp"

#include "stratadb/memory/arena.hpp"

#include <limits>

namespace stratadb::memory {

TLAB::TLAB(Arena& arena) noexcept
    : arena_(&arena) {}

TLAB::~TLAB() noexcept = default;

auto TLAB::allocate_slow(std::size_t size, std::size_t alignment, std::size_t remaining) noexcept -> void* {
    const std::size_t tlab_size = arena_->tlab_size();
    const bool direct_large_allocation = size >= tlab_size;
    const bool preserve_useful_slack = current_block_ != nullptr && remaining >= alignment && size < tlab_size;

    if (direct_large_allocation || preserve_useful_slack) {
        return arena_->allocate_aligned(size, alignment);
    }

    if (size > std::numeric_limits<std::size_t>::max() - alignment) [[unlikely]] {
        return nullptr;
    }

    if (!refill(size + alignment)) [[unlikely]] {
        return nullptr;
    }

    return allocate(size, alignment);
}

auto TLAB::refill(std::size_t min_size) noexcept -> bool {
    // Request block from global Arena
    auto span = arena_->allocate_block(min_size);

    if (span.empty()) {
        return false; // Arena is OOM
    }

    current_block_ = span.data();
    block_end_ = current_block_ + span.size();

    return true;
}

} // namespace stratadb::memory
