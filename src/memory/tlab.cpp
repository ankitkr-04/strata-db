#include "stratadb/memory/tlab.hpp"

#include "stratadb/memory/arena.hpp"

namespace stratadb::memory {

TLAB::TLAB(Arena& arena) noexcept
    : arena_(&arena) {}

TLAB::~TLAB() noexcept = default;

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