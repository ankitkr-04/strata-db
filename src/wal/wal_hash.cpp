#define XXH_INLINE_ALL
#include <xxhash.h>

#include "stratadb/wal/gamma_block.hpp"

namespace stratadb::wal {

auto compute_wal_block_hash(const void* data,
                            std::size_t length) noexcept
    -> XXH128_hash_t {
    return XXH3_128bits(data, length);
}

} // namespace stratadb::wal