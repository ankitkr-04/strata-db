#pragma once

#include <cstddef>
#include <linux/fs.h>
#include <sys/ioctl.h>

namespace stratadb::utils {

// 64 bytes is the common L1 cache-line size on x86_64 and ARM64.
inline constexpr std::size_t CACHE_LINE_SIZE = 64;

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
inline auto detect_optimal_block_size(int fd, size_t fallback_size) noexcept -> std::size_t {
    unsigned int physical_block_size = 0;

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
    if (ioctl(fd, BLKPBSZGET, &physical_block_size) == 0) {
        return physical_block_size;
    }

    return fallback_size;
};
} // namespace stratadb::utils
