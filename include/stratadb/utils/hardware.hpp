#pragma once

#include <cstddef>
#include <linux/fs.h>
#include <sys/ioctl.h>

namespace stratadb::utils {

// 64 bytes is the common L1 cache-line size on x86_64 and ARM64.
inline constexpr std::size_t CACHE_LINE_SIZE = 64;
inline constexpr std::size_t DEFAULT_ATOMIC_WRITE_BOUNDARY = 4096;

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] inline auto probe_atomic_write_boundary(int fd,
                                                      size_t fallback_size = DEFAULT_ATOMIC_WRITE_BOUNDARY) noexcept
    -> std::size_t {
    unsigned int physical_block_size = 0;

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
    if (ioctl(fd, BLKPBSZGET, &physical_block_size) == 0) {
        return physical_block_size;
    }

    return fallback_size;
};
} // namespace stratadb::utils
