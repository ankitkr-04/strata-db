#include "stratadb/utils/os.hpp"

#include <climits>
#include <unistd.h>

namespace stratadb::utils::os {

auto sync_data(int fd) noexcept -> bool {
    if (fd < 0)
        return false;
#if defined(__APPLE__)
    return ::fcntl(fd, F_FULLFSYNC) == 0;
#else
    return ::fdatasync(fd) == 0;
#endif
}

auto get_iov_max() noexcept -> std::size_t {
#if defined(IOV_MAX)
    return static_cast<std::size_t>(IOV_MAX);
#else
    return 256; // Safe POSIX fallback
#endif
}

} // namespace stratadb::utils::os