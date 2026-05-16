#include "stratadb/utils/os.hpp"

#include <climits>

#if defined(__linux__) || defined(__APPLE__)
#include <fcntl.h>   // For fcntl, F_FULLFSYNC
#include <sys/uio.h> // For IOV_MAX (on some POSIX systems)
#include <unistd.h>  // For close, fdatasync
#endif

namespace stratadb::utils::os {
namespace detail {

#if defined(__linux__)
inline auto sync_data_impl(int fd) noexcept -> bool {
    return ::fdatasync(fd) == 0;
}
#elif defined(__APPLE__)
inline auto sync_data_impl(int fd) noexcept -> bool {
    // macOS fsync/fdatasync does NOT flush the hardware write cache.
    // F_FULLFSYNC strictly forces the drive to flush to physical platters.
    return ::fcntl(fd, F_FULLFSYNC) == 0;
}
#else
inline auto sync_data_impl(int /*fd*/) noexcept -> bool {
    return false; // Unsupported OS fallback
}
#endif

} // namespace detail

auto sync_data(int fd) noexcept -> bool {
    if (fd < 0)
        return false;
    return detail::sync_data_impl(fd);
}

} // namespace stratadb::utils::os

auto get_iov_max() noexcept -> std::size_t {
#if defined(IOV_MAX)
    return static_cast<std::size_t>(IOV_MAX);
#else
    // Safe POSIX minimum fallback. Linux usually supports 1024.
    return 256;
#endif
}

void close_fd(int fd) noexcept {
    if (fd >= 0) {
#if defined(__linux__) || defined(__APPLE__)
        ::close(fd);
#endif
    }
}
// namespace stratadb::utils::os