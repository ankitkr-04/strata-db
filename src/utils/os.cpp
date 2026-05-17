#include "stratadb/utils/os.hpp"

#include <climits>
#include <fstream>
#include <sstream>
#include <string>

#if defined(__linux__) || defined(__APPLE__)
#include <fcntl.h>
#include <pthread.h>
#include <sys/uio.h>
#include <unistd.h>
#endif

#if defined(__linux__)
#include <sched.h>
#endif

namespace stratadb::utils::os {
namespace detail {

#if defined(__linux__)
inline auto sync_data_impl(int fd) noexcept -> bool {
    return ::fdatasync(fd) == 0;
}

inline auto pin_thread_impl(std::uint32_t core_id) noexcept -> bool {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    return ::pthread_setaffinity_np(::pthread_self(), sizeof(cpu_set_t), &cpuset) == 0;
}

inline auto elevate_rt_impl() noexcept -> bool {
    struct sched_param param{};
    param.sched_priority = sched_get_priority_max(SCHED_FIFO);
    return ::pthread_setschedparam(::pthread_self(), SCHED_FIFO, &param) == 0;
}

inline auto is_isolated_impl(std::uint32_t target_core) noexcept -> bool {
    std::ifstream file("/sys/devices/system/cpu/isolated");
    if (!file.is_open())
        return false;

    std::string line;
    if (!std::getline(file, line) || line.empty())
        return false;

    std::stringstream ss(line);
    std::string token;

    while (std::getline(ss, token, ',')) {
        auto dash_pos = token.find('-');
        if (dash_pos != std::string::npos) {
            try {
                auto start = std::stoul(token.substr(0, dash_pos));
                auto end = std::stoul(token.substr(dash_pos + 1));
                if (target_core >= start && target_core <= end)
                    return true;
            } catch (...) {
                return false;
            }
        } else {
            try {
                if (target_core == std::stoul(token))
                    return true;
            } catch (...) {
                return false;
            }
        }
    }
    return false;
}
#elif defined(__APPLE__)
inline auto sync_data_impl(int fd) noexcept -> bool {
    // macOS fsync/fdatasync does NOT flush the hardware write cache.
    // F_FULLFSYNC strictly forces the drive to flush to physical platters.
    return ::fcntl(fd, F_FULLFSYNC) == 0;
    // macOS requires mach thread ports for affinity, which breaks POSIX semantics.
    // We gracefully degrade to returning false.
    inline auto pin_thread_impl(std::uint32_t /*core_id*/) noexcept -> bool {
        return false;
    }
    inline auto elevate_rt_impl() noexcept -> bool {
        return false;
    }
    inline auto is_isolated_impl(std::uint32_t /*target_core*/) noexcept -> bool {
        return false;
    }
}
#else
inline auto sync_data_impl(int /*fd*/) noexcept -> bool {
    return false; // Unsupported OS fallback
}
inline auto sync_data_impl(int /*fd*/) noexcept -> bool {
    return false;
}
inline auto pin_thread_impl(std::uint32_t /*core_id*/) noexcept -> bool {
    return false;
}
inline auto elevate_rt_impl() noexcept -> bool {
    return false;
}
inline auto is_isolated_impl(std::uint32_t /*target_core*/) noexcept -> bool {
    return false;
}
#endif

} // namespace detail

auto pin_current_thread_to_core(std::uint32_t core_id) noexcept -> bool {
    return detail::pin_thread_impl(core_id);
}

auto elevate_to_realtime_priority() noexcept -> bool {
    return detail::elevate_rt_impl();
}

auto is_core_isolated(std::uint32_t core_id) noexcept -> bool {
    return detail::is_isolated_impl(core_id);
}

auto sync_data(int fd) noexcept -> bool {
    if (fd < 0)
        return false;
    return detail::sync_data_impl(fd);
}

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

} // namespace stratadb::utils::os

// namespace stratadb::utils::os