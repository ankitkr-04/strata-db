#include "stratadb/utils/os.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <climits>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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
#ifndef NDEBUG
namespace test_hooks {
alignas(64) std::atomic<bool> fail_core_pinning{false};
alignas(64) std::atomic<bool> fail_realtime_elevation{false};
} // namespace test_hooks
#endif

namespace detail {

struct CpuRange {
    std::uint32_t first{};
    std::uint32_t last{};
};

auto map_errno(int err) noexcept -> io::IOError {
    switch (err) {
        case ENOSPC:
            return io::IOError::DeviceFull;
        case EACCES:
        case EPERM:
            return io::IOError::PermissionDenied;
        case EINVAL:
            return io::IOError::AlignmentViolation;
        case EIO:
            return io::IOError::HardwareError;
        default:
            return io::IOError::UnknownError;
    }
}

auto parse_u32(std::string_view text, std::uint32_t& out) noexcept -> bool {
    if (text.empty()) {
        return false;
    }

    std::uint32_t value{};
    const char* begin = text.data();
    const char* end = text.data() + text.size();
    const auto [ptr, ec] = std::from_chars(begin, end, value);
    if (ec != std::errc{} || ptr != end) {
        return false;
    }

    out = value;
    return true;
}

auto parse_cpu_ranges(std::string_view line) noexcept -> std::vector<CpuRange> {
    std::vector<CpuRange> ranges;
    std::size_t start = 0;

    while (start < line.size()) {
        const std::size_t comma = line.find(',', start);
        const std::string_view token =
            line.substr(start, comma == std::string_view::npos ? std::string_view::npos : comma - start);

        if (!token.empty()) {
            const std::size_t dash = token.find('-');
            if (dash == std::string_view::npos) {
                std::uint32_t core{};
                if (parse_u32(token, core)) {
                    ranges.push_back(CpuRange{core, core});
                }
            } else {
                std::uint32_t first{};
                std::uint32_t last{};
                if (parse_u32(token.substr(0, dash), first) && parse_u32(token.substr(dash + 1), last)
                    && first <= last) {
                    ranges.push_back(CpuRange{first, last});
                }
            }
        }

        if (comma == std::string_view::npos) {
            break;
        }
        start = comma + 1;
    }

    return ranges;
}

auto read_isolated_cpu_ranges() noexcept -> std::vector<CpuRange> {
    std::ifstream file("/sys/devices/system/cpu/isolated");
    if (!file.is_open()) {
        return {};
    }

    std::string line;
    if (!std::getline(file, line) || line.empty()) {
        return {};
    }

    return parse_cpu_ranges(line);
}

auto open_file_impl(const std::filesystem::path& path, int flags) noexcept
    -> std::expected<io::FileHandle, io::IOError> {
    const int fd = ::open(path.c_str(), flags, 0644);
    if (fd < 0) {
        return std::unexpected(map_errno(errno));
    }
    return fd;
}

#if defined(__linux__)
auto sync_data_impl(int fd) noexcept -> bool {
    return ::fdatasync(fd) == 0;
}

auto pin_thread_impl(std::uint32_t core_id) noexcept -> bool {
    if (core_id >= static_cast<std::uint32_t>(CPU_SETSIZE)) {
        return false;
    }

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(static_cast<int>(core_id), &cpuset);
    return ::pthread_setaffinity_np(::pthread_self(), sizeof(cpu_set_t), &cpuset) == 0;
}

auto elevate_rt_impl() noexcept -> bool {
    const int max_prio = ::sched_get_priority_max(SCHED_FIFO);
    if (max_prio < 0) {
        return false;
    }

    sched_param param{};
    param.sched_priority = max_prio;
    return ::pthread_setschedparam(::pthread_self(), SCHED_FIFO, &param) == 0;
}

auto is_isolated_impl(std::uint32_t target_core) noexcept -> bool {
    const auto ranges = read_isolated_cpu_ranges();
    for (const auto& range : ranges) {
        if (target_core >= range.first && target_core <= range.last) {
            return true;
        }
    }
    return false;
}

auto auto_discover_isolated_core_impl() noexcept -> std::optional<std::uint32_t> {
    const auto ranges = read_isolated_cpu_ranges();
    if (ranges.empty()) {
        return std::nullopt;
    }

    std::uint32_t best = std::numeric_limits<std::uint32_t>::max();
    for (const auto& range : ranges) {
        best = std::min(best, range.first);
    }

    if (best == std::numeric_limits<std::uint32_t>::max()) {
        return std::nullopt;
    }
    return best;
}
#elif defined(__APPLE__)
auto sync_data_impl(int fd) noexcept -> bool {
    return ::fcntl(fd, F_FULLFSYNC) == 0;
}

auto pin_thread_impl(std::uint32_t) noexcept -> bool {
    return false;
}

auto elevate_rt_impl() noexcept -> bool {
    return false;
}

auto is_isolated_impl(std::uint32_t) noexcept -> bool {
    return false;
}

auto auto_discover_isolated_core_impl() noexcept -> std::optional<std::uint32_t> {
    return std::nullopt;
}
#else
auto sync_data_impl(int) noexcept -> bool {
    return false;
}

auto pin_thread_impl(std::uint32_t) noexcept -> bool {
    return false;
}

auto elevate_rt_impl() noexcept -> bool {
    return false;
}

auto is_isolated_impl(std::uint32_t) noexcept -> bool {
    return false;
}

auto auto_discover_isolated_core_impl() noexcept -> std::optional<std::uint32_t> {
    return std::nullopt;
}
#endif

} // namespace detail

auto pin_current_thread_to_core(std::uint32_t core_id) noexcept -> bool {
#ifndef NDEBUG
    if (test_hooks::fail_core_pinning.load(std::memory_order_relaxed)) {
        return false;
    }
#endif
    return detail::pin_thread_impl(core_id);
}

auto elevate_to_realtime_priority() noexcept -> bool {
#ifndef NDEBUG
    if (test_hooks::fail_realtime_elevation.load(std::memory_order_relaxed)) {
        return false;
    }
#endif
    return detail::elevate_rt_impl();
}

auto is_core_isolated(std::uint32_t core_id) noexcept -> bool {
    return detail::is_isolated_impl(core_id);
}

auto auto_discover_isolated_core() noexcept -> std::optional<std::uint32_t> {
    return detail::auto_discover_isolated_core_impl();
}

auto sync_data(int fd) noexcept -> bool {
    return fd >= 0 && detail::sync_data_impl(fd);
}

auto get_iov_max() noexcept -> std::size_t {
#if defined(IOV_MAX)
    return static_cast<std::size_t>(IOV_MAX);
#else
    return 256;
#endif
}

void close_fd(int fd) noexcept {
    if (fd >= 0) {
#if defined(__linux__) || defined(__APPLE__)
        (void)::close(fd);
#endif
    }
}

auto open_buffered(const std::filesystem::path& path, bool create) noexcept
    -> std::expected<io::FileHandle, io::IOError> {
    int flags = O_RDWR | O_CLOEXEC;
    if (create) {
        flags |= O_CREAT | O_TRUNC;
    }
    return detail::open_file_impl(path, flags);
}

auto open_direct(const std::filesystem::path& path) noexcept -> std::expected<io::FileHandle, io::IOError> {
#if defined(__linux__)
    return detail::open_file_impl(path, O_RDWR | O_DIRECT | O_CLOEXEC);
#else
    auto fd = detail::open_file_impl(path, O_RDWR | O_CLOEXEC);
    if (!fd) {
        return fd;
    }

    if (::fcntl(*fd, F_NOCACHE, 1) < 0) {
        const int saved = errno;
        ::close(*fd);
        return std::unexpected(detail::map_errno(saved));
    }

    return fd;
#endif
}

auto allocate_file_space(int fd, std::uint64_t bytes) noexcept -> std::expected<void, io::IOError> {
    if (fd < 0) {
        return std::unexpected(io::IOError::UnknownError);
    }

    if (bytes > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
        return std::unexpected(io::IOError::AlignmentViolation);
    }

    const int rc = ::posix_fallocate(fd, 0, static_cast<off_t>(bytes));
    if (rc != 0) {
        return std::unexpected(detail::map_errno(rc));
    }
    return {};
}
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto read_exact(int fd, void* buf, std::size_t size, std::uint64_t offset) noexcept
    -> std::expected<void, io::IOError> {
    auto* ptr = static_cast<std::uint8_t*>(buf);
    std::size_t remaining = size;
    std::uint64_t current_offset = offset;

    while (remaining > 0) {
        const ssize_t n = ::pread(fd, ptr, remaining, static_cast<off_t>(current_offset));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return std::unexpected(detail::map_errno(errno));
        }
        if (n == 0) {
            return std::unexpected(io::IOError::HardwareError);
        }

        ptr += n;
        current_offset += static_cast<std::uint64_t>(n);
        remaining -= static_cast<std::size_t>(n);
    }

    return {};
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto write_exact(int fd, const void* buf, std::size_t size, std::uint64_t offset) noexcept
    -> std::expected<void, io::IOError> {
    const auto* ptr = static_cast<const std::uint8_t*>(buf);
    std::size_t remaining = size;
    std::uint64_t current_offset = offset;

    while (remaining > 0) {
        const ssize_t n = ::pwrite(fd, ptr, remaining, static_cast<off_t>(current_offset));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return std::unexpected(detail::map_errno(errno));
        }

        ptr += n;
        current_offset += static_cast<std::uint64_t>(n);
        remaining -= static_cast<std::size_t>(n);
    }

    return {};
}

} // namespace stratadb::utils::os
