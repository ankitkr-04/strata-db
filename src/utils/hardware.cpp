#include "stratadb/utils/hardware.hpp"

#if defined(__linux__)
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <unistd.h>
#elif defined(__APPLE__)
#include <sys/sysctl.h>
#include <sys/disk.h>
#include <sys/ioctl.h>
#include <unistd.h>
#elif defined(_WIN32)
#include <windows.h>
#endif

namespace stratadb::utils {

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto probe_atomic_write_boundary(int fd, std::size_t fallback_size) noexcept -> std::size_t {
#if defined(__linux__)
    unsigned int physical_block_size = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
    if (ioctl(fd, BLKPBSZGET, &physical_block_size) == 0) {
        return physical_block_size;
    }

#elif defined(__APPLE__)
    uint32_t physical_block_size = 0;
    if (ioctl(fd, DKIOCGETBLOCKSIZE, &physical_block_size) == 0) {
        return physical_block_size;
    }
#else
    // Windows or POSIX fallback
    (void)fd; // Unused parameter
#endif
    return fallback_size;
};

auto system_page_size() noexcept -> std::size_t {
#if defined(__linux__) || defined(__APPLE__)
    const long sz = ::sysconf(_SC_PAGESIZE);
    return (sz > 0) ? static_cast<std::size_t>(sz) : 4096UZ;
#elif defined(_WIN32)
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return static_cast<std::size_t>(si.dwPageSize);
#else
    return 4096UZ; // Default fallback
#endif
};

auto logical_core_count() noexcept -> std::uint32_t {
#if defined(__linux__)
    const long count = ::sysconf(_SC_NPROCESSORS_ONLN);
    return (count > 0) ? static_cast<std::uint32_t>(count) : 1U;
#else
    const unsigned hw = std::thread::hardware_concurrency();
    return (hw > 0) ? static_cast<std::uint32_t>(hw) : 1U;
#endif
}

auto total_physical_memory_bytes() noexcept -> std::size_t {
#if defined(__linux__)
    const long page_size = ::sysconf(_SC_PAGESIZE);
    const long page_count = ::sysconf(_SC_PHYS_PAGES);
    if (page_size > 0 && page_count > 0) {
        return static_cast<std::size_t>(page_size) * static_cast<std::size_t>(page_count);
    }
    return 0UZ;
#elif defined(__APPLE__)
    std::uint64_t mem_size = 0;
    std::size_t len = sizeof(mem_size);
    if (::sysctlbyname("hw.memsize", &mem_size, &len, nullptr, 0) == 0 && len == sizeof(mem_size)) {
        return static_cast<std::size_t>(mem_size);
    }
    return 0UZ;
#elif defined(_WIN32)
    MEMORYSTATUSEX status{};
    status.dwLength = sizeof(status);
    if (GlobalMemoryStatusEx(&status) != 0) {
        return static_cast<std::size_t>(status.ullTotalPhys);
    }
    return 0UZ;
#else
    return 0UZ;
#endif
}

} // namespace stratadb::utils