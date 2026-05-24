#include "stratadb/utils/probe.hpp"

#include <thread>

#if defined(__linux__)
#include <fcntl.h>
#include <fstream>
#include <linux/fs.h>
#include <string>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <unistd.h>
#elif defined(__APPLE__)
#include <sys/disk.h>
#include <sys/ioctl.h>
#include <sys/sysctl.h>
#include <unistd.h>
#elif defined(_WIN32)
#include <windows.h>
#endif

namespace stratadb::utils {


auto system_page_size() noexcept -> std::size_t {
#if defined(__linux__) || defined(__APPLE__)
    const long sz = ::sysconf(_SC_PAGESIZE);
    return (sz > 0) ? static_cast<std::size_t>(sz) : 4096UZ;
#elif defined(_WIN32)
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return static_cast<std::size_t>(si.dwPageSize);
#else
    return 4096UZ;
#endif
}

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
#elif defined(__APPLE__)
    std::uint64_t mem_size = 0;
    std::size_t len = sizeof(mem_size);
    if (::sysctlbyname("hw.memsize", &mem_size, &len, nullptr, 0) == 0 && len == sizeof(mem_size)) {
        return static_cast<std::size_t>(mem_size);
    }
#elif defined(_WIN32)
    MEMORYSTATUSEX status{};
    status.dwLength = sizeof(status);
    if (GlobalMemoryStatusEx(&status) != 0) {
        return static_cast<std::size_t>(status.ullTotalPhys);
    }
#endif
    return 0UZ;
}


auto probe_logical_sector_size(int fd) noexcept -> std::size_t {
#if defined(__linux__)
    unsigned int logical = 0;
    if (::ioctl(fd, BLKSSZGET, &logical) == 0) {
        return logical;
    }
#endif
    (void)fd;
    return 512UZ;
}

auto probe_physical_sector_size(int fd) noexcept -> std::size_t {
#if defined(__linux__)
    unsigned int physical = 0;
    if (::ioctl(fd, BLKPBSZGET, &physical) == 0) {
        return physical;
    }
#elif defined(__APPLE__)
    uint32_t physical = 0;
    if (::ioctl(fd, DKIOCGETBLOCKSIZE, &physical) == 0) {
        return physical;
    }
#endif
    (void)fd;
    return 4096UZ;
}

auto probe_atomic_write_unit_min(int fd) noexcept -> std::size_t {
#if defined(__linux__) && defined(STATX_WRITE_ATOMIC)
    struct statx stx{};
    if (::statx(fd, "", AT_EMPTY_PATH, STATX_WRITE_ATOMIC, &stx) == 0) {
        if (stx.stx_attributes_mask & STATX_ATTR_WRITE_ATOMIC) {
            return stx.stx_atomic_write_unit_min;
        }
    }
#endif
    return probe_physical_sector_size(fd);
}

auto probe_atomic_write_unit_max(int fd) noexcept -> std::size_t {
#if defined(__linux__) && defined(STATX_WRITE_ATOMIC)
    struct statx stx{};
    if (::statx(fd, "", AT_EMPTY_PATH, STATX_WRITE_ATOMIC, &stx) == 0) {
        if (stx.stx_attributes_mask & STATX_ATTR_WRITE_ATOMIC) {
            return stx.stx_atomic_write_unit_max;
        }
    }
#endif
    return probe_physical_sector_size(fd);
}

auto probe_is_rotational(int fd) noexcept -> bool {
#if defined(__linux__)
    struct stat st{};
    if (::fstat(fd, &st) == 0) {
        char path[64];
        std::snprintf(path, sizeof(path), "/sys/dev/block/%u:%u/queue/rotational", major(st.st_dev), minor(st.st_dev));
        std::ifstream file(path);
        int is_rot = 0;
        if (file >> is_rot) {
            return is_rot == 1;
        }
    }
#endif
    (void)fd;
    return false;
}

auto probe_supports_fua(int) noexcept -> bool {
#if defined(__linux__)
    return true;
#else
    return false;
#endif
}

auto probe_supports_rwf_atomic(int fd) noexcept -> bool {
#if defined(__linux__) && defined(STATX_WRITE_ATOMIC)
    struct statx stx{};
    if (::statx(fd, "", AT_EMPTY_PATH, STATX_WRITE_ATOMIC, &stx) == 0) {
        return (stx.stx_attributes_mask & STATX_ATTR_WRITE_ATOMIC) != 0;
    }
#endif
    (void)fd;
    return false;
}

auto probe_supports_fallocate(int) noexcept -> bool {
#if defined(__linux__)
    return true;
#else
    return false;
#endif
}

}