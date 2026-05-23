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

namespace detail {

#if defined(__linux__)

auto probe_io_capabilities_impl(int fd) noexcept -> io::IOCapabilities {
    io::IOCapabilities caps{
        .logical_sector_size = 512,
        .physical_sector_size = 4096,
        .is_rotational = false,
        .supports_fua = true,
        .atomic_write_unit_min = 4096,
        .atomic_write_unit_max = 4096,
        .supports_rwf_atomic = false,
        .supports_fallocate = true // Linux filesystems generally support this
    };

    // 1. Sector Sizes
    unsigned int logical = 0;
    if (ioctl(fd, BLKSSZGET, &logical) == 0) {
        caps.logical_sector_size = logical;
    }

    unsigned int physical = 0;
    if (ioctl(fd, BLKPBSZGET, &physical) == 0) {
        caps.physical_sector_size = physical;
        caps.atomic_write_unit_min = physical;
        caps.atomic_write_unit_max = physical;
    }

    // 2. Rotational Media Check
    struct stat st{};
    if (fstat(fd, &st) == 0) {
        char path[64];
        std::snprintf(path, sizeof(path), "/sys/dev/block/%u:%u/queue/rotational", major(st.st_dev), minor(st.st_dev));
        std::ifstream file(path);
        int is_rot = 0;
        if (file >> is_rot) {
            caps.is_rotational = (is_rot == 1);
        }
    }

// STATX_WRITE_ATOMIC (Linux 6.11+)
#if defined(STATX_WRITE_ATOMIC)
    struct statx stx{};
    if (statx(fd, "", AT_EMPTY_PATH, STATX_WRITE_ATOMIC, &stx) == 0) {
        if (stx.stx_attributes_mask & STATX_ATTR_WRITE_ATOMIC) {
            caps.supports_rwf_atomic = true;
            caps.atomic_write_unit_min = stx.stx_atomic_write_unit_min;
            caps.atomic_write_unit_max = stx.stx_atomic_write_unit_max;
        }
    }
#endif

    return caps;
}

auto system_page_size_impl() noexcept -> std::size_t {
    const long sz = ::sysconf(_SC_PAGESIZE);
    return (sz > 0) ? static_cast<std::size_t>(sz) : 4096UZ;
}

auto total_physical_memory_bytes_impl() noexcept -> std::size_t {
    const long page_size = ::sysconf(_SC_PAGESIZE);
    const long page_count = ::sysconf(_SC_PHYS_PAGES);
    if (page_size > 0 && page_count > 0) {
        return static_cast<std::size_t>(page_size) * static_cast<std::size_t>(page_count);
    }
    return 0UZ;
}

#elif defined(__APPLE__)

auto probe_io_capabilities_impl(int fd) noexcept -> io::IOCapabilities {
    io::IOCapabilities caps{.logical_sector_size = 512,
                            .physical_sector_size = 4096,
                            .is_rotational = false,
                            .supports_fua = false,
                            .atomic_write_unit_min = 4096,
                            .atomic_write_unit_max = 4096,
                            .supports_rwf_atomic = false,
                            .supports_fallocate = false};
    uint32_t physical = 0;
    if (ioctl(fd, DKIOCGETBLOCKSIZE, &physical) == 0) {
        caps.physical_sector_size = physical;
        caps.atomic_write_unit_min = physical;
        caps.atomic_write_unit_max = physical;
    }
    return caps;
}

auto system_page_size_impl() noexcept -> std::size_t {
    const long sz = ::sysconf(_SC_PAGESIZE);
    return (sz > 0) ? static_cast<std::size_t>(sz) : 4096UZ;
}

auto total_physical_memory_bytes_impl() noexcept -> std::size_t {
    std::uint64_t mem_size = 0;
    std::size_t len = sizeof(mem_size);
    if (::sysctlbyname("hw.memsize", &mem_size, &len, nullptr, 0) == 0 && len == sizeof(mem_size)) {
        return static_cast<std::size_t>(mem_size);
    }
    return 0UZ;
}

#elif defined(_WIN32)

auto probe_io_capabilities_impl(int /*fd*/) noexcept -> io::IOCapabilities {
    return io::IOCapabilities{.logical_sector_size = 512,
                              .physical_sector_size = 4096,
                              .is_rotational = false,
                              .supports_fua = false,
                              .atomic_write_unit_min = 4096,
                              .atomic_write_unit_max = 4096,
                              .supports_rwf_atomic = false,
                              .supports_fallocate = false};
}

auto system_page_size_impl() noexcept -> std::size_t {
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return static_cast<std::size_t>(si.dwPageSize);
}

auto total_physical_memory_bytes_impl() noexcept -> std::size_t {
    MEMORYSTATUSEX status{};
    status.dwLength = sizeof(status);
    if (GlobalMemoryStatusEx(&status) != 0) {
        return static_cast<std::size_t>(status.ullTotalPhys);
    }
    return 0UZ;
}

#endif

} // namespace detail

auto probe_io_capabilities(int fd) noexcept -> io::IOCapabilities {
    return detail::probe_io_capabilities_impl(fd);
}

auto system_page_size() noexcept -> std::size_t {
    return detail::system_page_size_impl();
}

auto total_physical_memory_bytes() noexcept -> std::size_t {
    return detail::total_physical_memory_bytes_impl();
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

} // namespace stratadb::utils