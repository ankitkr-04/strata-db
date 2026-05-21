#include "stratadb/io/posix_io_engine.hpp"

#include <atomic>
#include <cassert>
#include <cerrno>
#include <climits>
#include <sys/uio.h>
#include <unistd.h>

namespace stratadb::io {
#ifndef NDEBUG
namespace test_hooks {
alignas(64) std::atomic<bool> mock_io_error{false};
alignas(64) std::atomic<bool> mock_sync_error{false};
alignas(64) std::atomic<ssize_t> mock_short_write_bytes{-1};
} // namespace test_hooks
#endif

namespace {

inline void assert_aligned(const void* ptr, size_t alignment) noexcept {
    assert(reinterpret_cast<uintptr_t>(ptr) % alignment == 0);
}

inline auto map_write_errno() noexcept -> IOError {
    switch (errno) {
        case ENOSPC:
            return IOError::DeviceFull;

        case EINVAL:
            return IOError::AlignmentViolation;

        default:
            return IOError::HardwareError;
    }
}

inline auto map_read_errno() noexcept -> IOError {
    switch (errno) {
        case EINVAL:
            return IOError::AlignmentViolation;

        default:
            return IOError::HardwareError;
    }
}

} // namespace
auto PosixIoEngine::writev(FileHandle fd, std::span<const struct iovec> iovs, uint64_t offset) const noexcept
    -> std::expected<size_t, IOError> {

    assert(offset % caps_.logical_sector_size == 0);
    assert(!iovs.empty());
    assert_aligned(iovs[0].iov_base, caps_.logical_sector_size);
    assert(iovs.size() <= static_cast<size_t>(IOV_MAX));

#ifndef NDEBUG
    // Dynamic fault interception for tests
    if (test_hooks::mock_io_error.load(std::memory_order_acquire)) {
        return std::unexpected(IOError::HardwareError);
    }

    size_t requested_total = 0;
    for (const auto& io : iovs) {
        requested_total += io.iov_len;
    }

    ssize_t short_limit = test_hooks::mock_short_write_bytes.load(std::memory_order_acquire);
    if (short_limit >= 0 && requested_total > static_cast<size_t>(short_limit)) {
        // Force a short-write execution limit to test your WAL recovery loop mechanics
        return static_cast<size_t>(short_limit);
    }
#endif

    const int iovcnt = static_cast<int>(iovs.size());
    const off_t posix_offset = static_cast<off_t>(offset);
    ssize_t bytes_written = -1;

#if defined(__linux__) && defined(RWF_ATOMIC)
    if (caps_.supports_rwf_atomic) {
        bytes_written = ::pwritev2(fd, iovs.data(), iovcnt, posix_offset, RWF_ATOMIC);
    } else {
        bytes_written = ::pwritev(fd, iovs.data(), iovcnt, posix_offset);
    }
#else
    bytes_written = ::pwritev(fd, iovs.data(), iovcnt, posix_offset);
#endif

    if (bytes_written < 0) {
        return std::unexpected(map_write_errno());
    }

    return static_cast<size_t>(bytes_written);
}

auto PosixIoEngine::sync(FileHandle fd) const noexcept -> std::expected<void, IOError> {
#ifndef NDEBUG
    if (test_hooks::mock_sync_error.load(std::memory_order_acquire)) {
        return std::unexpected(IOError::HardwareError);
    }
#endif

    if (::fdatasync(fd) < 0) {
        return std::unexpected(IOError::HardwareError);
    }

    return {};
}

auto PosixIoEngine::read(FileHandle fd, std::span<std::byte> buffer, uint64_t offset) const noexcept
    -> std::expected<size_t, IOError> {

    assert(offset % caps_.logical_sector_size == 0);
    assert_aligned(buffer.data(), caps_.logical_sector_size);

    const off_t posix_offset = static_cast<off_t>(offset);

    const ssize_t bytes_read = ::pread(fd, buffer.data(), buffer.size(), posix_offset);

    if (bytes_read < 0) {
        return std::unexpected(map_read_errno());
    }

    return static_cast<size_t>(bytes_read);
}

} // namespace stratadb::io