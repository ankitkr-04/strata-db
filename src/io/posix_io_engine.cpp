#include "stratadb/io/posix_io_engine.hpp"

#include <cassert>
#include <cerrno>
#include <climits>
#include <sys/uio.h>
#include <unistd.h>

namespace stratadb::io {

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

    // POSIX guarantees IOV_MAX entries max per writev/pwritev call.
    assert(iovs.size() <= static_cast<size_t>(IOV_MAX));

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

auto PosixIoEngine::sync(FileHandle fd) const noexcept -> std::expected<void, IOError> {

    if (::fdatasync(fd) < 0) {
        return std::unexpected(IOError::HardwareError);
    }

    return {};
}

} // namespace stratadb::io