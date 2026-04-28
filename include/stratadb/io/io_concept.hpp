#pragma once

#include <concepts>
#include <span>
#include <sys/uio.h>

namespace stratadb::io {
template <typename T>
concept IsIOEngine = requires(T engine, int fd, std::span<const struct iovec> iov) {
    // Scatter-gather write. Returns number of bytes written, or -1 on error.
    { engine.write_vectored(fd, iov) } -> std::same_as<bool>;

    // Force durability: flushes OS buffers to disk. Returns true on success, false on failure.
    { engine.flush(fd) } -> std::convertible_to<bool>;
};
} // namespace stratadb::io