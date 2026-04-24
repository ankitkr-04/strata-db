#pragma once

#include <cstdint>

namespace stratadb::config {

enum class IoStrategy : std::uint8_t {
    // Attempt to probe /sys/block. If permission denied (e.g., Docker),
    // safely fallback to AdaptiveBuffered.
    AutoDetect,

    // FOR: Enterprise PLP NVMe.
    // MECHANISM: io_uring OP_URING_CMD passthrough. Zero fdatasync. Micro-batching.
    DirectUringPassthrough,

    // FOR: Consumer NVMe / SATA SSDs.
    // MECHANISM: io_uring block I/O + fdatasync. Absorbs 500ms GC stalls.
    AdaptiveBuffered,

    // FOR: 7200RPM HDDs / Slow Network Block Storage.
    // MECHANISM: multi-megabyte macro-batching to minimize mechanical seeks.
    MacroBatched
};

} // namespace stratadb::config