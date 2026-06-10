#include "../support/wal_test_helpers.hpp"
#include "stratadb/io/io_types.hpp"
#include "stratadb/io/posix_io_engine.hpp"
#include "stratadb/platform/hardware_model.hpp"
#include "stratadb/utils/os.hpp"

#include <array>
#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <sys/uio.h>
#include <unistd.h>
#include <vector>

using namespace stratadb::io;
using namespace stratadb::platform;

namespace {

struct TempFile {
    std::string path;
    int fd{-1};

    TempFile() {
        char tpl[] = "/tmp/posix_io_test_XXXXXX";
        fd = ::mkstemp(tpl);
        if (fd >= 0)
            path = tpl;
    }
    TempFile(TempFile&&) = delete;
    auto operator=(TempFile&&) -> TempFile& = delete;
    ~TempFile() {
        if (fd >= 0)
            ::close(fd);
        if (!path.empty())
            ::unlink(path.c_str());
    }
    TempFile(const TempFile&) = delete;
    auto operator=(const TempFile&) -> TempFile& = delete;
};

[[nodiscard]] auto make_io_info() -> HardwareInfo::Io {
    return stratadb::wal::test::make_test_hw_info().io;
}

} // namespace

TEST(PosixIoEngine, BasicBufferedWrite) {
    TempFile f;
    ASSERT_GE(f.fd, 0);

    PosixIoEngine engine(make_io_info());

    alignas(512) std::array<char, 512> buf{};
    std::memcpy(buf.data(), "hello_world", 11);

    struct iovec iov{.iov_base = buf.data(), .iov_len = buf.size()};
    auto result = engine.writev(f.fd, std::span(&iov, 1), 0);

    ASSERT_TRUE(result.has_value()) << "writev failed";
    EXPECT_EQ(*result, buf.size());
}

TEST(PosixIoEngine, ReadBackData) {
    TempFile f;
    ASSERT_GE(f.fd, 0);

    PosixIoEngine engine(make_io_info());

    alignas(512) std::array<std::byte, 512> write_buf{};
    write_buf[0] = std::byte{0xDE};
    write_buf[1] = std::byte{0xAD};
    write_buf[2] = std::byte{0xBE};
    write_buf[3] = std::byte{0xEF};

    struct iovec iov{.iov_base = write_buf.data(), .iov_len = write_buf.size()};
    auto wr = engine.writev(f.fd, std::span(&iov, 1), 0);
    ASSERT_TRUE(wr.has_value());

    alignas(512) std::array<std::byte, 512> read_buf{};
    auto rd = engine.read(f.fd, std::span(read_buf), 0);
    ASSERT_TRUE(rd.has_value());

    EXPECT_EQ(read_buf[0], std::byte{0xDE});
    EXPECT_EQ(read_buf[1], std::byte{0xAD});
    EXPECT_EQ(read_buf[2], std::byte{0xBE});
    EXPECT_EQ(read_buf[3], std::byte{0xEF});
}

TEST(PosixIoEngine, SyncSucceeds) {
    TempFile f;
    ASSERT_GE(f.fd, 0);

    PosixIoEngine engine(make_io_info());

    alignas(512) std::array<char, 512> buf{};
    struct iovec iov{.iov_base = buf.data(), .iov_len = buf.size()};
    ASSERT_TRUE(engine.writev(f.fd, std::span(&iov, 1), 0).has_value());

    auto sync_result = engine.sync(f.fd);
    EXPECT_TRUE(sync_result.has_value()) << "sync (fdatasync) failed";
}

TEST(PosixIoEngine, VectorizedMultiIovWrite) {
    TempFile f;
    ASSERT_GE(f.fd, 0);

    PosixIoEngine engine(make_io_info());

    // Three separate 512-byte sectors.
    alignas(512) std::array<char, 512> buf0{}, buf1{}, buf2{};
    std::memset(buf0.data(), 'A', buf0.size());
    std::memset(buf1.data(), 'B', buf1.size());
    std::memset(buf2.data(), 'C', buf2.size());

    std::array<struct iovec, 3> iovs{{
        {buf0.data(), buf0.size()},
        {buf1.data(), buf1.size()},
        {buf2.data(), buf2.size()},
    }};

    auto result = engine.writev(f.fd, std::span(iovs), 0);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 3 * 512u);

    // Read back sector 1 and verify it contains 'B'.
    alignas(512) std::array<char, 512> verify{};
    ::pread(f.fd, verify.data(), verify.size(), 512);
    EXPECT_EQ(verify[0], 'B');
    EXPECT_EQ(verify[511], 'B');
}

// O_DIRECT requires sector-aligned buffer and offset.  With a deliberately
// misaligned buffer the engine must return AlignmentViolation (in release) or
// trigger a debug assert.  We only run this test when O_DIRECT is available.
TEST(PosixIoEngine, AlignmentViolationOnDirectFd) {
    // Probe O_DIRECT support.
    char probe_tpl[] = "/tmp/posix_direct_probe_XXXXXX";
    int probe_fd = ::mkstemp(probe_tpl);
    if (probe_fd < 0)
        GTEST_SKIP() << "Cannot create probe file";
    ::posix_fallocate(probe_fd, 0, 4096);
    ::fdatasync(probe_fd);
    ::close(probe_fd);

    auto direct_result = stratadb::utils::os::open_direct(probe_tpl);
    ::unlink(probe_tpl);
    if (!direct_result.has_value()) {
        GTEST_SKIP() << "O_DIRECT not supported on this filesystem";
    }
    int direct_fd = *direct_result;

    auto info = make_io_info();
    PosixIoEngine engine(info);

    // A deliberately non-aligned buffer (offset by 1 byte from alignment).
    alignas(4096) std::array<char, 8192> aligned_storage{};
    char* misaligned = aligned_storage.data() + 1;

    struct iovec iov{.iov_base = misaligned, .iov_len = 512};

    if (iov.iov_base == nullptr) {
        GTEST_SKIP() << "Failed to create misaligned buffer; skipping test";
    }

    // Release builds report the syscall EINVAL as AlignmentViolation.
    // Debug builds assert before the syscall.
#ifdef NDEBUG
    auto result = engine.writev(direct_fd, std::span(&iov, 1), 0);
    EXPECT_FALSE(result.has_value());
    if (!result.has_value()) {
        EXPECT_EQ(result.error(), IOError::AlignmentViolation);
    }
#else
    GTEST_SKIP() << "Debug assert fires before syscall; skipping in debug mode";
#endif

    ::close(direct_fd);
}

#ifndef NDEBUG
namespace stratadb::io::test_hooks {
extern std::atomic<bool> mock_io_error;
extern std::atomic<ssize_t> mock_short_write_bytes;
} // namespace stratadb::io::test_hooks

TEST(PosixIoEngine, MockIoErrorReturnsHardwareError) {
    TempFile f;
    ASSERT_GE(f.fd, 0);

    stratadb::io::test_hooks::mock_io_error.store(true);

    PosixIoEngine engine(make_io_info());
    alignas(512) std::array<char, 512> buf{};
    struct iovec iov{buf.data(), buf.size()};
    auto result = engine.writev(f.fd, std::span(&iov, 1), 0);

    stratadb::io::test_hooks::mock_io_error.store(false);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), IOError::HardwareError);
}

TEST(PosixIoEngine, MockShortWriteLimitsBytes) {
    TempFile f;
    ASSERT_GE(f.fd, 0);

    stratadb::io::test_hooks::mock_short_write_bytes.store(128);

    PosixIoEngine engine(make_io_info());
    alignas(512) std::array<char, 512> buf{};
    struct iovec iov{buf.data(), buf.size()};
    auto result = engine.writev(f.fd, std::span(&iov, 1), 0);

    stratadb::io::test_hooks::mock_short_write_bytes.store(-1);

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 128u);
}
#endif // !NDEBUG
