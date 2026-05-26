#pragma once

#include "io_types.hpp"
#include "stratadb/utils/os.hpp"

#include <utility>

namespace stratadb::io {
class UniqueFd {
  public:
    constexpr UniqueFd() noexcept = default;
    explicit UniqueFd(FileHandle fd) noexcept
        : fd_(fd) {}

    ~UniqueFd() noexcept {
        reset();
    }

    UniqueFd(const UniqueFd&) = delete;
    auto operator=(const UniqueFd&) -> UniqueFd& = delete;

    constexpr UniqueFd(UniqueFd&& other) noexcept
        : fd_(std::exchange(other.fd_, -1)) {}

    auto operator=(UniqueFd&& other) noexcept -> UniqueFd& {
        if (this != &other) {
            reset(std::exchange(other.fd_, -1));
        }
        return *this;
    }

    [[nodiscard]] constexpr auto get() const noexcept -> FileHandle {
        return fd_;
    }
    [[nodiscard]] constexpr auto is_valid() const noexcept -> bool {
        return fd_ >= 0;
    }

    constexpr auto release() noexcept -> FileHandle {
        return std::exchange(fd_, -1);
    }

    void reset(FileHandle new_fd = -1) noexcept {
        if (is_valid()) {
            utils::os::close_fd(fd_);
        }
        fd_ = new_fd;
    }

  private:
    FileHandle fd_{-1};
};
} // namespace stratadb::io