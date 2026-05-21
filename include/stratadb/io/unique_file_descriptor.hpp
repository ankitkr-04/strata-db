#pragma once

#include "stratadb/utils/os.hpp"

#include <utility>

namespace stratadb::io {
class UniqueFd {
  public:
    constexpr UniqueFd() noexcept = default;
    explicit UniqueFd(int fd) noexcept
        : fd_(fd) {}

    ~UniqueFd() noexcept {
        reset();
    }

    UniqueFd(const UniqueFd&) = delete;
    auto operator=(const UniqueFd&) -> UniqueFd& = delete;

    constexpr UniqueFd(UniqueFd&& other) noexcept
        : fd_(std::exchange(other.fd_, -1)) {}

    constexpr auto operator=(UniqueFd&& other) noexcept -> UniqueFd& {
        if (this != &other) {
            reset(std::exchange(other.fd_, -1));
        }
        return *this;
    }

    [[nodiscard]] constexpr auto get() const noexcept -> int {
        return fd_;
    }
    [[nodiscard]] constexpr auto is_valid() const noexcept -> bool {
        return fd_ >= 0;
    }

    constexpr auto release() noexcept -> int {
        return std::exchange(fd_, -1);
    }

    void reset(int new_fd = -1) noexcept {
        if (is_valid()) {
            utils::os::close_fd(fd_);
        }
        fd_ = new_fd;
    }

  private:
    int fd_{-1};
};
} // namespace stratadb::io