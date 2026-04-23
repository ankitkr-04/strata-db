#pragma once

#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

namespace stratadb::utils {

template <typename UInt>
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] constexpr auto align_up_checked(UInt value, UInt alignment, UInt& out) noexcept -> bool {
    static_assert(std::is_unsigned_v<UInt>, "align_up_checked requires an unsigned integer type");

    assert(std::has_single_bit(alignment));
    if (!std::has_single_bit(alignment)) {
        return false;
    }

    const UInt mask = alignment - 1;
    if (value > std::numeric_limits<UInt>::max() - mask) {
        return false;
    }

    out = (value + mask) & ~mask;
    return true;
}
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] constexpr auto align_up_pow2(std::uintptr_t value, std::size_t alignment) noexcept -> std::uintptr_t {
    assert(std::has_single_bit(alignment));
    const auto mask = static_cast<std::uintptr_t>(alignment - 1);
    return (value + mask) & ~mask;
}

} // namespace stratadb::utils
