#pragma once

#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

namespace stratadb::utils {

// Overflow-safe upward alignment. Returns false (out is unchanged) if:
//   - alignment is not a power of 2, or
//   - (value + alignment - 1) would overflow UInt.
// Use during construction / validation where the inputs are untrusted.
template <typename UInt>
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] constexpr auto try_align_up(UInt value, UInt alignment, UInt& out) noexcept -> bool {
    static_assert(std::is_unsigned_v<UInt>, "try_align_up requires an unsigned integer type");
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

// Fast upward alignment for hot paths where inputs are already validated.
// Debug-asserts power-of-2; behaviour is undefined on overflow.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] constexpr auto align_up(std::uintptr_t value, std::size_t alignment) noexcept -> std::uintptr_t {
    assert(std::has_single_bit(alignment));
    const auto mask = static_cast<std::uintptr_t>(alignment - 1);
    return (value + mask) & ~mask;
}

} // namespace stratadb::utils