#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace stratadb::test {

template <std::size_t N>
auto as_bytes(const char (&lit)[N]) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(lit), N - 1};
}

inline auto sv_bytes(std::string_view sv) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(sv.data()), sv.size()};
}

template <typename Block>
auto raw(const Block& block) -> const std::uint8_t* {
    return reinterpret_cast<const std::uint8_t*>(&block);
}

} // namespace stratadb::test
