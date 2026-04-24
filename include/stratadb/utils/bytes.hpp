#pragma once

#include <cstddef>

namespace stratadb::utils::bytes {

inline constexpr std::size_t KiB = 1024ULL;
inline constexpr std::size_t MiB = 1024ULL * KiB;
inline constexpr std::size_t GiB = 1024ULL * MiB;
inline constexpr std::size_t TiB = 1024ULL * GiB;

namespace literals {
consteval auto operator""_KiB(unsigned long long value) -> std::size_t {
    return static_cast<std::size_t>(value) * KiB;
}

consteval auto operator""_MiB(unsigned long long value) -> std::size_t {
    return static_cast<std::size_t>(value) * MiB;
}

consteval auto operator""_GiB(unsigned long long value) -> std::size_t {
    return static_cast<std::size_t>(value) * GiB;
}

consteval auto operator""_TiB(unsigned long long value) -> std::size_t {
    return static_cast<std::size_t>(value) * TiB;
}
} // namespace literals

} // namespace stratadb::utils::bytes