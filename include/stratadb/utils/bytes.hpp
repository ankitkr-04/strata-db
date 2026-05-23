#pragma once

#include <cstddef>

namespace stratadb::utils::bytes {

inline constexpr std::size_t KiB = 1024UZ;
inline constexpr std::size_t MiB = 1024UZ * KiB;
inline constexpr std::size_t GiB = 1024UZ * MiB;

} // namespace stratadb::utils::bytes