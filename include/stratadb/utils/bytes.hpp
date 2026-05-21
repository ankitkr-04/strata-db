#pragma once

#include <cstddef>

namespace stratadb::utils::bytes {

inline constexpr std::size_t KiB = 1024ULL;
inline constexpr std::size_t MiB = 1024ULL * KiB;
inline constexpr std::size_t GiB = 1024ULL * MiB;

} // namespace stratadb::utils::bytes