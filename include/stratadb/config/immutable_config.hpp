#pragma once

#include <cstddef>
#include <string>

namespace stratadb::config {

namespace defaults {
inline constexpr std::size_t BLOCK_SIZE = 4096;
inline constexpr const char* WAL_DIRECTORY = "./wal";
} // namespace defaults

struct ImmutableConfig {
    std::size_t block_size{defaults::BLOCK_SIZE};
    std::string wal_directory{defaults::WAL_DIRECTORY};
};

} // namespace stratadb::config