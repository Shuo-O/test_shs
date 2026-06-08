#pragma once

#include "shm_layout.h"

#include <cstddef>
#include <string>

namespace mdsys {

// Pointers into the three mapped regions. A reader maps control+rings only.
struct Mapping {
    ControlRegion* ctrl = nullptr;
    RingRegion*    rings = nullptr;
    LogRegion*     log = nullptr;
};

// Owns the shm_open/mmap lifecycle for one process. The writer calls create();
// strategy readers call open(read_only=true, with_log=false).
class ShmManager {
public:
    ShmManager() = default;
    ~ShmManager();
    ShmManager(const ShmManager&) = delete;
    ShmManager& operator=(const ShmManager&) = delete;

    bool create(uint32_t trading_day);         // writer: fresh, zeroed, initialized
    bool open(bool read_only, bool with_log);  // attach to an existing region
    void close();

    Mapping&       mapping()       { return map_; }
    const Mapping& mapping() const { return map_; }
    const std::string& error() const { return error_; }

    static void unlink();  // remove the named segments

private:
    bool map_one(const char* name, size_t size, bool create_it, bool read_only, void** out);
    void initialize(uint32_t trading_day);

    Mapping map_;
    std::string error_;
};

}  // namespace mdsys
