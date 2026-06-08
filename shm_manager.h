#pragma once

#include "shm_layout.h"

#include <cstddef>
#include <string>

namespace mdsys {

struct ShmContext {
    ControlSegment* ctrl = nullptr;
    RingSegment* rings = nullptr;
    DayLogSegment* daylog = nullptr;
};

class ShmManager {
public:
    ShmManager() = default;
    ~ShmManager();

    ShmManager(const ShmManager&) = delete;
    ShmManager& operator=(const ShmManager&) = delete;

    bool create(bool reset_existing, uint32_t trading_day);
    bool open(bool include_daylog, bool read_only);
    void close();

    ShmContext* context() { return &ctx_; }
    const ShmContext* context() const { return &ctx_; }
    const std::string& last_error() const { return last_error_; }

    static void unlink_all();

private:
    bool map_segment(const char* name,
                     size_t size,
                     bool create_segment,
                     bool read_only,
                     void** out);
    bool initialize(uint32_t trading_day);

    ShmContext ctx_;
    size_t ctrl_size_ = sizeof(ControlSegment);
    size_t rings_size_ = sizeof(RingSegment);
    size_t daylog_size_ = sizeof(DayLogSegment);
    bool has_ctrl_ = false;
    bool has_rings_ = false;
    bool has_daylog_ = false;
    std::string last_error_;
};

}  // namespace mdsys
