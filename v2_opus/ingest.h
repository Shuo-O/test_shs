#pragma once

#include "md.h"
#include "shm.h"

#include <memory>
#include <string>

namespace mdv2 {
class Tailer;
}

// v2 market-data store. on_md is the hot path: id->index lookup, one day-log
// append, one seqlock ring publish, publish sequence, return. Everything else
// (WAL, Parquet) runs on the tailer thread.
class MdStore {
public:
    explicit MdStore(std::string wal_dir = "wal_v2",
                     uint32_t trading_day = 20260608);
    ~MdStore();
    MdStore(const MdStore&) = delete;
    MdStore& operator=(const MdStore&) = delete;

    bool start();
    void stop();
    void on_md(const MDUniOrder& order);

    // Optional warm-up: register a symbol so the hot path is a pure lookup.
    // Unregistered symbols are still handled (registered lazily) but doing it
    // up front keeps the very first tick of each symbol off the slow path.
    int32_t register_instrument(int32_t instrument_id);

    mdv2::Mapping*     mapping();
    const std::string& error() const { return error_; }

private:
    int32_t resolve(int32_t instrument_id);  // lookup, lazily registering
    void publish_stats();

    std::string wal_dir_;
    uint32_t    trading_day_;
    std::unique_ptr<mdv2::Shm>    shm_;
    std::unique_ptr<mdv2::Tailer> tailer_;
    uint64_t    global_seq_ = 0;
    int32_t     next_index_ = 0;
    bool        started_ = false;
    std::string error_;

    // Writer-local monitoring accumulators, published on kStatsStride.
    uint64_t local_received_ = 0;
    uint64_t local_in_window_ = 0;
    uint64_t local_dropped_ = 0;
    uint64_t durable_view_ = 0;  // cached tailer durable cursor
};
