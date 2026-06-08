#pragma once

#include "md.h"
#include "shm_manager.h"

#include <cstdint>
#include <memory>
#include <string>

namespace mdsys {
class StorageTailer;
}

// Market-data store. on_md is the hot path: id->index lookup, one day-log
// append, one seqlock ring publish, publish sequence, return. Everything else
// (WAL, Parquet) runs on the tailer thread.
class DemoMd {
public:
    explicit DemoMd(std::string wal_dir = "wal",
                    uint32_t trading_day = 20260608);
    ~DemoMd();

    DemoMd(const DemoMd&) = delete;
    DemoMd& operator=(const DemoMd&) = delete;

    bool start();
    void stop();
    void on_md(const MDUniOrder& order);

    // Optional warm-up: register a symbol up front so the very first tick of each
    // symbol stays off the (lazy-registration) slow path. on_md still registers
    // lazily for any symbol seen before this is called.
    int32_t register_instrument(int32_t instrument_id);

    mdsys::Mapping*    mapping();
    const std::string& last_error() const { return last_error_; }

private:
    int32_t resolve(int32_t instrument_id);  // lookup, lazily registering
    void publish_stats();

    std::string wal_dir_;
    uint32_t    trading_day_;
    std::unique_ptr<mdsys::ShmManager>    shm_;
    std::unique_ptr<mdsys::StorageTailer> tailer_;
    uint64_t global_seq_ = 0;
    int32_t  next_symbol_index_ = 0;
    bool     started_ = false;
    std::string last_error_;

    // Writer-local monitoring accumulators. Single-writer, so these need no
    // atomics; they are published to shared memory on a stride (see on_md).
    uint64_t local_received_ = 0;
    uint64_t local_in_window_ = 0;
    uint64_t local_dropped_ = 0;
    uint64_t durable_view_ = 0;  // cached durable cursor, refreshed on a stride
};
