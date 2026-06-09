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
// append, one seqlock ring publish, publish the committed sequence, return.
// Persistence (WAL + Parquet) runs on the separate tailer thread.
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

    // Optional warm-up so the first tick of a symbol is not on the slow path.
    int32_t register_instrument(int32_t instrument_id);

    mdsys::Mapping*    mapping();
    const std::string& last_error() const { return last_error_; }

private:
    int32_t resolve(int32_t instrument_id);  // lookup, lazily registering

    std::string wal_dir_;
    uint32_t    trading_day_;
    std::unique_ptr<mdsys::ShmManager>    shm_;
    std::unique_ptr<mdsys::StorageTailer> tailer_;
    uint64_t global_seq_ = 0;
    int32_t  next_symbol_index_ = 0;
    bool     started_ = false;
    std::string last_error_;
};
