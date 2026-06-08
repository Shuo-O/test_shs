#pragma once

#include "md.h"
#include "shm_manager.h"

#include <cstdint>
#include <memory>
#include <string>

namespace mdsys {
class StorageTailer;
}

class DemoMd{
public:
    explicit DemoMd(std::string wal_dir = "wal",
                    uint32_t trading_day = 20260608,
                    bool reset_existing = true);
    ~DemoMd();

    DemoMd(const DemoMd&) = delete;
    DemoMd& operator=(const DemoMd&) = delete;

    void on_md(const MDUniOrder& order);

    bool start();
    void stop();
    mdsys::ShmContext* context();
    const std::string& last_error() const { return last_error_; }

private:
    int32_t get_or_register_instrument(int32_t instrument_id);

    std::string wal_dir_;
    uint32_t trading_day_;
    bool reset_existing_;
    std::unique_ptr<mdsys::ShmManager> shm_;
    std::unique_ptr<mdsys::StorageTailer> tailer_;
    uint64_t global_seq_ = 0;
    int32_t next_symbol_index_ = 0;
    bool started_ = false;
    std::string last_error_;
};
