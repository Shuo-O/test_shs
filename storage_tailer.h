#pragma once

#include "shm_manager.h"

#include <atomic>
#include <string>
#include <thread>
#include <vector>

namespace mdsys {

class StorageTailer {
public:
    StorageTailer(ShmContext* ctx, std::string wal_dir);
    ~StorageTailer();

    StorageTailer(const StorageTailer&) = delete;
    StorageTailer& operator=(const StorageTailer&) = delete;

    bool start();
    void stop();
    const std::string& last_error() const { return last_error_; }

private:
    void run();
    bool flush_buffer();
    bool open_next_file();
    bool write_all(const void* data, size_t size);
    void close_file();

    ShmContext* ctx_;
    std::string wal_dir_;
    std::string last_error_;
    std::atomic<bool> running_{false};
    std::thread thread_;
    std::vector<TickRecord> buffer_;
    int fd_ = -1;
    uint64_t next_read_seq_ = 0;
    uint64_t rows_in_segment_ = 0;
    uint64_t segment_index_ = 0;
};

}  // namespace mdsys
