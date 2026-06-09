#pragma once

#include "shm_manager.h"

#include <atomic>
#include <string>
#include <thread>
#include <vector>

namespace mdsys {

// Background thread that consumes the day log by increasing global_seq, writes
// the in-window records to an append-only WAL in batches, and advances
// durable_seq only after fsync. It never blocks the ingest thread.
class StorageTailer {
public:
    StorageTailer(Mapping* m, std::string wal_dir);
    ~StorageTailer();
    StorageTailer(const StorageTailer&) = delete;
    StorageTailer& operator=(const StorageTailer&) = delete;

    bool start();
    void stop();
    const std::string& error() const { return error_; }

private:
    void run();
    bool flush();          // write+fsync the buffer, then advance durable_seq
    void publish_durable();
    bool open_segment();
    bool write_all(const void* data, size_t bytes);
    void close_segment();

    Mapping*    m_;
    std::string wal_dir_;
    std::string error_;
    std::atomic<bool>      running_{false};
    std::thread            thread_;
    std::vector<LogRecord> buf_;
    int      fd_ = -1;
    uint64_t read_seq_ = 0;
    uint64_t rows_in_segment_ = 0;
    uint64_t segment_ = 0;
    uint64_t published_durable_ = 0;
    bool     failed_ = false;  // a WAL write failed: stop consuming, never retry
};

}  // namespace mdsys
