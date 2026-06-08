#pragma once

#include "shm_manager.h"

#include <atomic>
#include <string>
#include <thread>
#include <vector>

namespace mdsys {

// Consumes the day log by increasing global_seq, writes in-window records to an
// append-only WAL in large sequential batches, and advances durable_seq only
// after fsync. Never blocks the writer. Fails closed: a WAL write/fsync error
// stops the loop and freezes durable_seq (the writer's overwrite guard then
// trips) rather than silently advancing persistence state.
class StorageTailer {
public:
    StorageTailer(Mapping* m, std::string wal_dir);
    ~StorageTailer();
    StorageTailer(const StorageTailer&) = delete;
    StorageTailer& operator=(const StorageTailer&) = delete;

    bool start();
    void stop();
    const std::string& error() const { return error_; }
    bool faulted() const { return faulted_.load(std::memory_order_acquire); }

private:
    void run();
    bool flush();
    bool open_segment();
    bool write_all(const void* data, size_t bytes);
    bool close_segment();
    void record_fault();  // mark fail-closed and publish to shared memory

    Mapping*    m_;
    std::string wal_dir_;
    std::string error_;
    std::atomic<bool>      running_{false};
    std::atomic<bool>      faulted_{false};
    std::thread            thread_;
    std::vector<LogRecord> buf_;
    int      fd_ = -1;
    uint64_t read_seq_ = 0;
    uint64_t rows_in_segment_ = 0;
    uint64_t segment_ = 0;
};

}  // namespace mdsys
