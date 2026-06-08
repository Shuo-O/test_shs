#pragma once

#include "shm.h"

#include <atomic>
#include <string>
#include <thread>
#include <vector>

namespace mdv2 {

// Consumes the day log by increasing global_seq, writes in-window records to an
// append-only WAL in large sequential batches, and advances durable_seq only
// after fsync. Never blocks the writer.
class Tailer {
public:
    Tailer(Mapping* m, std::string wal_dir);
    ~Tailer();
    Tailer(const Tailer&) = delete;
    Tailer& operator=(const Tailer&) = delete;

    bool start();
    void stop();
    const std::string& error() const { return error_; }

private:
    void run();
    bool flush();
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
};

}  // namespace mdv2
