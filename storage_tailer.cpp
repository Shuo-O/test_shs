#include "storage_tailer.h"
#include "config.h"
#include "shm_layout.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <sstream>
#include <thread>
#include <unistd.h>
#include <utility>

namespace mdsys {
namespace {
std::string err(const char* what) { return std::string(what) + ": " + std::strerror(errno); }
}  // namespace

StorageTailer::StorageTailer(Mapping* m, std::string wal_dir)
    : m_(m), wal_dir_(std::move(wal_dir)) {
    buf_.reserve(kWalFlushRows);
}

StorageTailer::~StorageTailer() { stop(); }

bool StorageTailer::start() {
    if (m_ == nullptr || m_->ctrl == nullptr || m_->log == nullptr) {
        error_ = "tailer needs control + log mappings";
        return false;
    }
    std::error_code ec;
    std::filesystem::create_directories(wal_dir_, ec);
    if (ec) { error_ = "create_directories: " + ec.message(); return false; }
    running_.store(true, std::memory_order_release);
    thread_ = std::thread(&StorageTailer::run, this);
    return true;
}

void StorageTailer::stop() {
    running_.store(false, std::memory_order_release);
    if (thread_.joinable()) thread_.join();
    flush();          // final drain
    close_segment();
}

void StorageTailer::run() {
    auto committed = [&] {
        return m_->ctrl->writer.committed_seq.load(std::memory_order_acquire);
    };
    while (running_.load(std::memory_order_acquire) || read_seq_ < committed()) {
        uint64_t end = committed();
        if (read_seq_ >= end) {
            // Idle: flush a partial tail so durability lag stays bounded at low
            // volume, then back off briefly.
            if (!buf_.empty() && !flush()) break;
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            continue;
        }
        while (read_seq_ < end) {
            LogRecord& rec = m_->log->records[read_seq_ & (kLogCapacity - 1)];
            // If the circular log already wrapped past this slot, skip it.
            if (rec.global_seq != read_seq_) { ++read_seq_; continue; }
            if ((rec.flags & kFlagInWindow) != 0) buf_.push_back(rec);
            ++read_seq_;
            if (buf_.size() >= kWalFlushRows && !flush()) return;
        }
    }
}

bool StorageTailer::flush() {
    if (buf_.empty()) return true;
    if (fd_ < 0 && !open_segment()) return false;

    size_t off = 0;
    while (off < buf_.size()) {
        if (rows_in_segment_ >= static_cast<uint64_t>(kWalSegmentRows)) {
            close_segment();
            if (!open_segment()) return false;
        }
        uint64_t room = static_cast<uint64_t>(kWalSegmentRows) - rows_in_segment_;
        size_t rows = static_cast<size_t>(std::min<uint64_t>(room, buf_.size() - off));
        if (!write_all(buf_.data() + off, rows * sizeof(LogRecord))) return false;
        off += rows;
        rows_in_segment_ += rows;
    }
    if (::fsync(fd_) != 0) { error_ = err("fsync"); return false; }
    // Durable only after fsync: every in-window record up to read_seq_ is on disk.
    m_->ctrl->tailer.durable_seq.store(read_seq_, std::memory_order_release);
    buf_.clear();
    return true;
}

bool StorageTailer::open_segment() {
    std::ostringstream oss;
    oss << wal_dir_ << "/wal_" << m_->ctrl->sb.trading_day << "_";
    oss.width(6); oss.fill('0');
    oss << segment_++ << ".bin";
    fd_ = ::open(oss.str().c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600);
    if (fd_ < 0) { error_ = err("open wal"); return false; }
    rows_in_segment_ = 0;
    return true;
}

bool StorageTailer::write_all(const void* data, size_t bytes) {
    const char* p = static_cast<const char*>(data);
    size_t done = 0;
    while (done < bytes) {
        ssize_t n = ::write(fd_, p + done, bytes - done);
        if (n < 0) {
            if (errno == EINTR) continue;
            error_ = err("write wal");
            return false;
        }
        done += static_cast<size_t>(n);
    }
    return true;
}

void StorageTailer::close_segment() {
    if (fd_ >= 0) { ::fsync(fd_); ::close(fd_); fd_ = -1; }
}

}  // namespace mdsys
