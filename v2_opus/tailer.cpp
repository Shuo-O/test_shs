#include "tailer.h"
#include "config.h"
#include "layout.h"

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

namespace mdv2 {
namespace {
std::string err(const char* what) { return std::string(what) + ": " + std::strerror(errno); }
}  // namespace

Tailer::Tailer(Mapping* m, std::string wal_dir)
    : m_(m), wal_dir_(std::move(wal_dir)) {
    buf_.reserve(kWalFlushRows);
}

Tailer::~Tailer() { stop(); }

bool Tailer::start() {
    if (m_ == nullptr || m_->ctrl == nullptr || m_->log == nullptr) {
        error_ = "tailer needs control + log mappings";
        return false;
    }
    std::error_code ec;
    std::filesystem::create_directories(wal_dir_, ec);
    if (ec) { error_ = "create_directories: " + ec.message(); return false; }
    running_.store(true, std::memory_order_release);
    thread_ = std::thread(&Tailer::run, this);
    return true;
}

void Tailer::stop() {
    running_.store(false, std::memory_order_release);
    if (thread_.joinable()) thread_.join();
    flush();
    close_segment();
}

void Tailer::run() {
    auto committed = [&] {
        return m_->ctrl->wc.committed_seq.load(std::memory_order_acquire);
    };
    while (running_.load(std::memory_order_acquire) || read_seq_ < committed()) {
        uint64_t end = committed();
        if (read_seq_ >= end) {
            // Idle: flush a partial tail so durability lag stays bounded when the
            // batch threshold is never reached at low volume.
            if (!buf_.empty()) flush();
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            continue;
        }
        while (read_seq_ < end) {
            LogRecord& rec = m_->log->records[read_seq_ & (kLogCapacity - 1)];
            if (rec.global_seq != read_seq_) {
                // The circular slot was overwritten before we read it.
                m_->ctrl->tstats.log_overwrite_count.fetch_add(1, std::memory_order_relaxed);
                ++read_seq_;
                continue;
            }
            if ((rec.flags & kFlagInWindow) != 0) buf_.push_back(rec);
            ++read_seq_;
            if (buf_.size() >= kWalFlushRows) flush();
        }
        // Read progress (monitoring); durability advances only in flush().
        m_->ctrl->tstats.read_seq.store(read_seq_, std::memory_order_release);
    }
}

bool Tailer::flush() {
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
    // Durable: every in-window record up to read_seq_ is now on disk. The
    // writer's overwrite guard trusts this cursor.
    m_->ctrl->tstats.durable_seq.store(read_seq_, std::memory_order_release);
    buf_.clear();
    return true;
}

bool Tailer::open_segment() {
    std::ostringstream oss;
    oss << wal_dir_ << "/wal_" << m_->ctrl->sb.trading_day << "_";
    oss.width(6); oss.fill('0');
    oss << segment_++ << ".bin";
    fd_ = ::open(oss.str().c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600);
    if (fd_ < 0) { error_ = err("open wal"); return false; }
    rows_in_segment_ = 0;
    return true;
}

bool Tailer::write_all(const void* data, size_t bytes) {
    const char* p = static_cast<const char*>(data);
    size_t done = 0;
    while (done < bytes) {
        ssize_t n = ::write(fd_, p + done, bytes - done);
        if (n < 0) {
            if (errno == EINTR) continue;
            error_ = err("write wal");
            return false;
        }
        if (n == 0) { error_ = "write wal returned zero"; return false; }
        done += static_cast<size_t>(n);
    }
    return true;
}

void Tailer::close_segment() {
    if (fd_ >= 0) { ::fsync(fd_); ::close(fd_); fd_ = -1; }
}

}  // namespace mdv2
