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
    // Best-effort final drain. If it fails, the fault is already recorded.
    if (!faulted()) {
        if (!flush()) record_fault();
        else publish_durable();  // fully drained: cursors converge
    }
    if (!close_segment() && !faulted()) record_fault();
}

void StorageTailer::publish_durable() {
    if (published_durable_ != read_seq_) {
        m_->ctrl->tstats.durable_seq.store(read_seq_, std::memory_order_release);
        published_durable_ = read_seq_;
    }
}

void StorageTailer::record_fault() {
    faulted_.store(true, std::memory_order_release);
    if (m_ && m_->ctrl) {
        m_->ctrl->tstats.wal_faults.fetch_add(1, std::memory_order_relaxed);
    }
}

void StorageTailer::run() {
    auto committed = [&] {
        return m_->ctrl->wc.committed_seq.load(std::memory_order_acquire);
    };
    while (running_.load(std::memory_order_acquire) || read_seq_ < committed()) {
        if (faulted()) break;  // fail closed: stop consuming once WAL IO failed

        uint64_t end = committed();
        if (read_seq_ >= end) {
            if (!buf_.empty()) {
                // Idle: flush a partial tail so durability lag stays bounded
                // when the batch threshold is never reached at low volume.
                if (!flush()) { record_fault(); break; }
            } else {
                // Fully drained with nothing buffered: every in-window record
                // up to read_seq_ is on disk. Keep the durable cursor honest --
                // otherwise off-window traffic shows phantom lag and trips the
                // writer's overwrite guard for data that needs no persistence.
                publish_durable();
            }
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            continue;
        }
        while (read_seq_ < end) {
            const LogRecord& rec = m_->log->records[read_seq_ & (kLogCapacity - 1)];
            if (rec.global_seq != read_seq_) {
                // The circular slot was overwritten before we read it.
                m_->ctrl->tstats.log_overwrite_count.fetch_add(1, std::memory_order_relaxed);
                ++read_seq_;
                continue;
            }
            LogRecord copy;
            std::memcpy(&copy, &rec, sizeof copy);
            // Recheck identity after the copy: if the writer lapped us mid-copy
            // the slot names a different record now, so drop the torn copy. A
            // lap is unreachable by design (the log holds ~1.8x a day), but
            // this keeps the unreachable path from persisting garbage.
            std::atomic_thread_fence(std::memory_order_acquire);
            if (rec.global_seq != read_seq_) {
                m_->ctrl->tstats.log_overwrite_count.fetch_add(1, std::memory_order_relaxed);
                ++read_seq_;
                continue;
            }
            if ((copy.flags & kFlagInWindow) != 0) buf_.push_back(copy);
            ++read_seq_;
            if (buf_.size() >= kWalFlushRows) {
                if (!flush()) { record_fault(); break; }
            }
        }
        if (faulted()) break;
        // Read progress (monitoring); durability advances only in flush().
        m_->ctrl->tstats.read_seq.store(read_seq_, std::memory_order_release);
    }
}

bool StorageTailer::flush() {
    if (buf_.empty()) return true;
    if (fd_ < 0 && !open_segment()) return false;

    size_t off = 0;
    while (off < buf_.size()) {
        if (rows_in_segment_ >= static_cast<uint64_t>(kWalSegmentRows)) {
            if (!close_segment()) return false;
            if (!open_segment()) return false;
        }
        uint64_t room = static_cast<uint64_t>(kWalSegmentRows) - rows_in_segment_;
        size_t rows = static_cast<size_t>(std::min<uint64_t>(room, buf_.size() - off));
        if (!write_all(buf_.data() + off, rows * sizeof(LogRecord))) return false;
        off += rows;
        rows_in_segment_ += rows;
    }
    if (::fsync(fd_) != 0) { error_ = err("fsync"); return false; }
    // Durable: every in-window record up to read_seq_ is now on disk. Advance
    // the cursor the writer's overwrite guard trusts -- only after fsync.
    m_->ctrl->tstats.durable_seq.store(read_seq_, std::memory_order_release);
    published_durable_ = read_seq_;
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
        if (n == 0) { error_ = "write wal returned zero"; return false; }
        done += static_cast<size_t>(n);
    }
    return true;
}

bool StorageTailer::close_segment() {
    if (fd_ < 0) return true;
    bool ok = true;
    if (::fsync(fd_) != 0) { error_ = err("fsync on close"); ok = false; }
    if (::close(fd_) != 0) { error_ = err("close wal"); ok = false; }
    fd_ = -1;
    return ok;
}

}  // namespace mdsys
