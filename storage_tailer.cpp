#include "storage_tailer.h"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <sstream>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

namespace mdsys {

namespace {

std::string errno_text(const char* action) {
    std::string msg(action);
    msg += ": ";
    msg += std::strerror(errno);
    return msg;
}

}  // namespace

StorageTailer::StorageTailer(ShmContext* ctx, std::string wal_dir)
    : ctx_(ctx), wal_dir_(std::move(wal_dir)) {
    buffer_.reserve(4096);
}

StorageTailer::~StorageTailer() {
    stop();
}

bool StorageTailer::start() {
    if (ctx_ == nullptr || ctx_->ctrl == nullptr || ctx_->daylog == nullptr) {
        last_error_ = "StorageTailer requires control and daylog mappings";
        return false;
    }
    std::error_code ec;
    std::filesystem::create_directories(wal_dir_, ec);
    if (ec) {
        last_error_ = "create_directories: " + ec.message();
        return false;
    }

    running_.store(true, std::memory_order_release);
    thread_ = std::thread(&StorageTailer::run, this);
    return true;
}

void StorageTailer::stop() {
    running_.store(false, std::memory_order_release);
    if (thread_.joinable()) {
        thread_.join();
    }
    flush_buffer();
    close_file();
}

void StorageTailer::run() {
    while (running_.load(std::memory_order_acquire) ||
           next_read_seq_ < ctx_->ctrl->header.committed_seq.load(std::memory_order_acquire)) {
        uint64_t committed = ctx_->ctrl->header.committed_seq.load(std::memory_order_acquire);
        if (next_read_seq_ >= committed) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            continue;
        }

        uint64_t durable = ctx_->ctrl->status.durable_wal_seq.load(std::memory_order_relaxed);
        if (committed - durable > kLogCapacity) {
            ctx_->ctrl->status.daylog_overwrite_count.fetch_add(1, std::memory_order_relaxed);
        }

        while (next_read_seq_ < committed) {
            TickRecord& rec = ctx_->daylog->records[next_read_seq_ % kLogCapacity];
            if (rec.global_seq != next_read_seq_) {
                ctx_->ctrl->status.daylog_overwrite_count.fetch_add(1, std::memory_order_relaxed);
                ++next_read_seq_;
                continue;
            }

            if ((rec.flags & kFlagInWindow) != 0) {
                buffer_.push_back(rec);
                if (buffer_.size() >= 4096) {
                    flush_buffer();
                }
            }
            ++next_read_seq_;
            ctx_->ctrl->status.written_wal_seq.store(next_read_seq_, std::memory_order_release);
        }
    }
}

bool StorageTailer::flush_buffer() {
    if (buffer_.empty()) {
        return true;
    }
    if (fd_ < 0 && !open_next_file()) {
        return false;
    }

    size_t offset = 0;
    while (offset < buffer_.size()) {
        if (rows_in_segment_ >= static_cast<uint64_t>(kWalSegmentRows)) {
            close_file();
            if (!open_next_file()) {
                return false;
            }
        }

        uint64_t room = static_cast<uint64_t>(kWalSegmentRows) - rows_in_segment_;
        size_t rows = static_cast<size_t>(
            std::min<uint64_t>(room, static_cast<uint64_t>(buffer_.size() - offset)));
        size_t bytes = rows * sizeof(TickRecord);
        if (!write_all(buffer_.data() + offset, bytes)) {
            return false;
        }
        offset += rows;
        rows_in_segment_ += rows;
    }

    if (::fsync(fd_) != 0) {
        last_error_ = errno_text("fsync");
        return false;
    }
    ctx_->ctrl->status.durable_wal_seq.store(next_read_seq_, std::memory_order_release);
    buffer_.clear();
    return true;
}

bool StorageTailer::open_next_file() {
    std::ostringstream oss;
    oss << wal_dir_ << "/wal_" << ctx_->ctrl->header.trading_day << "_";
    oss.width(6);
    oss.fill('0');
    oss << segment_index_++ << ".bin";

    fd_ = ::open(oss.str().c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600);
    if (fd_ < 0) {
        last_error_ = errno_text("open wal");
        return false;
    }
    rows_in_segment_ = 0;
    return true;
}

bool StorageTailer::write_all(const void* data, size_t size) {
    const char* ptr = static_cast<const char*>(data);
    size_t written = 0;
    while (written < size) {
        ssize_t n = ::write(fd_, ptr + written, size - written);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            last_error_ = errno_text("write wal");
            return false;
        }
        if (n == 0) {
            last_error_ = "write wal returned zero";
            return false;
        }
        written += static_cast<size_t>(n);
    }
    return true;
}

void StorageTailer::close_file() {
    if (fd_ >= 0) {
        ::fsync(fd_);
        ::close(fd_);
        fd_ = -1;
    }
}

}  // namespace mdsys
