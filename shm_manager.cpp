#include "shm_manager.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace mdsys {

namespace {

std::string errno_message(const char* action, const char* name) {
    std::string msg(action);
    msg += " ";
    msg += name;
    msg += ": ";
    msg += std::strerror(errno);
    return msg;
}

}  // namespace

ShmManager::~ShmManager() {
    close();
}

void ShmManager::unlink_all() {
    ::shm_unlink(kCtrlShmName);
    ::shm_unlink(kRingsShmName);
    ::shm_unlink(kDayLogShmName);
}

bool ShmManager::create(bool reset_existing, uint32_t trading_day) {
    close();
    if (reset_existing) {
        unlink_all();
    }

    // The segments are split so readers can map only control+rings and avoid
    // mapping the large day log used by storage.
    if (!map_segment(kCtrlShmName, ctrl_size_, true, false,
                     reinterpret_cast<void**>(&ctx_.ctrl))) {
        close();
        return false;
    }
    has_ctrl_ = true;

    if (!map_segment(kRingsShmName, rings_size_, true, false,
                     reinterpret_cast<void**>(&ctx_.rings))) {
        close();
        return false;
    }
    has_rings_ = true;

    if (!map_segment(kDayLogShmName, daylog_size_, true, false,
                     reinterpret_cast<void**>(&ctx_.daylog))) {
        close();
        return false;
    }
    has_daylog_ = true;

    return initialize(trading_day);
}

bool ShmManager::open(bool include_daylog, bool read_only) {
    close();

    if (!map_segment(kCtrlShmName, ctrl_size_, false, read_only,
                     reinterpret_cast<void**>(&ctx_.ctrl))) {
        close();
        return false;
    }
    has_ctrl_ = true;

    if (!map_segment(kRingsShmName, rings_size_, false, read_only,
                     reinterpret_cast<void**>(&ctx_.rings))) {
        close();
        return false;
    }
    has_rings_ = true;

    if (include_daylog) {
        if (!map_segment(kDayLogShmName, daylog_size_, false, read_only,
                         reinterpret_cast<void**>(&ctx_.daylog))) {
            close();
            return false;
        }
        has_daylog_ = true;
    }

    if (ctx_.ctrl->header.magic != kMagic ||
        ctx_.ctrl->header.abi_version != kAbiVersion ||
        ctx_.ctrl->header.ring_capacity != static_cast<uint32_t>(kRingCapacity)) {
        last_error_ = "shared-memory ABI mismatch";
        close();
        return false;
    }

    return true;
}

void ShmManager::close() {
    if (ctx_.daylog != nullptr) {
        ::munmap(ctx_.daylog, daylog_size_);
    }
    if (ctx_.rings != nullptr) {
        ::munmap(ctx_.rings, rings_size_);
    }
    if (ctx_.ctrl != nullptr) {
        ::munmap(ctx_.ctrl, ctrl_size_);
    }
    ctx_ = ShmContext{};
    has_ctrl_ = false;
    has_rings_ = false;
    has_daylog_ = false;
}

bool ShmManager::map_segment(const char* name,
                             size_t size,
                             bool create_segment,
                             bool read_only,
                             void** out) {
    int flags = create_segment ? (O_CREAT | O_RDWR) : (read_only ? O_RDONLY : O_RDWR);
    int fd = ::shm_open(name, flags, 0600);
    if (fd < 0) {
        last_error_ = errno_message("shm_open", name);
        return false;
    }

    if (create_segment && ::ftruncate(fd, static_cast<off_t>(size)) != 0) {
        last_error_ = errno_message("ftruncate", name);
        ::close(fd);
        return false;
    }

    int prot = read_only ? PROT_READ : (PROT_READ | PROT_WRITE);
    void* mapped = ::mmap(nullptr, size, prot, MAP_SHARED, fd, 0);
    int saved_errno = errno;
    ::close(fd);
    if (mapped == MAP_FAILED) {
        errno = saved_errno;
        last_error_ = errno_message("mmap", name);
        return false;
    }

    *out = mapped;
    return true;
}

bool ShmManager::initialize(uint32_t trading_day) {
    std::memset(ctx_.ctrl, 0, ctrl_size_);
    // Fill the whole direct index before publishing the header.
    for (int32_t i = 0; i < kIdArraySize; ++i) {
        ctx_.ctrl->instrument_to_index[i] = kInvalidInstrumentIndex;
    }

    ctx_.ctrl->header.magic = kMagic;
    ctx_.ctrl->header.abi_version = kAbiVersion;
    ctx_.ctrl->header.trading_day = trading_day;
    ctx_.ctrl->header.committed_seq.store(0, std::memory_order_relaxed);
    ctx_.ctrl->header.writer_heartbeat.store(0, std::memory_order_relaxed);
    ctx_.ctrl->header.instrument_count = 0;
    ctx_.ctrl->header.ring_capacity = kRingCapacity;
    ctx_.ctrl->header.log_capacity = kLogCapacity;

    for (int32_t i = 0; i < kInstrumentCount; ++i) {
        ctx_.rings->rings[i].header.write_seq.store(0, std::memory_order_relaxed);
        ctx_.rings->rings[i].header.symbol_id = 0;
    }

    return true;
}

}  // namespace mdsys
