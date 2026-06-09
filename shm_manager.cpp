#include "shm_manager.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace mdsys {
namespace {
std::string err(const char* what, const char* name) {
    return std::string(what) + " " + name + ": " + std::strerror(errno);
}
}  // namespace

ShmManager::~ShmManager() { close(); }

void ShmManager::unlink() {
    ::shm_unlink(kCtrlShmName);
    ::shm_unlink(kRingsShmName);
    ::shm_unlink(kDayLogShmName);
}

bool ShmManager::map_one(const char* name, size_t size, bool create_it, bool read_only,
                         void** out) {
    int oflag = create_it ? (O_CREAT | O_RDWR) : (read_only ? O_RDONLY : O_RDWR);
    int fd = ::shm_open(name, oflag, 0600);
    if (fd < 0) {
        error_ = err("shm_open", name);
        return false;
    }
    if (create_it && ::ftruncate(fd, static_cast<off_t>(size)) != 0) {
        error_ = err("ftruncate", name);
        ::close(fd);
        return false;
    }
    if (!create_it) {
        // Refuse to map a segment smaller than this build expects (stale
        // segment from another config); touching it would SIGBUS, not error.
        struct stat st {};
        if (::fstat(fd, &st) != 0) {
            error_ = err("fstat", name);
            ::close(fd);
            return false;
        }
        if (st.st_size < static_cast<off_t>(size)) {
            error_ = std::string(name) + ": segment smaller than expected (stale or mismatched build)";
            ::close(fd);
            return false;
        }
    }
    int prot = read_only ? PROT_READ : (PROT_READ | PROT_WRITE);
    void* p = ::mmap(nullptr, size, prot, MAP_SHARED, fd, 0);
    int saved = errno;
    ::close(fd);  // the mapping keeps the region alive after the fd is closed
    if (p == MAP_FAILED) {
        errno = saved;
        error_ = err("mmap", name);
        return false;
    }
    *out = p;
    return true;
}

bool ShmManager::create(uint32_t trading_day) {
    close();
    unlink();  // start from a clean slate

    if (!map_one(kCtrlShmName, sizeof(ControlRegion), true, false,
                 reinterpret_cast<void**>(&map_.ctrl))) { close(); return false; }
    if (!map_one(kRingsShmName, sizeof(RingRegion), true, false,
                 reinterpret_cast<void**>(&map_.rings))) { close(); return false; }
    if (!map_one(kDayLogShmName, sizeof(LogRegion), true, false,
                 reinterpret_cast<void**>(&map_.log))) { close(); return false; }

    initialize(trading_day);
    return true;
}

bool ShmManager::open(bool read_only, bool with_log) {
    close();
    if (!map_one(kCtrlShmName, sizeof(ControlRegion), false, read_only,
                 reinterpret_cast<void**>(&map_.ctrl))) { close(); return false; }
    if (!map_one(kRingsShmName, sizeof(RingRegion), false, read_only,
                 reinterpret_cast<void**>(&map_.rings))) { close(); return false; }
    if (with_log) {
        if (!map_one(kDayLogShmName, sizeof(LogRegion), false, read_only,
                     reinterpret_cast<void**>(&map_.log))) { close(); return false; }
    }
    const Superblock& sb = map_.ctrl->sb;
    if (sb.magic.load(std::memory_order_acquire) != kMagic ||
        sb.abi_version != kAbiVersion ||
        sb.ring_capacity != static_cast<uint32_t>(kRingCapacity) ||
        (with_log && sb.log_capacity != kLogCapacity)) {
        error_ = "shared-memory ABI mismatch";
        close();
        return false;
    }
    return true;
}

void ShmManager::initialize(uint32_t trading_day) {
    // Zero only the small control region. The kernel hands back zeroed pages for
    // the multi-GiB rings/log, so faulting them all in here would be wasteful.
    std::memset(map_.ctrl, 0, sizeof(ControlRegion));
    for (int32_t i = 0; i < kIdSpace; ++i) {
        map_.ctrl->index[i].store(-1, std::memory_order_relaxed);
    }

    Superblock& sb = map_.ctrl->sb;
    sb.trading_day      = trading_day;
    sb.instrument_count.store(0, std::memory_order_relaxed);
    sb.ring_capacity    = static_cast<uint32_t>(kRingCapacity);
    sb.log_capacity     = kLogCapacity;
    sb.abi_version      = kAbiVersion;
    // Publish magic last with release so an attacher that acquire-loads it
    // observes the fully initialized superblock and index.
    sb.magic.store(kMagic, std::memory_order_release);
}

void ShmManager::close() {
    if (map_.log)   { ::munmap(map_.log, sizeof(LogRegion)); }
    if (map_.rings) { ::munmap(map_.rings, sizeof(RingRegion)); }
    if (map_.ctrl)  { ::munmap(map_.ctrl, sizeof(ControlRegion)); }
    map_ = Mapping{};
}

}  // namespace mdsys
