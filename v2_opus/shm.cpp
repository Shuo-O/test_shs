#include "shm.h"
#include "clock.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace mdv2 {
namespace {

std::string err(const char* what, const char* name) {
    return std::string(what) + " " + name + ": " + std::strerror(errno);
}

// Best-effort huge-page advice. Backed by transparent huge pages on Linux;
// a no-op where unsupported (e.g. macOS). Never fatal -- it is a perf hint.
void advise_hugepages(void* p, size_t size) {
#if defined(MADV_HUGEPAGE)
    ::madvise(p, size, MADV_HUGEPAGE);
#else
    (void)p;
    (void)size;
#endif
}

}  // namespace

Shm::~Shm() { close(); }

void Shm::unlink() {
    ::shm_unlink(kCtrlShmName);
    ::shm_unlink(kRingsShmName);
    ::shm_unlink(kLogShmName);
}

bool Shm::map_one(const char* name, size_t size, bool create_it, bool read_only, void** out) {
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
    int prot = read_only ? PROT_READ : (PROT_READ | PROT_WRITE);
    void* p = ::mmap(nullptr, size, prot, MAP_SHARED, fd, 0);
    int saved = errno;
    ::close(fd);
    if (p == MAP_FAILED) {
        errno = saved;
        error_ = err("mmap", name);
        return false;
    }
    advise_hugepages(p, size);
    *out = p;
    return true;
}

bool Shm::create(uint32_t trading_day) {
    close();
    unlink();  // start from a clean slate

    if (!map_one(kCtrlShmName, sizeof(ControlRegion), true, false,
                 reinterpret_cast<void**>(&map_.ctrl))) { close(); return false; }
    has_ctrl_ = true;
    if (!map_one(kRingsShmName, sizeof(RingRegion), true, false,
                 reinterpret_cast<void**>(&map_.rings))) { close(); return false; }
    has_rings_ = true;
    if (!map_one(kLogShmName, sizeof(LogRegion), true, false,
                 reinterpret_cast<void**>(&map_.log))) { close(); return false; }
    has_log_ = true;

    initialize(trading_day);
    return true;
}

bool Shm::open(bool read_only, bool with_log) {
    close();
    if (!map_one(kCtrlShmName, sizeof(ControlRegion), false, read_only,
                 reinterpret_cast<void**>(&map_.ctrl))) { close(); return false; }
    has_ctrl_ = true;
    if (!map_one(kRingsShmName, sizeof(RingRegion), false, read_only,
                 reinterpret_cast<void**>(&map_.rings))) { close(); return false; }
    has_rings_ = true;
    if (with_log) {
        if (!map_one(kLogShmName, sizeof(LogRegion), false, read_only,
                     reinterpret_cast<void**>(&map_.log))) { close(); return false; }
        has_log_ = true;
    }
    const Superblock& sb = map_.ctrl->sb;
    if (sb.magic != kMagic || sb.abi_version != kAbiVersion ||
        sb.ring_capacity != static_cast<uint32_t>(kRingCapacity)) {
        error_ = "shared-memory ABI mismatch";
        close();
        return false;
    }
    return true;
}

void Shm::initialize(uint32_t trading_day) {
    // Zero the small control region (NOT the multi-GiB rings/log: the kernel
    // already hands back zeroed pages, and faulting them all in here would be
    // gratuitous). Then publish the index sentinel and the superblock.
    std::memset(map_.ctrl, 0, sizeof(ControlRegion));
    for (int32_t i = 0; i < kIdSpace; ++i) {
        map_.ctrl->index[i] = -1;
    }

    Superblock& sb = map_.ctrl->sb;
    sb.trading_day      = trading_day;
    sb.instrument_count = 0;
    sb.ring_capacity    = static_cast<uint32_t>(kRingCapacity);
    sb.log_capacity     = kLogCapacity;
    sb.epoch_ns_at_start = wall_epoch_ns();
    sb.ticks_at_start    = fast_ticks();
    sb.ticks_per_sec     = fast_ticks_per_sec();
    sb.abi_version      = kAbiVersion;
    sb.magic            = kMagic;  // publish magic last
}

void Shm::close() {
    if (map_.log)   { ::munmap(map_.log, sizeof(LogRegion)); }
    if (map_.rings) { ::munmap(map_.rings, sizeof(RingRegion)); }
    if (map_.ctrl)  { ::munmap(map_.ctrl, sizeof(ControlRegion)); }
    map_ = Mapping{};
    has_ctrl_ = has_rings_ = has_log_ = false;
}

}  // namespace mdv2
