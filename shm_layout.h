#pragma once

// Shared-memory layout. Everything here is POD: fixed arrays, integer offsets,
// and lock-free atomics only -- no STL containers, no process-local pointers --
// so the region is valid across every process that maps it.
//
// Three independently mmap'd regions:
//   ControlRegion : superblock + id->index map + the two progress cursors
//   RingRegion    : per-symbol seqlock rings  (requirement A: latest-n query)
//   LogRegion     : append-only day log        (requirement B: persistence)
// Strategy processes map Control + Rings read-only and never touch the log.

#include "config.h"
#include "md.h"

#include <atomic>
#include <cstdint>

// Spin hint for seqlock retry loops (PAUSE on x86, YIELD on arm).
#if defined(__x86_64__) || defined(__i386__)
#  define MDSYS_RELAX() __asm__ __volatile__("pause" ::: "memory")
#elif defined(__aarch64__) || defined(__arm__)
#  define MDSYS_RELAX() __asm__ __volatile__("yield" ::: "memory")
#else
#  include <thread>
#  define MDSYS_RELAX() std::this_thread::yield()
#endif

namespace mdsys {

static_assert(std::atomic<uint64_t>::is_always_lock_free,
              "shared uint64 atomics must be lock-free");
static_assert(sizeof(MDUniOrder) == 40, "MDUniOrder ABI changed");

enum QueryError : int {
    kOk               = 0,
    kErrBadArg        = -1,
    kErrUnknownSymbol = -2,
    kErrOverwritten   = -3,  // ring wrapped under the reader
};

// --- Control region --------------------------------------------------------
// The two cursors live on their own cache lines: committed_seq is written by the
// ingest thread, durable_seq by the tailer thread, so separating them avoids
// false sharing between the two cores.

// Static metadata, written once at init then read-only.
struct alignas(64) Superblock {
    uint64_t magic;
    uint32_t abi_version;
    uint32_t trading_day;       // YYYYMMDD
    uint32_t instrument_count;  // registered symbols
    uint32_t ring_capacity;
    uint64_t log_capacity;
    uint8_t  _pad[32];
};
static_assert(sizeof(Superblock) == 64, "Superblock must be one cache line");

// committed_seq is the exclusive end of the day log: records [0, committed_seq)
// are fully written and visible to the tailer.
struct alignas(64) WriterCursor {
    std::atomic<uint64_t> committed_seq;
    uint8_t _pad[56];
};
static_assert(sizeof(WriterCursor) == 64, "WriterCursor must be one cache line");

// durable_seq is how far the tailer has fsynced to the WAL.
struct alignas(64) TailerCursor {
    std::atomic<uint64_t> durable_seq;
    uint8_t _pad[56];
};
static_assert(sizeof(TailerCursor) == 64, "TailerCursor must be one cache line");

struct ControlRegion {
    Superblock   sb;
    WriterCursor writer;
    TailerCursor tailer;
    // Direct id->index map. -1 == unregistered. Sized to the full code space.
    int32_t      index[kIdSpace];
};

// --- Per-symbol ring (seqlock) ---------------------------------------------

// One cache line per slot. `seq` is BOTH the seqlock witness and the slot's
// record identity: while a writer fills ring position p, seq = 2*(p+1)-1 (odd);
// once the payload is stable, seq = 2*(p+1) (even). Because the even value
// encodes the position, a reader can tell whether the slot still holds the
// record it asked for, even after the ring wraps -- no extra sequence field.
struct alignas(64) Slot {
    std::atomic<uint64_t> seq;
    MDUniOrder            order;
    uint8_t               _pad[16];
};
static_assert(sizeof(Slot) == 64, "Slot must be one cache line");

struct alignas(64) RingHead {
    std::atomic<uint64_t> write_seq;  // exclusive end; latest record is write_seq-1
    uint32_t              symbol_id;
    uint8_t               _pad[52];
};
static_assert(sizeof(RingHead) == 64, "RingHead must be one cache line");

struct Ring {
    RingHead head;
    Slot     slots[kRingCapacity];
};

struct RingRegion {
    Ring rings[kInstrumentCount];
};

// --- Append-only day log / WAL record --------------------------------------

struct alignas(64) LogRecord {
    uint64_t   global_seq;
    MDUniOrder order;
    uint32_t   flags;
    uint8_t    _pad[12];
};
static_assert(sizeof(LogRecord) == 64, "LogRecord must be one cache line");

struct LogRegion {
    LogRecord records[kLogCapacity];
};

constexpr uint32_t kFlagValid    = 1u << 0;
constexpr uint32_t kFlagInWindow = 1u << 1;

// 09:25:00 .. 15:00:00 inclusive, on the hhmmss-style recvTime field.
inline bool in_trading_window(int32_t recv_time) {
    return recv_time >= 92500 && recv_time <= 150000;
}

// Even seqlock witness value for ring position p.
inline uint64_t stable_seq_for(uint64_t position) { return (position + 1) << 1; }

}  // namespace mdsys
