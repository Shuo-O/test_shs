#pragma once

// Shared-memory layout. Everything here is POD: fixed arrays, integer offsets,
// and lock-free atomics only -- no STL containers, no process-local pointers --
// so the region is valid across every process that maps it.
//
// Three independently mmap'd regions:
//   ControlRegion : superblock + id index + writer/tailer stat lines  (~3.8 MiB)
//   RingRegion    : per-symbol seqlock rings for latest-n queries     (4.88 GiB)
//   LogRegion     : append-only day log consumed by the storage tailer (32 GiB)
// Strategy processes map Control + Rings read-only and never touch the log.

#include "config.h"
#include "md.h"

#include <atomic>
#include <cstdint>

// Portable spin hint for seqlock retry loops (PAUSE / YIELD).
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

// --- Control region, cache-line partitioned by access pattern --------------

// Line 0: static metadata, written once at init then read-only. Carries the
// clock anchor used to convert raw recv_ticks to wall-clock ns offline.
struct alignas(64) Superblock {
    uint64_t magic;
    uint32_t abi_version;
    uint32_t trading_day;        // YYYYMMDD
    uint32_t instrument_count;   // registered symbols
    uint32_t ring_capacity;
    uint64_t log_capacity;
    uint64_t epoch_ns_at_start;  // wall-clock anchor (ns)
    uint64_t ticks_at_start;     // fast_ticks() anchor
    uint64_t ticks_per_sec;      // fast clock frequency
    uint8_t  _pad[8];
};
static_assert(sizeof(Superblock) == 64, "Superblock must be one cache line");

// Line 1: the writer's hot publication. committed_seq is the canonical exclusive
// end of the day log; records in [0, committed_seq) are visible to the tailer.
struct alignas(64) WriterControl {
    std::atomic<uint64_t> committed_seq;
    std::atomic<uint64_t> heartbeat_ticks;
    uint8_t _pad[48];
};
static_assert(sizeof(WriterControl) == 64, "WriterControl must be one cache line");

// Line 2: writer-only monitoring counters, published on a stride so the hot path
// keeps this line in M state (never shared with the tailer or readers).
struct alignas(64) WriterStats {
    std::atomic<uint64_t> ticks_received;
    std::atomic<uint64_t> ticks_in_window;
    std::atomic<uint64_t> ticks_dropped;   // unknown / unregistered symbol
    uint8_t _pad[40];
};
static_assert(sizeof(WriterStats) == 64, "WriterStats must be one cache line");

// Line 3: tailer-owned progress. read_seq is the day-log consume cursor;
// durable_seq advances only after fsync and is what the writer's overwrite guard
// trusts. wal_faults is non-zero iff a WAL write/fsync has failed (fail-closed).
struct alignas(64) TailerStats {
    std::atomic<uint64_t> read_seq;
    std::atomic<uint64_t> durable_seq;
    std::atomic<uint64_t> log_overwrite_count;
    std::atomic<uint64_t> wal_faults;
    uint8_t _pad[32];
};
static_assert(sizeof(TailerStats) == 64, "TailerStats must be one cache line");

struct ControlRegion {
    Superblock    sb;
    WriterControl wc;
    WriterStats   wstats;
    TailerStats   tstats;
    // Direct id->index map. -1 == unregistered. Sized to the full code space.
    int32_t       index[kIdSpace];
};

// --- Per-symbol ring (seqlock) ---------------------------------------------

// One cache line per slot. `seq` is the seqlock witness AND the slot's record
// identity: while a writer fills position p it holds 2*(p+1)-1 (odd); once stable
// it holds 2*(p+1) (even). The even value encodes the position, so a reader can
// tell whether the slot still holds the record it asked for after a wrap.
struct alignas(64) Slot {
    std::atomic<uint64_t> seq;
    uint64_t              global_seq;
    MDUniOrder            order;
    uint8_t               _pad[8];
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
    uint64_t   recv_ticks;  // raw fast_ticks(); convert to ns via the anchor
    MDUniOrder order;
    uint32_t   crc32;
    uint32_t   flags;
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
