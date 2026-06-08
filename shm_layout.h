#pragma once

#include "config.h"
#include "md.h"

#include <atomic>
#include <cstdint>

namespace mdsys {

static_assert(std::atomic<uint64_t>::is_always_lock_free,
              "shared-memory uint64 atomics must be lock-free");
static_assert(sizeof(MDUniOrder) == 40, "MDUniOrder ABI changed");

// Negative values are reserved for reader-facing API errors.
enum MdError {
    kOk = 0,
    kErrUnknownInstrument = -1,
    kErrOverwriteDetected = -2,
    kErrStaleWriter = -3,
    kErrInvalidArgument = -4,
    kErrNoCapacity = -5,
};

// Control metadata is mapped by both writer and readers. committed_seq is an
// exclusive end: records in [0, committed_seq) have been published.
struct alignas(64) ShmHeader {
    uint64_t magic;
    uint32_t abi_version;
    uint32_t trading_day;
    std::atomic<uint64_t> committed_seq;     // exclusive end: [0, committed_seq)
    std::atomic<uint64_t> writer_heartbeat;  // nanoseconds from steady clock
    uint32_t instrument_count;
    uint32_t ring_capacity;
    uint64_t log_capacity;
    uint8_t _pad[16];
};

// Runtime counters intentionally live in shared memory so external processes
// can monitor ingestion, persistence, and reader health without RPC.
struct alignas(64) RuntimeStatus {
    std::atomic<uint64_t> committed_global_seq;
    std::atomic<uint64_t> written_wal_seq;
    std::atomic<uint64_t> durable_wal_seq;
    std::atomic<uint64_t> total_ticks_received;
    std::atomic<uint64_t> total_ticks_in_window;
    std::atomic<uint64_t> reader_retry_count;
    std::atomic<uint64_t> ring_overwrite_count;
    std::atomic<uint64_t> daylog_overwrite_count;
};

struct ControlSegment {
    ShmHeader header;
    // Direct indexing avoids hash-table branches and pointer chasing in the
    // hot path. Invalid instruments map to kInvalidInstrumentIndex.
    int32_t instrument_to_index[kIdArraySize];
    RuntimeStatus status;
};

// One cache-line slot in a per-symbol ring. version is a seqlock:
// odd means a writer is updating the slot, even means the payload is stable.
struct alignas(64) TickSlot {
    std::atomic<uint64_t> version;  // odd while writer is updating, even when stable
    uint64_t local_seq;
    uint64_t global_seq;
    MDUniOrder order;
};

static_assert(sizeof(TickSlot) == 64, "TickSlot must stay one cache line");

// write_seq is also an exclusive end for this symbol. The latest record is
// write_seq - 1 when write_seq > 0.
struct alignas(64) PerSymbolRingHeader {
    std::atomic<uint64_t> write_seq;  // exclusive end for this symbol
    uint32_t symbol_id;
    uint32_t _pad[13];
};

struct PerSymbolRing {
    PerSymbolRingHeader header;
    TickSlot slots[kRingCapacity];
};

struct RingSegment {
    PerSymbolRing rings[kInstrumentCount];
};

// Append-only global stream used by the storage tailer. The array is circular,
// so consumers must verify global_seq before trusting a slot.
struct alignas(64) TickRecord {
    uint64_t global_seq;
    uint64_t recv_ns;
    MDUniOrder order;
    uint32_t crc32;
    uint32_t flags;
};

static_assert(sizeof(TickRecord) == 64, "TickRecord must stay one cache line");

struct DayLogSegment {
    TickRecord records[kLogCapacity];
};

// The demo assumes recvTime is hhmmss-style integer time.
inline bool is_in_trading_window(int32_t recv_time) {
    return recv_time >= 92500 && recv_time <= 150000;
}

}  // namespace mdsys
