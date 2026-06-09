#pragma once

// Single production profile for a single-socket high-frequency server. One fixed
// set of dimensions drives the whole shared-memory ABI -- there is intentionally
// no mock/dev profile in this branch.
//
//   instruments   : 5,000 A-share securities
//   ring_capacity : 16,384  (== max query depth, power of two)
//   log_capacity  : 512Mi records (~1.79x the ~300M daily volume of headroom)
//   slot/record   : 64 B (one cache line)
//
//   rings : 5,000 * 16,384 * 64 B  = 4.88 GiB
//   log   : 512Mi   * 64 B          = 32 GiB

#include <cstdint>

namespace mdsys {

constexpr int32_t  kInstrumentCount = 5'000;
constexpr int32_t  kRingCapacity    = 16'384;          // 2^14
constexpr uint64_t kLogCapacity     = 536'870'912ULL;  // 2^29 (512Mi)
constexpr int32_t  kWalSegmentRows  = 4'000'000;       // 244 MiB WAL segments

// A-share codes fit in [0, 1,000,000); a direct array gives O(1) id->index with
// no hashing. 1e6 * 4 B = 3.8 MiB, resident in L3.
constexpr int32_t  kIdSpace = 1'000'000;

static_assert((kRingCapacity & (kRingCapacity - 1)) == 0,
              "kRingCapacity must be a power of two for mask indexing");
static_assert((kLogCapacity & (kLogCapacity - 1)) == 0,
              "kLogCapacity must be a power of two for mask indexing");

// Hot-path publication cadences. Monitoring counters and the writer heartbeat
// do not need per-tick freshness; publishing on a power-of-two stride keeps the
// extra/contended stores off the per-tick path. The day-log overwrite guard
// reads a cached durable cursor refreshed on the same stride.
constexpr uint64_t kStatsStride     = 4096;  // publish writer stats / refresh durable view
constexpr uint64_t kHeartbeatStride = 256;   // refresh liveness timestamp
static_assert((kStatsStride & (kStatsStride - 1)) == 0, "power of two");
static_assert((kHeartbeatStride & (kHeartbeatStride - 1)) == 0, "power of two");

// WAL flush batch (records buffered before write+fsync).
constexpr uint64_t kWalFlushRows = 4096;

constexpr uint64_t kMagic      = 0x4D44535953303031ULL;  // "MDSYS001"
constexpr uint32_t kAbiVersion = 1;

constexpr const char* kCtrlShmName  = "/md_ctrl_v1";
constexpr const char* kRingsShmName = "/md_rings_v1";
constexpr const char* kDayLogShmName = "/md_daylog_v1";

}  // namespace mdsys
