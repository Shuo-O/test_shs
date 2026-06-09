#pragma once

// Compile-time sizing for the v2 market-data store. One translation-unit-wide
// set of constants drives every shared-memory dimension so the layout is fixed
// and ABI-stable. Select a profile with -DMDV2_STANDARD / -DMDV2_EXTENDED;
// the default is the laptop-friendly mock profile.
//
// Derivation of the production numbers (see docs/DESIGN_SPEC.md):
//   instruments     : ~5,000 A-share securities
//   daily ticks      : ~300,000,000
//   ring_capacity    : 16,384 == recommended max query depth (power of two)
//   log_capacity     : 512Mi records == 1.79x daily volume of headroom
//   slot/record size : 64 B (one cache line)

#include <cstdint>

namespace mdv2 {

#if defined(MDV2_STANDARD)
constexpr int32_t  kInstrumentCount = 5'000;
constexpr int32_t  kRingCapacity    = 16'384;        // 2^14
constexpr uint64_t kLogCapacity     = 536'870'912ULL;// 2^29 (512Mi)
constexpr int32_t  kWalSegmentRows  = 4'000'000;
#elif defined(MDV2_EXTENDED)
constexpr int32_t  kInstrumentCount = 5'000;
constexpr int32_t  kRingCapacity    = 65'536;        // 2^16
constexpr uint64_t kLogCapacity     = 536'870'912ULL;
constexpr int32_t  kWalSegmentRows  = 16'000'000;
#else
#define MDV2_MOCK 1
constexpr int32_t  kInstrumentCount = 256;
constexpr int32_t  kRingCapacity    = 1'024;         // 2^10
constexpr uint64_t kLogCapacity     = 1'048'576ULL;  // 2^20
constexpr int32_t  kWalSegmentRows  = 4'096;
#endif

// A-share codes fit in [0, 1,000,000); a direct array makes id->index an O(1)
// load with no hashing. 1e6 * 4 B = 3.8 MiB, which stays resident in L3.
constexpr int32_t kIdSpace = 1'000'000;

static_assert((kRingCapacity & (kRingCapacity - 1)) == 0,
              "kRingCapacity must be a power of two for mask indexing");
static_assert((kLogCapacity & (kLogCapacity - 1)) == 0,
              "kLogCapacity must be a power of two for mask indexing");

// Hot-path publication cadences. Monitoring counters and the writer heartbeat
// do not need per-tick freshness; publishing them on a power-of-two stride
// keeps the contended/extra stores off the per-tick path. The day-log overwrite
// guard reads a cached durable cursor refreshed on the same stride.
constexpr uint64_t kStatsStride     = 4096;  // publish writer stats / refresh durable view
constexpr uint64_t kHeartbeatStride = 256;   // refresh liveness timestamp
static_assert((kStatsStride & (kStatsStride - 1)) == 0, "power of two");
static_assert((kHeartbeatStride & (kHeartbeatStride - 1)) == 0, "power of two");

// WAL flush batch (records buffered before write+fsync).
constexpr uint64_t kWalFlushRows = 4096;

constexpr uint64_t kMagic      = 0x4D44563200000001ULL;  // "MDV2" + version
constexpr uint32_t kAbiVersion = 1;

constexpr const char* kCtrlShmName  = "/test_shs_v2_ctrl";
constexpr const char* kRingsShmName = "/test_shs_v2_rings";
constexpr const char* kLogShmName   = "/test_shs_v2_daylog";

}  // namespace mdv2
