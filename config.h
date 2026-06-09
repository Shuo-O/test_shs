#pragma once

// Compile-time dimensions of the shared-memory layout. One fixed set of
// constants makes the layout a stable ABI across every process that maps it.
//
// Default = small, laptop-runnable sizes (this is a teaching/core build, not a
// production deployment). Define -DPROD for the real single-server profile.
//
//   field            core (default)     production (-DPROD)
//   instruments      256                5,000   (A-share universe)
//   ring_capacity    1,024              16,384  (== max query depth)
//   log_capacity     1Mi records        512Mi   (~1.79x daily volume)
//   => rings          16 MiB             4.88 GiB
//   => day log        64 MiB             32 GiB

#include <cstdint>

namespace mdsys {

#if defined(PROD)
constexpr int32_t  kInstrumentCount = 5'000;
constexpr int32_t  kRingCapacity    = 16'384;          // 2^14
constexpr uint64_t kLogCapacity     = 536'870'912ULL;  // 2^29 (512Mi)
constexpr int32_t  kWalSegmentRows  = 4'000'000;       // 244 MiB WAL segments
#else
constexpr int32_t  kInstrumentCount = 256;
constexpr int32_t  kRingCapacity    = 1'024;           // 2^10
constexpr uint64_t kLogCapacity     = 1'048'576ULL;    // 2^20 (1Mi)
constexpr int32_t  kWalSegmentRows  = 65'536;
#endif

// A-share codes fit in [0, 1,000,000); a direct array gives O(1) id->index with
// no hashing. 1e6 * 4 B = 3.8 MiB, resident in L3.
constexpr int32_t  kIdSpace = 1'000'000;

// Records buffered in the tailer before one write()+fsync() to the WAL.
constexpr uint64_t kWalFlushRows = 4096;

static_assert((kRingCapacity & (kRingCapacity - 1)) == 0,
              "kRingCapacity must be a power of two for mask indexing");
static_assert((kLogCapacity & (kLogCapacity - 1)) == 0,
              "kLogCapacity must be a power of two for mask indexing");

constexpr uint64_t kMagic      = 0x4D44535953ULL;  // "MDSYS"
constexpr uint32_t kAbiVersion = 1;

constexpr const char* kCtrlShmName   = "/md_ctrl";
constexpr const char* kRingsShmName  = "/md_rings";
constexpr const char* kDayLogShmName = "/md_daylog";

}  // namespace mdsys
