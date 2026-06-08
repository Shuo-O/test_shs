#pragma once

#include <cstdint>

namespace mdsys {

constexpr int32_t kInstrumentCount = 5000;
constexpr int32_t kIdArraySize = 1'000'000;
constexpr int32_t kRingCapacity = 16'384;
constexpr uint64_t kLogCapacity = 536'870'912ULL;
constexpr int32_t kWalSegmentRows = 1'000'000;

static_assert((kRingCapacity & (kRingCapacity - 1)) == 0,
              "kRingCapacity must be a power of two");

constexpr uint64_t kMagic = 0x5449434b44415441ULL;  // "TICKDATA"
constexpr uint32_t kAbiVersion = 1;
constexpr int32_t kInvalidInstrumentIndex = -1;

constexpr const char* kCtrlShmName = "/md_tick_ctrl_v1";
constexpr const char* kRingsShmName = "/md_tick_rings_v1";
constexpr const char* kDayLogShmName = "/md_tick_daylog_v1";

constexpr uint32_t kFlagValid = 1u << 0;
constexpr uint32_t kFlagInWindow = 1u << 1;

}  // namespace mdsys
