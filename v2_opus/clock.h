#pragma once

// Fast monotonic timestamp for the hot path. The ingest path must not call into
// the kernel, so we read a userspace cycle/tick counter:
//   x86-64        : RDTSC (invariant TSC on server parts)
//   macOS (arm64) : mach_absolute_time() -- a commpage read, no syscall
//   Linux arm64   : CNTVCT_EL0 virtual counter
//   fallback      : std::chrono::steady_clock (portable, slower)
//
// The counter is stored raw in each record; ticks are converted to wall-clock
// nanoseconds offline using the (epoch_ns, ticks, ticks_per_sec) anchor that the
// writer records in the superblock at startup. Keeping conversion off the hot
// path is the whole point.

#include <cstdint>
#include <chrono>

#if defined(__APPLE__)
#  include <mach/mach_time.h>
#elif defined(__x86_64__)
#  include <x86intrin.h>
#endif

namespace mdv2 {

inline uint64_t fast_ticks() {
#if defined(MDV2_FORCE_STEADY)
    // Portable fallback / A-B test knob: use the standard clock even where a
    // faster counter exists. Useful for attributing hot-path cost.
    return static_cast<uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
#elif defined(__APPLE__)
    return mach_absolute_time();
#elif defined(__x86_64__)
    return __rdtsc();
#elif defined(__aarch64__)
    uint64_t v;
    asm volatile("mrs %0, cntvct_el0" : "=r"(v));
    return v;
#else
    return static_cast<uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
#endif
}

// Frequency of fast_ticks() in ticks/second, used to convert to nanoseconds.
inline uint64_t fast_ticks_per_sec() {
#if defined(__APPLE__)
    mach_timebase_info_data_t tb;
    mach_timebase_info(&tb);
    // mach_absolute_time() * numer/denom == nanoseconds, so
    // ticks_per_sec = 1e9 * denom / numer.
    return static_cast<uint64_t>(1'000'000'000.0 * tb.denom / tb.numer);
#elif defined(__aarch64__) && !defined(__APPLE__)
    uint64_t hz;
    asm volatile("mrs %0, cntfrq_el0" : "=r"(hz));
    return hz;
#elif defined(__x86_64__)
    // Calibrate RDTSC against steady_clock once. Invariant TSC makes this stable
    // enough for timestamp conversion (not used on the hot path).
    using clock = std::chrono::steady_clock;
    uint64_t t0 = fast_ticks();
    auto c0 = clock::now();
    while (std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - c0).count() < 20) {
    }
    uint64_t t1 = fast_ticks();
    auto c1 = clock::now();
    double secs = std::chrono::duration<double>(c1 - c0).count();
    return static_cast<uint64_t>((t1 - t0) / secs);
#else
    return 1'000'000'000ULL;  // steady_clock fallback already counts ns-ish
#endif
}

inline uint64_t wall_epoch_ns() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
}

}  // namespace mdv2
