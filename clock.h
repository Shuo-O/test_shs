#pragma once

// Hot-path timestamp from a userspace cycle/tick counter -- no syscall, no
// standard-library clock wrapper. Production targets a single-socket x86-64
// server (invariant TSC via RDTSC); arm64 servers use the CNTVCT_EL0 virtual
// counter. The raw counter is stored in each record and converted to wall-clock
// nanoseconds offline using the (epoch_ns, ticks, ticks_per_sec) anchor the
// writer records in the superblock at startup.

#include <chrono>
#include <cstdint>

#if defined(__x86_64__)
#  include <x86intrin.h>
#endif

namespace mdsys {

inline uint64_t fast_ticks() {
#if defined(__x86_64__)
    return __rdtsc();
#elif defined(__aarch64__)
    uint64_t v;
    asm volatile("mrs %0, cntvct_el0" : "=r"(v));
    return v;
#else
#  error "unsupported architecture: this build targets x86-64 or arm64 servers"
#endif
}

// Frequency of fast_ticks() in ticks/second, used for offline ns conversion.
inline uint64_t fast_ticks_per_sec() {
#if defined(__aarch64__)
    uint64_t hz;
    asm volatile("mrs %0, cntfrq_el0" : "=r"(hz));
    return hz;
#elif defined(__x86_64__)
    // Calibrate RDTSC against the steady clock once at startup (not on the hot
    // path). Invariant TSC keeps this stable enough for timestamp conversion.
    using clock = std::chrono::steady_clock;
    uint64_t t0 = fast_ticks();
    auto c0 = clock::now();
    while (std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - c0).count() < 20) {
    }
    uint64_t t1 = fast_ticks();
    auto c1 = clock::now();
    double secs = std::chrono::duration<double>(c1 - c0).count();
    return static_cast<uint64_t>((t1 - t0) / secs);
#endif
}

inline uint64_t wall_epoch_ns() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
}

}  // namespace mdsys
