#pragma once

// Strategy-side query API. Header-only so strategy processes inline it. The
// mapping may be read-only, so this code never writes through `ctrl`; per-reader
// metrics go into a caller-owned QueryStats instead.

#include "layout.h"
#include "shm.h"

#include <algorithm>

namespace mdv2 {

// Caller-owned, process-local reader metrics (read-only mappings cannot publish
// counters into shared memory). One instance per reader thread; plain integers.
struct QueryStats {
    uint64_t retries = 0;     // slots that needed at least one seqlock retry
    uint64_t overwrites = 0;  // queries aborted because the ring wrapped
};

// Copy the latest min(n, available) records for `symbol` into out[], oldest
// first. Returns the count, or a negative QueryError. `stats` is optional.
inline int query_latest(const Mapping& m, int32_t symbol, int n,
                        MDUniOrder* out, QueryStats* stats = nullptr) {
    if (m.ctrl == nullptr || m.rings == nullptr || out == nullptr || n <= 0) {
        return kErrBadArg;
    }
    if (symbol < 0 || symbol >= kIdSpace) {
        return kErrUnknownSymbol;
    }
    int32_t idx = m.ctrl->index[symbol];
    if (idx < 0 || idx >= kInstrumentCount) {
        return kErrUnknownSymbol;
    }

    const Ring& ring = m.rings->rings[idx];
    uint64_t write_seq = ring.head.write_seq.load(std::memory_order_acquire);
    uint64_t avail = std::min<uint64_t>(write_seq, kRingCapacity);
    int count = static_cast<int>(std::min<uint64_t>(avail, static_cast<uint64_t>(n)));
    uint64_t start = write_seq - static_cast<uint64_t>(count);

    for (int i = 0; i < count; ++i) {
        uint64_t pos = start + static_cast<uint64_t>(i);
        const Slot& slot = ring.slots[pos & (kRingCapacity - 1)];
        uint64_t want = stable_seq_for(pos);  // expected even witness for `pos`
        bool retried = false;

        for (;;) {
            uint64_t s1 = slot.seq.load(std::memory_order_acquire);
            if (s1 != want) {
                // Odd  -> a writer is mid-update; spin and retry.
                // Even -> the slot now holds a different (newer) record, i.e.
                //         the ring wrapped past the position we wanted.
                if ((s1 & 1u) == 0) {
                    if (stats) ++stats->overwrites;
                    return kErrOverwritten;
                }
                MDV2_RELAX();
                retried = true;
                continue;
            }
            MDUniOrder copy = slot.order;
            // Pair the payload load with the witness recheck: on weak models the
            // acquire fence stops the load from sinking past the second read.
            std::atomic_thread_fence(std::memory_order_acquire);
            uint64_t s2 = slot.seq.load(std::memory_order_relaxed);
            if (s1 == s2) {
                out[i] = copy;
                break;
            }
            MDV2_RELAX();
            retried = true;
        }
        if (retried && stats) ++stats->retries;
    }

    // Final wrap guard: a writer can lap the reader during the copy loop.
    uint64_t after = ring.head.write_seq.load(std::memory_order_acquire);
    if (after - start > static_cast<uint64_t>(kRingCapacity)) {
        if (stats) ++stats->overwrites;
        return kErrOverwritten;
    }
    return count;
}

}  // namespace mdv2
