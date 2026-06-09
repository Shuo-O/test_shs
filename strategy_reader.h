#pragma once

// Strategy-side query API. Header-only so strategy processes inline it. The
// mapping may be read-only, so this code never writes through `ctrl`; per-reader
// metrics go into a caller-owned QueryStats instead.

#include "shm_layout.h"
#include "shm_manager.h"

#include <algorithm>

namespace mdsys {

// Caller-owned, process-local reader metrics (read-only mappings cannot publish
// counters into shared memory). One instance per reader thread; plain integers.
struct QueryStats {
    uint64_t retries = 0;     // tiles/slots that needed a seqlock retry
    uint64_t overwrites = 0;  // queries aborted because the ring wrapped
};

// Per-slot seqlock read: the n==1 fast path, and the fallback when a batched
// tile is found unstable. Spins until the slot is stable, copies the payload,
// and reports an overwrite if the slot no longer holds position `pos`.
inline int read_slot_seqlock(const Ring& ring, uint64_t pos, MDUniOrder* out,
                             QueryStats* stats) {
    const Slot& slot = ring.slots[pos & (kRingCapacity - 1)];
    const uint64_t want = stable_seq_for(pos);
    bool retried = false;
    for (;;) {
        uint64_t s1 = slot.seq.load(std::memory_order_acquire);
        if (s1 != want) {
            if ((s1 & 1u) == 0) {            // stable but a newer record: wrapped
                if (stats) ++stats->overwrites;
                return kErrOverwritten;
            }
            MDSYS_RELAX();                   // odd: writer mid-update
            retried = true;
            continue;
        }
        MDUniOrder copy = slot.order;
        std::atomic_thread_fence(std::memory_order_acquire);
        uint64_t s2 = slot.seq.load(std::memory_order_relaxed);
        if (s1 == s2) {
            *out = copy;
            if (retried && stats) ++stats->retries;
            return kOk;
        }
        MDSYS_RELAX();
        retried = true;
    }
}

// Copy the latest min(n, available) records for `symbol` into out[], oldest
// first. Returns the count, or a negative QueryError. `stats` is optional.
//
// The bulk copy validates the seqlock in tiles: snapshot versions, copy
// payloads, recheck versions -- with just two acquire fences per tile instead
// of two per slot. On a weak memory model (arm64) that turns ~2n DMBs into
// ~2*ceil(n/T); on x86 (TSO) the fences are no-ops either way. The standard
// seqlock invariant per slot still holds: version-read -> (fence) -> payload ->
// (fence) -> version-recheck. A tile that is found unstable (a writer touched it
// mid-read) falls back to the per-slot path, which spins to a stable copy.
//
// Consistency comes entirely from the per-slot identity witness: a slot is
// accepted only if seq == stable_seq_for(pos) on both sides of the copy, so
// every accepted copy is bit-exact for its position and the result equals the
// ring content at the initial write_seq snapshot. No trailing re-read of
// write_seq is needed; a writer lapping positions we already copied cannot
// invalidate them, it can only make the snapshot slightly stale -- which is
// true of any snapshot the moment it returns.
inline int query_latest_n(const Mapping& m, int32_t symbol, int n,
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

    // Latest-tick poll: one slot needs no tile batching -- the per-slot read is
    // one acquire load + one fence instead of two fences.
    if (count == 1) {
        int rc = read_slot_seqlock(ring, start, out, stats);
        return rc == kOk ? 1 : rc;
    }

    constexpr int kTile = 256;  // 2 KiB version scratch; ~16 KiB working set
    uint64_t ver[kTile];

    for (int base = 0; base < count; base += kTile) {
        int tile = std::min(kTile, count - base);

        // Pass A: snapshot each slot's seqlock witness.
        for (int k = 0; k < tile; ++k) {
            uint64_t pos = start + static_cast<uint64_t>(base + k);
            ver[k] = ring.slots[pos & (kRingCapacity - 1)].seq.load(std::memory_order_relaxed);
        }
        // F1: version snapshots are ordered before the payload copies.
        std::atomic_thread_fence(std::memory_order_acquire);

        // Pass B: copy payloads.
        for (int k = 0; k < tile; ++k) {
            uint64_t pos = start + static_cast<uint64_t>(base + k);
            out[base + k] = ring.slots[pos & (kRingCapacity - 1)].order;
        }
        // F2: payload copies are ordered before the version recheck.
        std::atomic_thread_fence(std::memory_order_acquire);

        // Pass C: recheck. Stable iff the witness is unchanged, even, and still
        // names the position we wanted.
        int unstable_at = -1;
        for (int k = 0; k < tile; ++k) {
            uint64_t pos = start + static_cast<uint64_t>(base + k);
            uint64_t s2 = ring.slots[pos & (kRingCapacity - 1)].seq.load(std::memory_order_relaxed);
            uint64_t want = stable_seq_for(pos);
            if (ver[k] != s2 || (ver[k] & 1u) != 0) {  // a writer touched this slot
                unstable_at = k;
                break;
            }
            if (ver[k] != want) {                       // stable but a newer record
                if (stats) ++stats->overwrites;
                return kErrOverwritten;
            }
        }
        if (unstable_at >= 0) {
            // Rare: slots before unstable_at are already validated; re-read the
            // rest of the tile slot-by-slot, spinning each to a stable copy.
            if (stats) ++stats->retries;
            for (int k = unstable_at; k < tile; ++k) {
                int rc = read_slot_seqlock(ring, start + static_cast<uint64_t>(base + k),
                                           &out[base + k], stats);
                if (rc != kOk) return rc;
            }
        }
    }
    return count;
}

}  // namespace mdsys
