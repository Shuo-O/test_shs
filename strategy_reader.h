#pragma once

#include "shm_layout.h"
#include "shm_manager.h"

#include <algorithm>

namespace mdsys {

inline int query_latest_n(const ShmContext* ctx,
                          int32_t instrument_id,
                          int n,
                          MDUniOrder* out_buf,
                          int* out_count) {
    if (out_count != nullptr) {
        *out_count = 0;
    }
    if (ctx == nullptr || ctx->ctrl == nullptr || ctx->rings == nullptr ||
        out_buf == nullptr || out_count == nullptr || n <= 0) {
        return kErrInvalidArgument;
    }
    if (instrument_id < 0 || instrument_id >= kIdArraySize) {
        return kErrUnknownInstrument;
    }

    int32_t idx = ctx->ctrl->instrument_to_index[instrument_id];
    if (idx < 0 || idx >= kInstrumentCount) {
        return kErrUnknownInstrument;
    }

    PerSymbolRing& ring = ctx->rings->rings[idx];
    uint64_t write_seq = ring.header.write_seq.load(std::memory_order_acquire);
    int available = static_cast<int>(std::min<uint64_t>(
        write_seq, static_cast<uint64_t>(kRingCapacity)));
    int actual = std::min(n, available);
    uint64_t start_seq = write_seq - static_cast<uint64_t>(actual);

    for (int i = 0; i < actual; ++i) {
        uint64_t expected_seq = start_seq + static_cast<uint64_t>(i);
        TickSlot& slot = ring.slots[expected_seq & (kRingCapacity - 1)];

        // Seqlock read loop.  v1 acquire pairs with the writer's even release,
        // ensuring all payload stores are visible once we see a stable version.
        // The explicit acquire fence before re-reading v2 prevents the compiler
        // or CPU from hoisting payload loads past the v2 check (ARMv8 plain
        // loads can otherwise be observed after a later LDAR instruction).
        // local_seq guards against ring wrap: a version-stable slot may still
        // carry a newer sequence than expected.
        bool retried = false;
        for (;;) {
            uint64_t v1 = slot.version.load(std::memory_order_acquire);
            if ((v1 & 1u) != 0) {
                MDSYS_CPU_RELAX();
                retried = true;
                continue;
            }

            uint64_t local_seq = slot.local_seq;
            MDUniOrder order = slot.order;

            // Fence: ensure payload loads complete before v2 is read.
            // On ARM this emits dmb ld; on x86 it is a no-op (TSO).
            std::atomic_thread_fence(std::memory_order_acquire);
            uint64_t v2 = slot.version.load(std::memory_order_relaxed);

            if (v1 == v2 && (v2 & 1u) == 0) {
                if (local_seq != expected_seq) {
                    return kErrOverwriteDetected;
                }
                out_buf[i] = order;
                break;
            }
            MDSYS_CPU_RELAX();
            retried = true;
        }
        if (retried) {
            ctx->ctrl->status.reader_retry_count.fetch_add(1, std::memory_order_relaxed);
        }
    }

    // A writer can wrap the ring while the reader is copying. Detect that after
    // the copy and force callers to retry instead of returning mixed epochs.
    uint64_t write_seq_after = ring.header.write_seq.load(std::memory_order_acquire);
    if (write_seq_after - start_seq > static_cast<uint64_t>(kRingCapacity)) {
        return kErrOverwriteDetected;
    }

    *out_count = actual;
    return kOk;
}

}  // namespace mdsys
