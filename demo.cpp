#include "demo.h"
#include "storage_tailer.h"

#include <chrono>
#include <utility>

namespace {

uint64_t now_ns() {
    using clock = std::chrono::steady_clock;
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            clock::now().time_since_epoch()).count());
}

}  // namespace

DemoMd::DemoMd(std::string wal_dir, uint32_t trading_day, bool reset_existing)
    : wal_dir_(std::move(wal_dir)),
      trading_day_(trading_day),
      reset_existing_(reset_existing),
      shm_(new mdsys::ShmManager()) {}

DemoMd::~DemoMd() {
    stop();
}

bool DemoMd::start() {
    if (started_) {
        return true;
    }
    if (!shm_->create(reset_existing_, trading_day_)) {
        last_error_ = shm_->last_error();
        return false;
    }

    tailer_.reset(new mdsys::StorageTailer(shm_->context(), wal_dir_));
    if (!tailer_->start()) {
        last_error_ = tailer_->last_error();
        return false;
    }

    started_ = true;
    return true;
}

void DemoMd::stop() {
    if (tailer_) {
        tailer_->stop();
        tailer_.reset();
    }
    started_ = false;
}

mdsys::ShmContext* DemoMd::context() {
    return shm_ ? shm_->context() : nullptr;
}

void DemoMd::on_md(const MDUniOrder& order) {
    if (!started_ || shm_ == nullptr || shm_->context()->ctrl == nullptr) {
        return;
    }

    int32_t symbol_index = get_or_register_instrument(order.instrumentId);
    if (symbol_index < 0) {
        return;
    }

    mdsys::ShmContext* ctx = shm_->context();
    uint64_t seq = global_seq_++;
    uint64_t heartbeat = now_ns();
    bool in_window = mdsys::is_in_trading_window(order.recvTime);

    // If the tailer falls a full log behind, the circular day log may overwrite
    // data that has not been durably written yet. Production should fail closed.
    if (seq - ctx->ctrl->status.durable_wal_seq.load(std::memory_order_relaxed) >=
        mdsys::kLogCapacity) {
        ctx->ctrl->status.daylog_overwrite_count.fetch_add(1, std::memory_order_relaxed);
    }

    // GlobalDayLog is the ordered full-market stream. Storage consumes it by
    // global sequence; strategies read latest-n from the per-symbol rings below.
    mdsys::TickRecord& record = ctx->daylog->records[seq % mdsys::kLogCapacity];
    record.global_seq = seq;
    record.recv_ns = heartbeat;
    record.order = order;
    record.crc32 = 0;
    record.flags = mdsys::kFlagValid | (in_window ? mdsys::kFlagInWindow : 0u);

    mdsys::PerSymbolRing& ring = ctx->rings->rings[symbol_index];
    uint64_t local_seq = ring.header.write_seq.load(std::memory_order_relaxed);
    mdsys::TickSlot& slot = ring.slots[local_seq & (mdsys::kRingCapacity - 1)];
    uint64_t completed_version = (local_seq + 1) << 1;

    // Seqlock publication: odd version marks an in-progress write; the final
    // even version makes the slot visible to readers.
    slot.version.store(completed_version | 1u, std::memory_order_release);
    slot.local_seq = local_seq;
    slot.global_seq = seq;
    slot.order = order;
    slot.version.store(completed_version, std::memory_order_release);
    ring.header.write_seq.store(local_seq + 1, std::memory_order_release);

    ctx->ctrl->header.writer_heartbeat.store(heartbeat, std::memory_order_release);
    ctx->ctrl->header.committed_seq.store(seq + 1, std::memory_order_release);
    ctx->ctrl->status.committed_global_seq.store(seq + 1, std::memory_order_release);
    ctx->ctrl->status.total_ticks_received.fetch_add(1, std::memory_order_relaxed);
    if (in_window) {
        ctx->ctrl->status.total_ticks_in_window.fetch_add(1, std::memory_order_relaxed);
    }
}

int32_t DemoMd::get_or_register_instrument(int32_t instrument_id) {
    if (instrument_id < 0 || instrument_id >= mdsys::kIdArraySize) {
        return mdsys::kErrUnknownInstrument;
    }

    mdsys::ShmContext* ctx = shm_->context();
    int32_t existing = ctx->ctrl->instrument_to_index[instrument_id];
    if (existing >= 0) {
        return existing;
    }
    if (next_symbol_index_ >= mdsys::kInstrumentCount) {
        return mdsys::kErrNoCapacity;
    }

    int32_t assigned = next_symbol_index_++;
    ctx->rings->rings[assigned].header.symbol_id = static_cast<uint32_t>(instrument_id);
    ctx->rings->rings[assigned].header.write_seq.store(0, std::memory_order_relaxed);
    // Single writer today, so no CAS is needed. A multi-feed production build
    // must protect this registration path during startup.
    ctx->ctrl->instrument_to_index[instrument_id] = assigned;
    ctx->ctrl->header.instrument_count = static_cast<uint32_t>(next_symbol_index_);
    return assigned;
}
