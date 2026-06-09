#include "demo.h"
#include "config.h"
#include "shm_layout.h"
#include "storage_tailer.h"

#include <utility>

using namespace mdsys;

DemoMd::DemoMd(std::string wal_dir, uint32_t trading_day)
    : wal_dir_(std::move(wal_dir)),
      trading_day_(trading_day),
      shm_(new mdsys::ShmManager()) {}

DemoMd::~DemoMd() { stop(); }

bool DemoMd::start() {
    if (started_) return true;
    if (!shm_->create(trading_day_)) {
        last_error_ = shm_->error();
        return false;
    }
    tailer_.reset(new mdsys::StorageTailer(&shm_->mapping(), wal_dir_));
    if (!tailer_->start()) {
        last_error_ = tailer_->error();
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

mdsys::Mapping* DemoMd::mapping() { return shm_ ? &shm_->mapping() : nullptr; }

int32_t DemoMd::register_instrument(int32_t instrument_id) {
    if (instrument_id < 0 || instrument_id >= kIdSpace) return kErrUnknownSymbol;
    ControlRegion* c = shm_->mapping().ctrl;
    int32_t existing = c->index[instrument_id];
    if (existing >= 0) return existing;
    if (next_symbol_index_ >= kInstrumentCount) return kErrUnknownSymbol;

    int32_t idx = next_symbol_index_++;
    Ring& ring = shm_->mapping().rings->rings[idx];
    ring.head.symbol_id = static_cast<uint32_t>(instrument_id);
    ring.head.write_seq.store(0, std::memory_order_relaxed);
    c->index[instrument_id] = idx;  // single writer: no CAS needed
    c->sb.instrument_count = static_cast<uint32_t>(next_symbol_index_);
    return idx;
}

int32_t DemoMd::resolve(int32_t instrument_id) {
    if (instrument_id < 0 || instrument_id >= kIdSpace) return -1;
    int32_t idx = shm_->mapping().ctrl->index[instrument_id];
    if (idx >= 0) return idx;
    return register_instrument(instrument_id);  // cold path, first tick per symbol
}

// Hot path. Must only touch memory: lookup, two writes, publish a sequence.
void DemoMd::on_md(const MDUniOrder& order) {
    if (!started_) return;

    int32_t idx = resolve(order.instrumentId);
    if (idx < 0) return;  // unknown symbol / ring table full

    Mapping& m = shm_->mapping();
    uint64_t seq = global_seq_++;
    bool in_window = in_trading_window(order.recvTime);

    // 1) Append to the ordered day log (consumed by the tailer for persistence).
    LogRecord& rec = m.log->records[seq & (kLogCapacity - 1)];
    rec.global_seq = seq;
    rec.order      = order;
    rec.flags      = kFlagValid | (in_window ? kFlagInWindow : 0u);

    // 2) Publish into this symbol's ring via a seqlock. The odd marker is
    //    relaxed and bracketed by a release fence so the payload store cannot
    //    sink in front of it on weak memory models; the even store is the
    //    release that the reader's acquire pairs with.
    Ring& ring = m.rings->rings[idx];
    uint64_t pos = ring.head.write_seq.load(std::memory_order_relaxed);
    Slot& slot = ring.slots[pos & (kRingCapacity - 1)];
    uint64_t stable = stable_seq_for(pos);

    slot.seq.store(stable - 1, std::memory_order_relaxed);  // odd: in progress
    std::atomic_thread_fence(std::memory_order_release);
    slot.order = order;
    slot.seq.store(stable, std::memory_order_release);       // even: published
    ring.head.write_seq.store(pos + 1, std::memory_order_release);

    // 3) Publish the day-log end so the tailer can consume [0, committed_seq).
    m.ctrl->writer.committed_seq.store(seq + 1, std::memory_order_release);
}
