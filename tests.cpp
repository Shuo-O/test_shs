// Minimal test / demo for the two requirements.
//   A) latest-n query by instrument id, correct under concurrent readers.
//   B) all 09:25-15:00 ticks land in the WAL (then -> Parquet via the script).
// Build with the default (small) config: `make test`.

#include "demo.h"
#include "shm_manager.h"
#include "strategy_reader.h"

#include <atomic>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>

using namespace mdsys;

namespace {

// Encode the sequence number into several order fields so that a torn seqlock
// read (a mix of two writer epochs) is detectable by a self-consistency check.
MDUniOrder make_order(int32_t symbol, int32_t seq, int32_t recv_time = 93000) {
    MDUniOrder o;
    o.instrumentId = symbol;
    o.recvTime     = recv_time;
    o.price        = 100000 + seq;
    o.qty          = 10 + seq;
    o.bizSeq       = seq;
    o.orderSeq     = static_cast<uint32_t>(seq);
    o.orderId      = static_cast<uint32_t>(1000000 + seq);
    return o;
}

int64_t check_consistent(const MDUniOrder& o) {
    int64_t s = o.bizSeq;
    assert(o.price == 100000 + s && "torn read: price");
    assert(o.qty == 10 + s && "torn read: qty");
    assert(o.orderSeq == static_cast<uint32_t>(s) && "torn read: orderSeq");
    assert(o.orderId == static_cast<uint32_t>(1000000 + s) && "torn read: orderId");
    return s;
}

uint64_t count_wal_rows(const std::filesystem::path& dir) {
    uint64_t rows = 0;
    if (!std::filesystem::exists(dir)) return 0;
    for (auto& e : std::filesystem::directory_iterator(dir))
        if (e.path().extension() == ".bin")
            rows += std::filesystem::file_size(e.path()) / sizeof(LogRecord);
    return rows;
}

// Requirement A + B, single threaded.
void test_functional() {
    const std::filesystem::path wal = "test_wal";
    std::filesystem::remove_all(wal);
    ShmManager::unlink();

    DemoMd md(wal.string(), 20260608);
    assert(md.start());

    for (int i = 0; i < 1500; ++i) md.on_md(make_order(600000, i));

    // A strategy attaches read-only (as a separate process would) and queries.
    ShmManager reader;
    assert(reader.open(/*read_only=*/true, /*with_log=*/false));
    MDUniOrder out[10];
    int n = query_latest_n(reader.mapping(), 600000, 10, out);
    assert(n == 10);
    assert(out[0].price == 100000 + 1490);  // latest 10, oldest first
    assert(out[9].price == 100000 + 1499);
    assert(query_latest_n(reader.mapping(), 123456, 10, out) == kErrUnknownSymbol);

    // Overflow the ring: the oldest records are overwritten, query returns the
    // newest ring_capacity records.
    for (int i = 0; i < kRingCapacity + 5; ++i) md.on_md(make_order(600001, i));
    std::vector<MDUniOrder> full(kRingCapacity);
    n = query_latest_n(reader.mapping(), 600001, kRingCapacity, full.data());
    assert(n == kRingCapacity);
    assert(full.front().price == 100000 + 5);

    // One out-of-window tick must NOT be persisted.
    md.on_md(make_order(600002, 0, /*recv_time=*/80000));
    uint64_t expect_in_window = 1500 + static_cast<uint64_t>(kRingCapacity) + 5;
    md.stop();  // drains + fsyncs the tailer

    assert(count_wal_rows(wal) == expect_in_window);          // requirement B
    // After a clean drain the cursors converge, even when the tail of the day
    // log is out-of-window (durable tracks consumption, not just WAL writes).
    assert(md.mapping()->ctrl->tailer.durable_seq.load() ==
           md.mapping()->ctrl->writer.committed_seq.load());
    reader.close();
    ShmManager::unlink();
    std::filesystem::remove_all(wal);
    std::cout << "functional: ok\n";
}

// Requirement A under contention: one writer, four read-only readers, torn-read
// detector. On arm64 (a weak memory model) this would expose a missing fence.
void test_concurrent() {
    const std::filesystem::path wal = "test_wal_concurrent";
    std::filesystem::remove_all(wal);
    ShmManager::unlink();

    DemoMd md(wal.string(), 20260608);
    assert(md.start());
    const int32_t sym = 600000;
    md.register_instrument(sym);
    md.on_md(make_order(sym, 0));

    ShmManager reader;
    assert(reader.open(true, false));
    const Mapping& rm = reader.mapping();

    std::atomic<bool> done{false};
    std::atomic<uint64_t> ok{0}, overwrites{0};
    auto reader_fn = [&] {
        std::vector<MDUniOrder> buf(256);
        QueryStats qs;
        while (!done.load(std::memory_order_acquire)) {
            int rc = query_latest_n(rm, sym, 256, buf.data(), &qs);
            if (rc == kErrOverwritten) continue;  // ring wrapped mid-read; retry
            assert(rc >= 0);
            int64_t prev = -1;
            for (int i = 0; i < rc; ++i) {
                int64_t s = check_consistent(buf[i]);
                if (prev >= 0) assert(s == prev + 1 && "records not consecutive");
                prev = s;
            }
            ok.fetch_add(1, std::memory_order_relaxed);
        }
        overwrites.fetch_add(qs.overwrites, std::memory_order_relaxed);
    };

    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) readers.emplace_back(reader_fn);
    for (int64_t s = 1; s < 2'000'000; ++s) md.on_md(make_order(sym, static_cast<int32_t>(s)));
    done.store(true, std::memory_order_release);
    for (auto& t : readers) t.join();

    assert(ok.load() > 0);
    std::cout << "concurrent: " << ok.load() << " consistent reads, overwrites="
              << overwrites.load() << "\n";

    reader.close();
    md.stop();
    ShmManager::unlink();
    std::filesystem::remove_all(wal);
}

}  // namespace

int main() {
    test_functional();
    test_concurrent();
    std::cout << "All tests passed\n";
    return 0;
}
