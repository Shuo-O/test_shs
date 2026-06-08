#include "ingest.h"
#include "reader.h"
#include "shm.h"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>

using namespace mdv2;

namespace {

// Encode the sequence number into several fields so a torn seqlock read (mixing
// two writer epochs) is detectable by a self-consistency check.
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

int64_t check_invariant(const MDUniOrder& o) {
    int64_t s = o.bizSeq;
    assert(o.price == 100000 + s && "torn: price");
    assert(o.qty == 10 + s && "torn: qty");
    assert(o.orderSeq == static_cast<uint32_t>(s) && "torn: orderSeq");
    assert(o.orderId == static_cast<uint32_t>(1000000 + s) && "torn: orderId");
    return s;
}

uint64_t count_wal_rows(const std::filesystem::path& dir) {
    uint64_t rows = 0;
    if (!std::filesystem::exists(dir)) return 0;
    for (auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() == ".bin") {
            rows += static_cast<uint64_t>(std::filesystem::file_size(e.path()) / sizeof(LogRecord));
        }
    }
    return rows;
}

// Functional: latest-n correctness, unknown symbol, ring wrap, WAL window filter.
void test_functional() {
    const std::filesystem::path wal = "test_v2_wal";
    std::filesystem::remove_all(wal);
    Shm::unlink();

    MdStore md(wal.string(), 20260608);
    assert(md.start() && "start failed");

    for (int i = 0; i < 1500; ++i) md.on_md(make_order(600000, i));

    Shm reader;
    assert(reader.open(/*read_only=*/true, /*with_log=*/false) && "reader open failed");
    const Mapping& rm = reader.mapping();

    MDUniOrder out[10];
    int n = query_latest(rm, 600000, 10, out);
    assert(n == 10);
    assert(out[0].price == 100000 + 1490);
    assert(out[9].price == 100000 + 1499);

    assert(query_latest(rm, 123456, 10, out) == kErrUnknownSymbol);
    assert(query_latest(rm, -1, 10, out) == kErrUnknownSymbol);

    // Overflow the ring so the oldest records are overwritten.
    for (int i = 0; i < kRingCapacity + 5; ++i) md.on_md(make_order(600001, i));
    std::vector<MDUniOrder> full(kRingCapacity);
    n = query_latest(rm, 600001, kRingCapacity, full.data());
    assert(n == kRingCapacity);
    assert(full.front().price == 100000 + 5);
    assert(full.back().price == 100000 + kRingCapacity + 4);

    // One out-of-window tick must be excluded from the WAL.
    md.on_md(make_order(600002, 0, 80000));
    uint64_t expect_in_window = 1500 + static_cast<uint64_t>(kRingCapacity) + 5;
    md.stop();

    assert(count_wal_rows(wal) == expect_in_window);
    assert(md.mapping()->ctrl->tstats.durable_seq.load() ==
           1500 + static_cast<uint64_t>(kRingCapacity) + 6);

    reader.close();
    Shm::unlink();
    std::filesystem::remove_all(wal);
    std::cout << "functional: ok\n";
}

// Concurrency: one writer, several read-only readers, torn-read invariant.
void test_concurrent() {
    const std::filesystem::path wal = "test_v2_wal_conc";
    std::filesystem::remove_all(wal);
    Shm::unlink();

    MdStore md(wal.string(), 20260608);
    assert(md.start() && "start failed");

    const int32_t kSymbol = 600000;
    const int64_t kWrites = 2'000'000;
    md.register_instrument(kSymbol);
    md.on_md(make_order(kSymbol, 0));

    Shm reader;
    assert(reader.open(true, false) && "reader open failed");
    const Mapping& rm = reader.mapping();

    std::atomic<bool> done{false};
    std::atomic<uint64_t> ok{0}, retries{0}, overwrites{0};

    auto reader_fn = [&] {
        std::vector<MDUniOrder> buf(256);
        QueryStats qs;
        while (!done.load(std::memory_order_acquire)) {
            int rc = query_latest(rm, kSymbol, 256, buf.data(), &qs);
            if (rc == kErrOverwritten) continue;
            assert(rc >= 0);
            int64_t prev = -1;
            for (int i = 0; i < rc; ++i) {
                int64_t s = check_invariant(buf[i]);
                if (prev >= 0) assert(s == prev + 1 && "non-consecutive");
                prev = s;
            }
            ok.fetch_add(1, std::memory_order_relaxed);
        }
        retries.fetch_add(qs.retries, std::memory_order_relaxed);
        overwrites.fetch_add(qs.overwrites, std::memory_order_relaxed);
    };

    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) readers.emplace_back(reader_fn);
    for (int64_t s = 1; s < kWrites; ++s) md.on_md(make_order(kSymbol, static_cast<int32_t>(s)));
    done.store(true, std::memory_order_release);
    for (auto& t : readers) t.join();

    assert(ok.load() > 0 && "readers never succeeded");
    std::cout << "concurrent: " << ok.load() << " consistent reads, retries="
              << retries.load() << " overwrites=" << overwrites.load() << "\n";

    reader.close();
    md.stop();
    Shm::unlink();
    std::filesystem::remove_all(wal);
}

}  // namespace

int main() {
    test_functional();
    test_concurrent();
    std::cout << "All v2 tests passed\n";
    return 0;
}
