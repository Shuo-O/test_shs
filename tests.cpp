#include "demo.h"
#include "shm_manager.h"
#include "strategy_reader.h"

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>

namespace {

MDUniOrder make_order(int32_t instrument_id, int32_t seq, int32_t recv_time = 93000) {
    MDUniOrder order;
    order.instrumentId = instrument_id;
    order.recvTime = recv_time;
    order.price = 100000 + seq;
    order.qty = 10 + seq;
    order.bizSeq = seq;
    order.orderSeq = static_cast<uint32_t>(seq);
    order.orderId = static_cast<uint32_t>(1000000 + seq);
    return order;
}

uint64_t count_wal_records(const std::filesystem::path& dir) {
    uint64_t rows = 0;
    if (!std::filesystem::exists(dir)) {
        return 0;
    }
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.path().extension() == ".bin") {
            rows += static_cast<uint64_t>(
                std::filesystem::file_size(entry.path()) / sizeof(mdsys::LogRecord));
        }
    }
    return rows;
}

void test_latest_n_and_wal() {
    const std::filesystem::path wal_dir = "test_wal";
    const bool keep_wal = std::getenv("KEEP_TEST_WAL") != nullptr;
    std::filesystem::remove_all(wal_dir);
    mdsys::ShmManager::unlink();

    DemoMd md(wal_dir.string(), 20260608);
    assert(md.start() && "DemoMd failed to start");

    for (int i = 0; i < 1500; ++i) {
        md.on_md(make_order(600000, i));
    }

    mdsys::ShmManager reader;
    assert(reader.open(true, false) && "reader failed to open shared memory");
    const mdsys::Mapping& mapping = reader.mapping();

    MDUniOrder latest[10];
    int count = mdsys::query_latest_n(mapping, 600000, 10, latest);
    assert(count == 10);
    assert(latest[0].price == 100000 + 1490);
    assert(latest[9].price == 100000 + 1499);

    int rc = mdsys::query_latest_n(mapping, 123456, 10, latest);
    assert(rc == mdsys::kErrUnknownSymbol);

    rc = mdsys::query_latest_n(mapping, -1, 10, latest);
    assert(rc == mdsys::kErrUnknownSymbol);

    for (int i = 0; i < mdsys::kRingCapacity + 5; ++i) {
        md.on_md(make_order(600001, i));
    }

    std::vector<MDUniOrder> wrapped(mdsys::kRingCapacity);
    count = mdsys::query_latest_n(mapping, 600001, mdsys::kRingCapacity, wrapped.data());
    assert(count == mdsys::kRingCapacity);
    assert(wrapped.front().price == 100000 + 5);
    assert(wrapped.back().price == 100000 + mdsys::kRingCapacity + 4);

    md.on_md(make_order(600002, 0, 80000));

    uint64_t expected_in_window =
        1500 + static_cast<uint64_t>(mdsys::kRingCapacity) + 5;
    md.stop();

    uint64_t wal_rows = count_wal_records(wal_dir);
    assert(wal_rows == expected_in_window);
    assert(md.mapping()->ctrl->tstats.durable_seq.load() ==
           1500 + static_cast<uint64_t>(mdsys::kRingCapacity) + 6);

    reader.close();
    mdsys::ShmManager::unlink();
    if (!keep_wal) {
        std::filesystem::remove_all(wal_dir);
    }
}

// make_order encodes the same sequence number into multiple fields. A
// torn seqlock read (mixing two writer epochs) breaks this invariant, so it
// doubles as a torn-read detector under concurrency. Returns the encoded seq.
int64_t check_record_invariant(const MDUniOrder& o) {
    int64_t seq = o.bizSeq;
    assert(o.price == 100000 + seq && "torn read: price/bizSeq mismatch");
    assert(o.qty == 10 + seq && "torn read: qty/bizSeq mismatch");
    assert(o.orderSeq == static_cast<uint32_t>(seq) && "torn read: orderSeq mismatch");
    assert(o.orderId == static_cast<uint32_t>(1000000 + seq) && "torn read: orderId mismatch");
    return seq;
}

// Stress the seqlock with one writer and several concurrent readers. On a weak
// memory model (this runs on arm64) a missing writer/reader fence would let a
// torn read slip past the version check and trip check_record_invariant.
void test_concurrent_seqlock() {
    const std::filesystem::path wal_dir = "test_wal_concurrent";
    std::filesystem::remove_all(wal_dir);
    mdsys::ShmManager::unlink();

    DemoMd md(wal_dir.string(), 20260608);
    assert(md.start() && "DemoMd failed to start");

    const int32_t kSymbol = 600000;
    const int64_t kWrites = 2'000'000;
    md.on_md(make_order(kSymbol, 0));  // register the symbol before readers run

    std::atomic<bool> done{false};
    std::atomic<uint64_t> reads_ok{0};
    std::atomic<uint64_t> total_retries{0};
    std::atomic<uint64_t> total_overwrites{0};

    // Strategy readers map the data segments read-only, exactly as a separate
    // process would, proving query_latest_n never writes through ctx.
    mdsys::ShmManager reader_shm;
    assert(reader_shm.open(true, false) && "reader failed to open shared memory");
    const mdsys::Mapping& rctx = reader_shm.mapping();

    auto reader_fn = [&]() {
        std::vector<MDUniOrder> buf(256);
        mdsys::QueryStats qs;  // process/thread-local, not shared memory
        while (!done.load(std::memory_order_acquire)) {
            int count = mdsys::query_latest_n(rctx, kSymbol, 256, buf.data(), &qs);
            if (count == mdsys::kErrOverwritten) {
                continue;  // writer wrapped mid-copy; legitimate, just retry
            }
            assert(count >= 0);
            int64_t prev = -1;
            for (int i = 0; i < count; ++i) {
                int64_t seq = check_record_invariant(buf[i]);
                // Records must be returned in arrival order, strictly +1 apart.
                if (prev >= 0) {
                    assert(seq == prev + 1 && "records not consecutive");
                }
                prev = seq;
            }
            reads_ok.fetch_add(1, std::memory_order_relaxed);
        }
        total_retries.fetch_add(qs.retries, std::memory_order_relaxed);
        total_overwrites.fetch_add(qs.overwrites, std::memory_order_relaxed);
    };

    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back(reader_fn);
    }

    for (int64_t s = 1; s < kWrites; ++s) {
        md.on_md(make_order(kSymbol, static_cast<int32_t>(s)));
    }
    done.store(true, std::memory_order_release);
    for (auto& t : readers) {
        t.join();
    }

    assert(reads_ok.load() > 0 && "readers never completed a query");
    std::cout << "concurrent: " << reads_ok.load() << " consistent reads, retries="
              << total_retries.load() << " overwrites=" << total_overwrites.load()
              << "\n";

    reader_shm.close();
    md.stop();
    mdsys::ShmManager::unlink();
    std::filesystem::remove_all(wal_dir);
}

}  // namespace

int main() {
    test_latest_n_and_wal();
    test_concurrent_seqlock();
    std::cout << "All tests passed\n";
    return 0;
}
