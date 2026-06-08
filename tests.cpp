#include "demo.h"
#include "shm_manager.h"
#include "strategy_reader.h"

#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <iostream>
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
                std::filesystem::file_size(entry.path()) / sizeof(mdsys::TickRecord));
        }
    }
    return rows;
}

void test_latest_n_and_wal() {
    const std::filesystem::path wal_dir = "test_wal";
    const bool keep_wal = std::getenv("KEEP_TEST_WAL") != nullptr;
    std::filesystem::remove_all(wal_dir);
    mdsys::ShmManager::unlink_all();

    DemoMd md(wal_dir.string(), 20260608, true);
    assert(md.start() && "DemoMd failed to start");

    for (int i = 0; i < 1500; ++i) {
        md.on_md(make_order(600000, i));
    }

    mdsys::ShmManager reader;
    assert(reader.open(false, true) && "reader failed to open shared memory");

    MDUniOrder latest[10];
    int count = 0;
    int rc = mdsys::query_latest_n(reader.context(), 600000, 10, latest, &count);
    assert(rc == mdsys::kOk);
    assert(count == 10);
    assert(latest[0].price == 100000 + 1490);
    assert(latest[9].price == 100000 + 1499);

    rc = mdsys::query_latest_n(reader.context(), 123456, 10, latest, &count);
    assert(rc == mdsys::kErrUnknownInstrument);

    rc = mdsys::query_latest_n(reader.context(), -1, 10, latest, &count);
    assert(rc == mdsys::kErrUnknownInstrument);

    for (int i = 0; i < mdsys::kRingCapacity + 5; ++i) {
        md.on_md(make_order(600001, i));
    }

    std::vector<MDUniOrder> wrapped(mdsys::kRingCapacity);
    rc = mdsys::query_latest_n(reader.context(),
                               600001,
                               mdsys::kRingCapacity,
                               wrapped.data(),
                               &count);
    assert(rc == mdsys::kOk);
    assert(count == mdsys::kRingCapacity);
    assert(wrapped.front().price == 100000 + 5);
    assert(wrapped.back().price == 100000 + mdsys::kRingCapacity + 4);

    md.on_md(make_order(600002, 0, 80000));

    uint64_t expected_in_window =
        1500 + static_cast<uint64_t>(mdsys::kRingCapacity) + 5;
    md.stop();

    uint64_t wal_rows = count_wal_records(wal_dir);
    assert(wal_rows == expected_in_window);

    reader.close();
    mdsys::ShmManager::unlink_all();
    if (!keep_wal) {
        std::filesystem::remove_all(wal_dir);
    }
}

}  // namespace

int main() {
    test_latest_n_and_wal();
    std::cout << "All tests passed\n";
    return 0;
}
