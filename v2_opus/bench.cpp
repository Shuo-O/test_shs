#include "ingest.h"
#include "reader.h"
#include "shm.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

using namespace mdv2;

namespace {

using Clock = std::chrono::steady_clock;

uint64_t to_ns(Clock::duration d) {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(d).count());
}

struct Stats { double p50, p99, p999, max; };

Stats summarize(std::vector<uint64_t> v) {
    std::sort(v.begin(), v.end());
    auto q = [&](double p) {
        return v.empty() ? 0.0 : static_cast<double>(v[static_cast<size_t>(p * (v.size() - 1))]);
    };
    return {q(0.50), q(0.99), q(0.999), v.empty() ? 0.0 : static_cast<double>(v.back())};
}

void print_stats(const char* name, const Stats& s) {
    std::cout << std::left << std::setw(24) << name
              << " p50=" << std::setw(8) << s.p50
              << " p99=" << std::setw(8) << s.p99
              << " p999=" << std::setw(8) << s.p999
              << " max=" << s.max << " ns\n";
}

MDUniOrder make_order(int32_t symbol, int32_t seq, int32_t recv_time = 93000) {
    MDUniOrder o;
    o.instrumentId = symbol;
    o.recvTime = recv_time;
    o.price = 100000 + seq;
    o.qty = 100 + (seq % 1000);
    o.bizSeq = seq;
    o.orderSeq = static_cast<uint32_t>(seq);
    o.orderId = static_cast<uint32_t>(1000000 + seq);
    return o;
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

Stats bench_query(const Mapping& m, int32_t symbol, int n, int iters) {
    std::vector<MDUniOrder> out(static_cast<size_t>(n));
    std::vector<uint64_t> samples;
    samples.reserve(static_cast<size_t>(iters));
    for (int i = 0; i < iters; ++i) {
        auto a = Clock::now();
        int rc = query_latest(m, symbol, n, out.data());
        auto b = Clock::now();
        if (rc <= 0) { std::cerr << "query failed rc=" << rc << "\n"; std::exit(1); }
        samples.push_back(to_ns(b - a));
    }
    return summarize(std::move(samples));
}

}  // namespace

int main(int argc, char** argv) {
    int rows = 200000;
    int symbols = 128;
    int query_iters = 50000;
    std::string wal_dir = "bench_v2_wal";
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--rows" && i + 1 < argc) rows = std::atoi(argv[++i]);
        else if (a == "--symbols" && i + 1 < argc) symbols = std::atoi(argv[++i]);
        else if (a == "--query-iters" && i + 1 < argc) query_iters = std::atoi(argv[++i]);
        else if (a == "--wal-dir" && i + 1 < argc) wal_dir = argv[++i];
    }
    symbols = std::max(1, std::min(symbols, kInstrumentCount));

    std::filesystem::remove_all(wal_dir);
    Shm::unlink();

    MdStore md(wal_dir, 20260608);
    if (!md.start()) { std::cerr << "start failed: " << md.error() << "\n"; return 1; }

    // Pre-register symbols out of band so the measured path is pure lookup.
    for (int i = 0; i < symbols; ++i) md.register_instrument(600000 + i);

    std::vector<uint64_t> ingest;
    ingest.reserve(static_cast<size_t>(rows));
    auto t0 = Clock::now();
    for (int i = 0; i < rows; ++i) {
        MDUniOrder o = make_order(600000 + (i % symbols), i, 93000);
        auto a = Clock::now();
        md.on_md(o);
        auto b = Clock::now();
        ingest.push_back(to_ns(b - a));
    }
    auto t1 = Clock::now();

    double secs = static_cast<double>(to_ns(t1 - t0)) / 1e9;
    Stats ingest_stats = summarize(std::move(ingest));

    Shm reader;
    if (!reader.open(true, false)) { std::cerr << "reader open failed: " << reader.error() << "\n"; return 1; }
    Stats q100 = bench_query(reader.mapping(), 600000, 100, query_iters);
    Stats q1000 = bench_query(reader.mapping(), 600000, 1000, query_iters / 5);

    auto s0 = Clock::now();
    md.stop();
    auto s1 = Clock::now();

    uint64_t wal_rows = count_wal_rows(wal_dir);

    std::cout << "BENCHMARK_RESULT\n";
    std::cout << "rows=" << rows << "\n";
    std::cout << "symbols=" << symbols << "\n";
    std::cout << "ingest_seconds=" << secs << "\n";
    std::cout << "ingest_throughput_ticks_per_sec=" << (rows / secs) << "\n";
    print_stats("on_md", ingest_stats);
    print_stats("query_latest(100)", q100);
    print_stats("query_latest(1000)", q1000);
    std::cout << "tailer_drain_ms=" << (static_cast<double>(to_ns(s1 - s0)) / 1e6) << "\n";
    std::cout << "wal_rows=" << wal_rows << "\n";
    std::cout << "committed_seq=" << md.mapping()->ctrl->wc.committed_seq.load() << "\n";
    std::cout << "durable_seq=" << md.mapping()->ctrl->tstats.durable_seq.load() << "\n";
    std::cout << "log_overwrite_count=" << md.mapping()->ctrl->tstats.log_overwrite_count.load() << "\n";

    reader.close();
    Shm::unlink();
    return wal_rows == static_cast<uint64_t>(rows) ? 0 : 1;
}
