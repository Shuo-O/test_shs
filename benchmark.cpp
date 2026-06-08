#include "demo.h"
#include "shm_manager.h"
#include "strategy_reader.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
    int rows = 200000;
    int symbols = 128;
    int query_iters = 50000;
    std::string wal_dir = "bench_wal";
};

struct Stats {
    double p50_ns = 0.0;
    double p99_ns = 0.0;
    double p999_ns = 0.0;
    double max_ns = 0.0;
};

uint64_t to_ns(Clock::duration duration) {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count());
}

Stats summarize(std::vector<uint64_t> values) {
    std::sort(values.begin(), values.end());
    auto pick = [&](double q) -> double {
        if (values.empty()) {
            return 0.0;
        }
        size_t idx = static_cast<size_t>(q * static_cast<double>(values.size() - 1));
        return static_cast<double>(values[idx]);
    };
    Stats stats;
    stats.p50_ns = pick(0.50);
    stats.p99_ns = pick(0.99);
    stats.p999_ns = pick(0.999);
    stats.max_ns = values.empty() ? 0.0 : static_cast<double>(values.back());
    return stats;
}

MDUniOrder make_order(int32_t instrument_id, int32_t seq, int32_t recv_time = 93000) {
    MDUniOrder order;
    order.instrumentId = instrument_id;
    order.recvTime = recv_time;
    order.price = 100000 + seq;
    order.qty = 100 + (seq % 1000);
    order.bizSeq = seq;
    order.orderSeq = static_cast<uint32_t>(seq);
    order.orderId = static_cast<uint32_t>(1000000 + seq);
    return order;
}

Options parse_args(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto need_value = [&](const char* name) -> const char* {
            if (i + 1 >= argc) {
                std::cerr << name << " requires a value\n";
                std::exit(2);
            }
            return argv[++i];
        };
        if (arg == "--rows") {
            options.rows = std::atoi(need_value("--rows"));
        } else if (arg == "--symbols") {
            options.symbols = std::atoi(need_value("--symbols"));
        } else if (arg == "--query-iters") {
            options.query_iters = std::atoi(need_value("--query-iters"));
        } else if (arg == "--wal-dir") {
            options.wal_dir = need_value("--wal-dir");
        } else {
            std::cerr << "unknown argument: " << arg << "\n";
            std::exit(2);
        }
    }
    options.rows = std::max(options.rows, 1);
    options.symbols = std::max(1, std::min(options.symbols, mdsys::kInstrumentCount));
    options.query_iters = std::max(options.query_iters, 1);
    return options;
}

void print_stats(const std::string& name, const Stats& stats) {
    std::cout << std::left << std::setw(24) << name
              << " p50=" << std::setw(8) << stats.p50_ns
              << " p99=" << std::setw(8) << stats.p99_ns
              << " p999=" << std::setw(8) << stats.p999_ns
              << " max=" << stats.max_ns << " ns\n";
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

Stats benchmark_query(const mdsys::ShmContext* ctx,
                      int32_t instrument_id,
                      int n,
                      int iterations) {
    std::vector<MDUniOrder> out(static_cast<size_t>(n));
    std::vector<uint64_t> samples;
    samples.reserve(static_cast<size_t>(iterations));
    int count = 0;
    for (int i = 0; i < iterations; ++i) {
        auto start = Clock::now();
        int rc = mdsys::query_latest_n(ctx, instrument_id, n, out.data(), &count);
        auto end = Clock::now();
        if (rc != mdsys::kOk || count <= 0) {
            std::cerr << "query_latest_n failed, rc=" << rc << " count=" << count << "\n";
            std::exit(1);
        }
        samples.push_back(to_ns(end - start));
    }
    return summarize(std::move(samples));
}

}  // namespace

int main(int argc, char** argv) {
    Options options = parse_args(argc, argv);

    std::filesystem::remove_all(options.wal_dir);
    mdsys::ShmManager::unlink_all();

    DemoMd md(options.wal_dir, 20260608, true);
    if (!md.start()) {
        std::cerr << "DemoMd start failed: " << md.last_error() << "\n";
        return 1;
    }

    for (int i = 0; i < options.symbols; ++i) {
        // Warm up instrument registration outside the persistence window so
        // measured rows only reflect the steady on_md path.
        md.on_md(make_order(600000 + i, i, 80000));
    }

    std::vector<uint64_t> ingest_samples;
    ingest_samples.reserve(static_cast<size_t>(options.rows));

    auto ingest_begin = Clock::now();
    for (int i = 0; i < options.rows; ++i) {
        int32_t instrument = 600000 + (i % options.symbols);
        MDUniOrder order = make_order(instrument, i, 93000);
        auto start = Clock::now();
        md.on_md(order);
        auto end = Clock::now();
        ingest_samples.push_back(to_ns(end - start));
    }
    auto ingest_end = Clock::now();

    double ingest_seconds =
        static_cast<double>(to_ns(ingest_end - ingest_begin)) / 1'000'000'000.0;
    double throughput = static_cast<double>(options.rows) / ingest_seconds;
    Stats ingest_stats = summarize(std::move(ingest_samples));

    mdsys::ShmManager reader;
    if (!reader.open(false, true)) {
        std::cerr << "reader open failed: " << reader.last_error() << "\n";
        return 1;
    }

    Stats query_100 = benchmark_query(reader.context(), 600000, 100, options.query_iters);
    // latest-1000 copies 40KB per call, so fewer iterations keep the benchmark
    // quick while still enough to expose p99 behavior.
    Stats query_1000 = benchmark_query(reader.context(), 600000, 1000, options.query_iters / 5);

    auto stop_begin = Clock::now();
    md.stop();
    auto stop_end = Clock::now();
    double drain_ms = static_cast<double>(to_ns(stop_end - stop_begin)) / 1'000'000.0;

    uint64_t wal_rows = count_wal_records(options.wal_dir);
    uint64_t committed = md.context()->ctrl->header.committed_seq.load();
    uint64_t durable = md.context()->ctrl->status.tailer.durable_wal_seq.load();

    std::cout << "BENCHMARK_RESULT\n";
    std::cout << "rows=" << options.rows << "\n";
    std::cout << "symbols=" << options.symbols << "\n";
    std::cout << "ingest_seconds=" << ingest_seconds << "\n";
    std::cout << "ingest_throughput_ticks_per_sec=" << throughput << "\n";
    print_stats("on_md", ingest_stats);
    print_stats("query_latest_n(100)", query_100);
    print_stats("query_latest_n(1000)", query_1000);
    std::cout << "tailer_drain_ms=" << drain_ms << "\n";
    std::cout << "wal_rows=" << wal_rows << "\n";
    std::cout << "committed_seq=" << committed << "\n";
    std::cout << "durable_wal_seq=" << durable << "\n";
    std::cout << "daylog_overwrite_count="
              << md.context()->ctrl->status.tailer.daylog_overwrite_count.load() << "\n";

    reader.close();
    mdsys::ShmManager::unlink_all();
    return wal_rows == static_cast<uint64_t>(options.rows) ? 0 : 1;
}
