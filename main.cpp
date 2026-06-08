#include "demo.h"
#include "shm_layout.h"

#include <atomic>
#include <csignal>
#include <iostream>
#include <thread>

namespace {
std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop.store(true, std::memory_order_release); }
}  // namespace

// Production entry point: bring up the shared-memory store and the storage
// tailer, then stay resident. The live feed delivers ticks by calling
// DemoMd::on_md from the ingest thread; strategy processes attach read-only via
// ShmManager::open and query through strategy_reader.h.
int main() {
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    DemoMd md;
    if (!md.start()) {
        std::cerr << "failed to start market-data store: " << md.last_error() << "\n";
        return 1;
    }
    std::cerr << "market-data store running (Ctrl-C to stop)\n";

    while (!g_stop.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    md.stop();
    std::cerr << "stopped\n";
    return 0;
}
