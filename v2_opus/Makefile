CXX      ?= clang++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -Wpedantic -O3
LDFLAGS  ?= -pthread
PROFILE  ?=            # set to -DMDV2_STANDARD or -DMDV2_EXTENDED for prod sizing

CORE := ingest.cpp shm.cpp tailer.cpp

.PHONY: all test bench clean

all: v2_test v2_bench

v2_test: $(CORE) test.cpp
	$(CXX) $(CXXFLAGS) $(PROFILE) $^ $(LDFLAGS) -o $@

v2_bench: $(CORE) bench.cpp
	$(CXX) $(CXXFLAGS) $(PROFILE) $^ $(LDFLAGS) -o $@

test: v2_test
	./v2_test

bench: v2_bench
	rm -rf bench_v2_wal
	./v2_bench --rows $(if $(ROWS),$(ROWS),200000) --wal-dir bench_v2_wal

clean:
	rm -f v2_test v2_bench
	rm -rf bench_v2_wal test_v2_wal test_v2_wal_conc wal_v2 parquet_v2_out
