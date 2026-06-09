CXX ?= c++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -Wpedantic -O3 -march=native
LDFLAGS ?= -pthread
PYTHON ?= .venv/bin/python
BENCH_ROWS ?= 500000

CORE_SRCS := demo.cpp shm_manager.cpp storage_tailer.cpp
SRCS := $(CORE_SRCS) main.cpp
HDRS := config.h clock.h md.h shm_layout.h shm_manager.h strategy_reader.h \
        demo.h storage_tailer.h

.PHONY: all setup test cpp-test parquet-test bench clean

all: tick_server

tick_server: $(SRCS) $(HDRS)
	$(CXX) $(CXXFLAGS) $(SRCS) $(LDFLAGS) -o $@

.venv/bin/python:
	python3 -m venv .venv

.venv/.requirements.stamp: requirements.txt .venv/bin/python
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt
	touch $@

setup: .venv/.requirements.stamp

tick_tests: $(CORE_SRCS) tests.cpp $(HDRS)
	$(CXX) $(CXXFLAGS) -DDEV_MOCK $(CORE_SRCS) tests.cpp $(LDFLAGS) -o $@

tick_bench: $(CORE_SRCS) benchmark.cpp $(HDRS)
	$(CXX) $(CXXFLAGS) -DDEV_MOCK $(CORE_SRCS) benchmark.cpp $(LDFLAGS) -o $@

cpp-test: tick_tests
	./tick_tests

parquet-test: tick_tests .venv/.requirements.stamp
	rm -rf test_wal parquet_out
	KEEP_TEST_WAL=1 ./tick_tests
	$(PYTHON) scripts/wal_to_parquet.py test_wal parquet_out --trading-day 20260608
	$(PYTHON) scripts/verify_parquet.py parquet_out 2529 --expected-instruments 600000,600001 --require-prices 100000,101499
	rm -rf test_wal parquet_out

test: cpp-test parquet-test

bench: tick_bench .venv/.requirements.stamp
	rm -rf bench_wal bench_parquet
	./tick_bench --rows $(BENCH_ROWS) --wal-dir bench_wal
	/usr/bin/time -p $(PYTHON) scripts/wal_to_parquet.py bench_wal bench_parquet --trading-day 20260608
	$(PYTHON) scripts/verify_parquet.py bench_parquet $(BENCH_ROWS)

clean:
	rm -f tick_server tick_tests tick_bench
	rm -rf wal parquet_out test_wal test_wal_concurrent bench_wal bench_parquet
