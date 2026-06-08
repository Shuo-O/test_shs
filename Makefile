CXX ?= clang++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -Wpedantic -O2 -DDEV_MOCK
LDFLAGS ?= -pthread
PYTHON ?= .venv/bin/python
BENCH_ROWS ?= 200000

COMMON_SRCS := demo.cpp shm_manager.cpp storage_tailer.cpp

.PHONY: all test cpp-test parquet-test setup bench clean

all: tick_mock

.venv/bin/python:
	python3 -m venv .venv

.venv/.requirements.stamp: requirements.txt .venv/bin/python
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt
	touch $@

setup: .venv/.requirements.stamp

tick_mock: $(COMMON_SRCS) main.cpp
	$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

tick_tests: $(COMMON_SRCS) tests.cpp
	$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

tick_bench: $(COMMON_SRCS) benchmark.cpp
	$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

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
	rm -f tick_mock tick_tests tick_bench
	rm -rf test_wal wal parquet_out bench_wal bench_parquet
