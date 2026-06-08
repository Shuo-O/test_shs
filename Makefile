CXX ?= clang++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -Wpedantic -O2 -DDEV_MOCK
LDFLAGS ?= -pthread
PYTHON ?= .venv/bin/python

COMMON_SRCS := demo.cpp shm_manager.cpp storage_tailer.cpp

.PHONY: all test cpp-test parquet-test setup clean

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

cpp-test: tick_tests
	./tick_tests

parquet-test: tick_tests .venv/.requirements.stamp
	rm -rf test_wal parquet_out
	KEEP_TEST_WAL=1 ./tick_tests
	$(PYTHON) scripts/wal_to_parquet.py test_wal parquet_out --trading-day 20260608
	$(PYTHON) scripts/verify_parquet.py parquet_out 2529
	rm -rf test_wal parquet_out

test: cpp-test parquet-test

clean:
	rm -f tick_mock tick_tests
	rm -rf test_wal wal parquet_out
