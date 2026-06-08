CXX ?= clang++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -Wpedantic -O2 -DDEV_MOCK
LDFLAGS ?= -pthread

COMMON_SRCS := demo.cpp shm_manager.cpp storage_tailer.cpp

.PHONY: all test clean

all: tick_mock

tick_mock: $(COMMON_SRCS) main.cpp
	$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

tick_tests: $(COMMON_SRCS) tests.cpp
	$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

test: tick_tests
	./tick_tests

clean:
	rm -f tick_mock tick_tests
	rm -rf test_wal wal parquet_out
