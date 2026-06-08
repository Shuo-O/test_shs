CXX ?= c++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -Wpedantic -O3 -march=native
LDFLAGS ?= -pthread

COMMON_SRCS := demo.cpp shm_manager.cpp storage_tailer.cpp

.PHONY: all clean

all: tick_server

tick_server: $(COMMON_SRCS) main.cpp
	$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

clean:
	rm -f tick_server
	rm -rf wal parquet_out
