CXX ?= c++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -Wpedantic -O3 -march=native
LDFLAGS ?= -pthread

SRCS := demo.cpp shm_manager.cpp storage_tailer.cpp main.cpp
HDRS := config.h clock.h md.h shm_layout.h shm_manager.h strategy_reader.h \
        demo.h storage_tailer.h

.PHONY: all clean

all: tick_server

tick_server: $(SRCS) $(HDRS)
	$(CXX) $(CXXFLAGS) $(SRCS) $(LDFLAGS) -o $@

clean:
	rm -f tick_server
	rm -rf wal parquet_out
