CXX      ?= c++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -O3
LDFLAGS  ?= -pthread

CORE := demo.cpp shm_manager.cpp storage_tailer.cpp
HDRS := config.h md.h shm_layout.h shm_manager.h strategy_reader.h demo.h storage_tailer.h

.PHONY: all test clean

# Resident server (default = small/laptop config; add PROD=1 for production sizes).
all: tick_server
tick_server: $(CORE) main.cpp $(HDRS)
	$(CXX) $(CXXFLAGS) $(if $(PROD),-DPROD,) $(CORE) main.cpp $(LDFLAGS) -o $@

# Functional + concurrent tests.
test: tick_tests
	./tick_tests
tick_tests: $(CORE) tests.cpp $(HDRS)
	$(CXX) $(CXXFLAGS) $(CORE) tests.cpp $(LDFLAGS) -o $@

clean:
	rm -f tick_server tick_tests
	rm -rf wal test_wal test_wal_concurrent
