# Validation Results

Validation was run locally on June 17, 2026 with GCC 14.2, CMake 3.31, SQLite 3.46, and Python 3.13.

## Build

```text
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j4
Result: successful
```

## C++ unit tests

```text
ctest --test-dir build --output-on-failure
1/1 tests passed
```

Covered CRUD, deterministic sharding, leader failover, majority loss, recovery, and restart persistence.

## End-to-end HTTP test

```text
python3 tests/integration_test.py --binary ./build/kvstore_server
Integration test passed: auth, CRUD, failover, quorum, recovery, metrics.
```

## Sanitizers

```text
cmake -S . -B build-san -DCMAKE_BUILD_TYPE=Debug -DENABLE_SANITIZERS=ON
cmake --build build-san -j4
ASAN_OPTIONS=detect_leaks=1 ctest --test-dir build-san --output-on-failure
1/1 tests passed
```
