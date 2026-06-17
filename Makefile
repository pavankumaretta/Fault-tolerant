.PHONY: configure build test run demo clean format

configure:
	cmake -S . -B build -DCMAKE_BUILD_TYPE=Release

build: configure
	cmake --build build -j

test: build
	ctest --test-dir build --output-on-failure
	python3 tests/integration_test.py --binary ./build/kvstore_server

run: build
	KV_API_KEY=dev-secret ./build/kvstore_server

demo: build
	bash scripts/demo.sh

clean:
	rm -rf build data demo-data

format:
	clang-format -i include/kv/*.hpp src/*.cpp tests/*.cpp
