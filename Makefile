PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=paimon
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# CMAKE_BUILD_TYPE: use target default, or override via: make debug CMAKE_BUILD_TYPE=RelWithDebInfo
# Target-specific vars ensure prebuild uses Debug/Release when invoked via make debug/release
CMAKE_BUILD_TYPE ?= Release

# Pre-build paimon-cpp and arrow-cpp before building the extension
.PHONY: prebuild-paimon-cpp prebuild-arrow-cpp

prebuild-paimon-cpp:
	@echo "Building paimon-cpp (CMAKE_BUILD_TYPE=$(CMAKE_BUILD_TYPE))..."
	@mkdir -p submodules/paimon-cpp/build
	@cd submodules/paimon-cpp/build && cmake .. -DCMAKE_BUILD_TYPE=$(CMAKE_BUILD_TYPE) -DPAIMON_BUILD_SHARED=ON -DPAIMON_BUILD_STATIC=OFF -DPAIMON_ENABLE_LUMINA=OFF -DPAIMON_ENABLE_LUCENE=OFF -DCMAKE_INSTALL_PREFIX=../../output/paimon-cpp
	@$(MAKE) -C submodules/paimon-cpp/build -j16
	@$(MAKE) -C submodules/paimon-cpp/build install
	@echo "paimon-cpp build and install completed."

prebuild-arrow-cpp:
	@echo "Building arrow-cpp (CMAKE_BUILD_TYPE=$(CMAKE_BUILD_TYPE))..."
	@mkdir -p submodules/arrow/cpp/build
	@cd submodules/arrow/cpp/build && cmake .. -DCMAKE_BUILD_TYPE=$(CMAKE_BUILD_TYPE) -DARROW_BUILD_SHARED=ON -DARROW_BUILD_STATIC=OFF -DCMAKE_INSTALL_PREFIX=../../../output/arrow-cpp -Dxsimd_SOURCE=BUNDLED
	@$(MAKE) -C submodules/arrow/cpp/build -j16
	@$(MAKE) -C submodules/arrow/cpp/build install
	@echo "arrow-cpp build and install completed."


include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Target-specific CMAKE_BUILD_TYPE so prebuild uses correct type (?= allows override)
debug: CMAKE_BUILD_TYPE?=Debug
debug: prebuild-paimon-cpp prebuild-arrow-cpp
	@echo "Building extension in debug mode..."
	@$(MAKE) -f extension-ci-tools/makefiles/duckdb_extension.Makefile debug PROJ_DIR="$(CURDIR)/" EXT_CONFIG="$(PROJ_DIR)extension_config.cmake" EXT_NAME="$(EXT_NAME)"

release: CMAKE_BUILD_TYPE?=Release
release: prebuild-paimon-cpp prebuild-arrow-cpp
	@echo "Building extension in release mode..."
	@$(MAKE) -f extension-ci-tools/makefiles/duckdb_extension.Makefile release PROJ_DIR="$(CURDIR)/" EXT_CONFIG="$(PROJ_DIR)extension_config.cmake" EXT_NAME="$(EXT_NAME)"

test_release_internal:
	@if [ -z "$(EXTENSION_SQL_TEST_PATTERN)" ]; then \
		echo "Running default paimon tests..."; \
		./build/release/$(TEST_PATH) --require $(EXT_NAME) "test/sql/paimon.test" && \
		./build/release/$(TEST_PATH) --require $(EXT_NAME) "test/sql/paimon_scan.test" && \
		./build/release/$(TEST_PATH) --require $(EXT_NAME) "test/sql/paimon_create_table.test" && \
		./build/release/$(TEST_PATH) --require $(EXT_NAME) "test/sql/paimon_insert.test"; \
	else \
		./build/release/$(TEST_PATH) --require $(EXT_NAME) "$(EXTENSION_SQL_TEST_PATTERN)"; \
	fi
test_debug_internal:
	@if [ -z "$(EXTENSION_SQL_TEST_PATTERN)" ]; then \
		echo "Running default paimon tests..."; \
		./build/debug/$(TEST_PATH) --require $(EXT_NAME) "test/sql/paimon.test" && \
		./build/debug/$(TEST_PATH) --require $(EXT_NAME) "test/sql/paimon_scan.test" && \
		./build/debug/$(TEST_PATH) --require $(EXT_NAME) "test/sql/paimon_create_table.test" && \
		./build/debug/$(TEST_PATH) --require $(EXT_NAME) "test/sql/paimon_insert.test"; \
	else \
		./build/debug/$(TEST_PATH) --require $(EXT_NAME) "$(EXTENSION_SQL_TEST_PATTERN)"; \
	fi