PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=paimon
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Pre-build paimon-cpp and arrow-cpp before building the extension
.PHONY: prebuild-paimon-cpp prebuild-arrow-cpp

prebuild-paimon-cpp:
	@echo "Building paimon-cpp and its dependencies..."
	@mkdir -p submodules/paimon-cpp/build
	@mkdir -p output
	@if [ -z "${CMAKE_BUILD_TYPE}" ]; then \
		echo "CMAKE_BUILD_TYPE is not set, defaulting to Release"; \
		BUILD_TYPE="Release"; \
	else \
		BUILD_TYPE="${CMAKE_BUILD_TYPE}"; \
	fi; \
	cd submodules/paimon-cpp/build && cmake .. -DCMAKE_BUILD_TYPE=$$BUILD_TYPE -DPAIMON_BUILD_SHARED=ON -DPAIMON_BUILD_STATIC=OFF -DPAIMON_ENABLE_LUMINA=OFF -DCMAKE_INSTALL_PREFIX=../../output/paimon-cpp
	@$(MAKE) -C submodules/paimon-cpp/build -j16
	@$(MAKE) -C submodules/paimon-cpp/build install
	@echo "paimon-cpp build and install completed."

prebuild-arrow-cpp:
	@echo "Building arrow-cpp and its dependencies..."
	@mkdir -p submodules/arrow/cpp/build
	@mkdir -p output
	@if [ -z "${CMAKE_BUILD_TYPE}" ]; then \
		echo "CMAKE_BUILD_TYPE is not set, defaulting to Release"; \
		BUILD_TYPE="Release"; \
	else \
		BUILD_TYPE="${CMAKE_BUILD_TYPE}"; \
	fi; \
	cd submodules/arrow/cpp/build && cmake .. -DCMAKE_BUILD_TYPE=$$BUILD_TYPE -DARROW_BUILD_SHARED=ON -DARROW_BUILD_STATIC=OFF -DCMAKE_INSTALL_PREFIX=../../../output/arrow-cpp -Dxsimd_SOURCE=BUNDLED
	@$(MAKE) -C submodules/arrow/cpp/build -j16
	@$(MAKE) -C submodules/arrow/cpp/build install
	@echo "arrow-cpp build and install completed."


include extension-ci-tools/makefiles/duckdb_extension.Makefile

debug: prebuild-paimon-cpp prebuild-arrow-cpp
	@echo "Building extension in debug mode..."
	@$(MAKE) -f extension-ci-tools/makefiles/duckdb_extension.Makefile debug PROJ_DIR="$(CURDIR)/" EXT_CONFIG="$(PROJ_DIR)extension_config.cmake" EXT_NAME="$(EXT_NAME)"

release: prebuild-paimon-cpp prebuild-arrow-cpp
	@echo "Building extension in release mode..."
	@$(MAKE) -f extension-ci-tools/makefiles/duckdb_extension.Makefile release PROJ_DIR="$(CURDIR)/" EXT_CONFIG="$(PROJ_DIR)extension_config.cmake" EXT_NAME="$(EXT_NAME)"

ifneq ($(EXTENSION_SQL_TEST_PATTERN),)
test_release_internal:
	./build/release/$(TEST_PATH) --require $(EXT_NAME) "$(EXTENSION_SQL_TEST_PATTERN)"
test_debug_internal:
	./build/debug/$(TEST_PATH) --require $(EXT_NAME) "$(EXTENSION_SQL_TEST_PATTERN)"
endif