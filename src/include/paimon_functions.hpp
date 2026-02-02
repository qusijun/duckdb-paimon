#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "paimon/api.h"
#include "arrow/c/bridge.h"

namespace duckdb {

/// Bind data for paimon_scan table function
struct PaimonScanBindData : FunctionData {
	std::string table_path;
	shared_ptr<ArrowTableSchema> arrow_table;

	PaimonScanBindData(std::string path, shared_ptr<ArrowTableSchema> arrow_table_p)
	    : table_path(std::move(path)), arrow_table(std::move(arrow_table_p)) {
	}

	~PaimonScanBindData() override = default;

	unique_ptr<FunctionData> Copy() const override;

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<PaimonScanBindData>();
		return table_path == other.table_path;
	}
};

/// Global state for paimon_scan
struct PaimonScanGlobalState final : GlobalTableFunctionState {
	std::unique_ptr<paimon::TableRead> table_read;
	std::unique_ptr<paimon::BatchReader> batch_reader;
	std::vector<std::shared_ptr<paimon::Split>> splits;
	idx_t current_split_idx = 0;
	bool done = false;

	PaimonScanGlobalState() = default;
};

/// Local state for paimon_scan
struct PaimonScanLocalState : LocalTableFunctionState {
	shared_ptr<ArrowArrayWrapper> current_batch;
	idx_t chunk_offset = 0;
	/// output_col_idx -> index in batch ArrowArray.children (by name, to skip Paimon internal columns)
	vector<idx_t> batch_column_indices;

	PaimonScanLocalState() = default;

	~PaimonScanLocalState() override = default;
};

class PaimonFunctions {
public:
	static TableFunctionSet GetPaimonScanFunction();
	/// Table function used by PaimonTableEntry::GetScanFunction
	static TableFunction GetPaimonScanTableFunction();
};

/// Build ArrowTableSchema from Arrow C schema
shared_ptr<ArrowTableSchema> PaimonArrowTableSchemaFromArrowSchema(ClientContext &context,
                                                                   const ArrowSchema &arrow_schema);

} // namespace duckdb
