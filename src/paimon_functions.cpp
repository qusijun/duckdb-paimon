#include "paimon_functions.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow/arrow_type_info.hpp"
#include "duckdb/main/config.hpp"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/read_context.h"
#include "paimon/table/source/table_read.h"
#include "paimon/reader/batch_reader.h"
#include "arrow/c/helpers.h"
#include "duckdb/function/table/arrow.hpp"

namespace duckdb {

unique_ptr<FunctionData> PaimonScanBindData::Copy() const {
	auto copied_schema = make_shared_ptr<ArrowTableSchema>();
	const auto &columns = arrow_table->GetColumns();
	const auto &names = arrow_table->GetNames();
	for (idx_t i = 0; i < names.size(); i++) {
		if (auto it = columns.find(i); it != columns.end()) {
			copied_schema->AddColumn(i, it->second, names[i]);
		}
	}
	return make_uniq<PaimonScanBindData>(table_path, std::move(copied_schema));
}

static void PopulateArrowTableSchemaFromPaimon(DBConfig &config, ArrowTableSchema &arrow_table,
                                               const ArrowSchema &arrow_schema) {
	if (!arrow_schema.release) {
		throw InternalException("arrow_schema has been released");
	}
	for (idx_t col_idx = 0; col_idx < static_cast<idx_t>(arrow_schema.n_children); col_idx++) {
		auto &schema = *arrow_schema.children[col_idx];
		if (!schema.release) {
			throw InvalidInputException("paimon_scan: released schema passed");
		}
		auto arrow_type = ArrowType::GetArrowLogicalType(config, schema);
		auto name = string(schema.name);
		arrow_table.AddColumn(col_idx, std::move(arrow_type), name);
	}
}

shared_ptr<ArrowTableSchema> PaimonArrowTableSchemaFromArrowSchema(ClientContext &context,
                                                                   const ArrowSchema &arrow_schema) {
	auto arrow_table = make_shared_ptr<ArrowTableSchema>();
	PopulateArrowTableSchemaFromPaimon(DBConfig::GetConfig(context), *arrow_table, arrow_schema);
	return arrow_table;
}

unique_ptr<FunctionData> PaimonScanBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	// Path-based scan is not supported; schema is only loaded via Catalog (ATTACH + catalog.schema.table).
	throw BinderException(
	    "paimon_scan(path) is not supported. Use ATTACH 'warehouse_path' AS catalog_name (TYPE paimon); "
	    "then SELECT * FROM catalog_name.schema_name.table_name;");
}

unique_ptr<GlobalTableFunctionState> PaimonScanInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<PaimonScanBindData>();
	auto result = make_uniq<PaimonScanGlobalState>();

	std::vector<std::shared_ptr<paimon::Split>> splits {};
	std::map<std::string, std::string> scan_options = {
	    {paimon::Options::FILE_SYSTEM, "local"},
	    {paimon::Options::MANIFEST_FORMAT, "avro"},
	};
	paimon::ScanContextBuilder scan_context_builder(bind_data.table_path);
	auto scan_context_result = scan_context_builder.SetOptions(scan_options).Finish();
	if (!scan_context_result.ok()) {
		throw InvalidInputException("Failed to create scan context: " + scan_context_result.status().ToString());
	}
	auto table_scan_result = paimon::TableScan::Create(std::move(scan_context_result).value());
	if (!table_scan_result.ok()) {
		throw InvalidInputException("Failed to create table scan: " + table_scan_result.status().ToString());
	}
	auto table_scan = std::move(table_scan_result).value();
	auto plan_result = table_scan->CreatePlan();
	if (!plan_result.ok()) {
		throw InvalidInputException("Failed to create scan plan: " + plan_result.status().ToString());
	}
	splits = std::move(plan_result).value()->Splits();
	if (splits.empty()) {
		result->done = true;
		return std::move(result);
	}

	// Create read context
	std::map<std::string, std::string> options = {{paimon::Options::FILE_SYSTEM, "local"}};
	paimon::ReadContextBuilder read_context_builder(bind_data.table_path);
	auto read_context_result = read_context_builder.SetOptions(options).Finish();
	if (!read_context_result.ok()) {
		throw InvalidInputException("Failed to create read context: " + read_context_result.status().ToString());
	}
	// Create table read
	auto table_read_result = paimon::TableRead::Create(std::move(read_context_result).value());
	if (!table_read_result.ok()) {
		throw InvalidInputException("Failed to create table read: " + table_read_result.status().ToString());
	}
	result->table_read = std::move(table_read_result).value();

	// Create batch reader with all splits
	auto reader_result = result->table_read->CreateReader(splits);
	if (!reader_result.ok()) {
		throw InvalidInputException("Failed to create batch reader: " + reader_result.status().ToString());
	}
	result->batch_reader = std::move(reader_result).value();
	result->splits = std::move(splits);

	return result;
}

unique_ptr<LocalTableFunctionState> PaimonScanLocalInit(ExecutionContext &context, TableFunctionInitInput &input,
                                                        GlobalTableFunctionState *global_state_p) {
	return make_uniq<PaimonScanLocalState>();
}

void PaimonScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	if (!data_p.local_state) {
		return;
	}
	auto &bind_data = data_p.bind_data->CastNoConst<PaimonScanBindData>();
	auto &state = data_p.local_state->Cast<PaimonScanLocalState>();
	auto &global_state = data_p.global_state->Cast<PaimonScanGlobalState>();
	if (global_state.done) {
		return;
	}
	if (!state.current_batch || state.chunk_offset >= static_cast<idx_t>(state.current_batch->arrow_array.length) ||
	    !state.current_batch->arrow_array.release) {
		// Get next batch
		if (!global_state.batch_reader) {
			global_state.done = true;
			return;
		}

		auto batch_result = global_state.batch_reader->NextBatch();
		if (!batch_result.ok()) {
			throw InvalidInputException("Failed to read batch: " + batch_result.status().ToString());
		}
		auto batch = std::move(batch_result).value();

		if (paimon::BatchReader::IsEofBatch(batch)) {
			global_state.done = true;
			return;
		}

		// Release old batch if exists
		state.current_batch.reset();

		// Move schema to a temporary RAII wrapper for automatic release
		ArrowSchemaWrapper schema_wrapper;
		ArrowSchemaMove(batch.second.get(), &schema_wrapper.arrow_schema);

		// Build output_col_idx -> batch.children index by name (batch may have Paimon internal columns)
		if (state.batch_column_indices.empty()) {
			const auto &schema_names = bind_data.arrow_table->GetNames();
			state.batch_column_indices.reserve(schema_names.size());
			const int64_t n_batch = schema_wrapper.arrow_schema.n_children;
			for (idx_t col_idx = 0; col_idx < schema_names.size(); col_idx++) {
				const string &want_name = schema_names[col_idx];
				idx_t batch_idx = NumericCast<idx_t>(n_batch);
				for (int64_t k = 0; k < n_batch; k++) {
					const char *batch_name = schema_wrapper.arrow_schema.children[k]->name;
					if (batch_name && string(batch_name) == want_name) {
						batch_idx = NumericCast<idx_t>(k);
						break;
					}
				}
				if (batch_idx >= static_cast<idx_t>(n_batch)) {
					throw InvalidInputException("Paimon batch schema missing column '%s'", want_name.c_str());
				}
				state.batch_column_indices.push_back(batch_idx);
			}
		}

		// Transfer array wrapper to shared_ptr for state management
		state.current_batch = make_shared_ptr<ArrowArrayWrapper>();
		ArrowArrayMove(batch.first.get(), &state.current_batch->arrow_array);

		state.chunk_offset = 0;
	}

	const auto output_size = MinValue<idx_t>(
	    STANDARD_VECTOR_SIZE, NumericCast<idx_t>(state.current_batch->arrow_array.length) - state.chunk_offset);
	if (output_size == 0) {
		return;
	}
	output.SetCardinality(output_size);

	// Convert each column using ArrowToDuckDBConversion (use batch_column_indices to skip internal columns)
	auto &arrow_types = bind_data.arrow_table->GetColumns();
	ArrowArrayScanState array_state(context);
	array_state.owned_data = state.current_batch;
	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		idx_t batch_idx = state.batch_column_indices[col_idx];
		const auto &parent_array = state.current_batch->arrow_array;
		auto &array = *parent_array.children[batch_idx];
		if (!array.release) {
			throw InvalidInputException("paimon_scan: released array passed");
		}

		D_ASSERT(arrow_types.find(col_idx) != arrow_types.end());

		switch (auto &arrow_type = *arrow_types.at(col_idx); arrow_type.GetPhysicalType()) {
		case ArrowArrayPhysicalType::DICTIONARY_ENCODED:
			ArrowToDuckDBConversion::ColumnArrowToDuckDBDictionary(output.data[col_idx], array, state.chunk_offset,
			                                                       array_state, output_size, arrow_type);
			break;
		case ArrowArrayPhysicalType::RUN_END_ENCODED:
			ArrowToDuckDBConversion::ColumnArrowToDuckDBRunEndEncoded(output.data[col_idx], array, state.chunk_offset,
			                                                          array_state, output_size, arrow_type);
			break;
		case ArrowArrayPhysicalType::DEFAULT:
			ArrowToDuckDBConversion::SetValidityMask(output.data[col_idx], array, state.chunk_offset, output_size,
			                                         parent_array.offset, -1);
			ArrowToDuckDBConversion::ColumnArrowToDuckDB(output.data[col_idx], array, state.chunk_offset, array_state,
			                                             output_size, arrow_type);
			break;
		default:
			throw NotImplementedException("Unsupported Arrow physical type");
		}
	}
	state.chunk_offset += output_size;
	output.Verify();
}

TableFunctionSet PaimonFunctions::GetPaimonScanFunction() {
	return TableFunctionSet(GetPaimonScanTableFunction());
}

TableFunction PaimonFunctions::GetPaimonScanTableFunction() {
	return TableFunction("paimon_scan", {LogicalType::VARCHAR}, PaimonScanFunction, PaimonScanBind, PaimonScanInit,
	                     PaimonScanLocalInit);
}

} // namespace duckdb
