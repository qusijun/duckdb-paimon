#include "paimon_scan.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow/arrow_type_info.hpp"
#include "duckdb/main/config.hpp"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/read_context.h"
#include "paimon/table/source/table_read.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/literal.h"

#include "arrow/c/helpers.h"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"

namespace duckdb {

namespace {

// Map DuckDB LogicalType to Paimon FieldType (subset used for predicate pushdown).
paimon::FieldType PaimonFieldTypeFromLogical(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return paimon::FieldType::BOOLEAN;
	case LogicalTypeId::TINYINT:
		return paimon::FieldType::TINYINT;
	case LogicalTypeId::SMALLINT:
		return paimon::FieldType::SMALLINT;
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
		// map smaller ints to INT
		return paimon::FieldType::INT;
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return paimon::FieldType::BIGINT;
	case LogicalTypeId::FLOAT:
		return paimon::FieldType::FLOAT;
	case LogicalTypeId::DOUBLE:
		return paimon::FieldType::DOUBLE;
	case LogicalTypeId::DECIMAL:
		return paimon::FieldType::DECIMAL;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::CHAR:
		return paimon::FieldType::STRING;
	case LogicalTypeId::BLOB:
		return paimon::FieldType::BLOB;
	case LogicalTypeId::DATE:
		return paimon::FieldType::DATE;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_NS:
		return paimon::FieldType::TIMESTAMP;
	default:
		return paimon::FieldType::UNKNOWN;
	}
}

// Convert DuckDB Value -> Paimon Literal for simple scalar types.
std::optional<paimon::Literal> PaimonLiteralFromValue(const Value &val, paimon::FieldType field_type) {
	if (val.IsNull()) {
		return paimon::Literal(field_type);
	}
	switch (field_type) {
	case paimon::FieldType::BOOLEAN: {
		return paimon::Literal(val.GetValue<bool>());
	}
	case paimon::FieldType::TINYINT: {
		return paimon::Literal(val.GetValue<int8_t>());
	}
	case paimon::FieldType::SMALLINT: {
		return paimon::Literal(val.GetValue<int16_t>());
	}
	case paimon::FieldType::INT: {
		return paimon::Literal(val.GetValue<int32_t>());
	}
	case paimon::FieldType::BIGINT: {
		return paimon::Literal(val.GetValue<int64_t>());
	}
	case paimon::FieldType::FLOAT: {
		return paimon::Literal(val.GetValue<float>());
	}
	case paimon::FieldType::DOUBLE: {
		return paimon::Literal(val.GetValue<double>());
	}
	case paimon::FieldType::STRING: {
		auto str = val.ToString();
		return paimon::Literal(paimon::FieldType::STRING, str.c_str(), str.size());
	}
	case paimon::FieldType::DATE: {
		// DuckDB stores date as days since epoch
		auto d = val.GetValue<date_t>();
		return paimon::Literal(paimon::FieldType::DATE, d.days);
	}
	case paimon::FieldType::TIMESTAMP: {
		// Store as raw int64 ticks; semantics are interpreted by Paimon schema
		auto ts = val.GetValue<timestamp_t>();
		return paimon::Literal(ts.value);
	}
	case paimon::FieldType::DECIMAL:
		// For now, fall back to string representation
		// to avoid dealing with precision/scale mismatch.
		// This still allows using indexes built over string-encoded decimals.
		{
			auto str = val.ToString();
			return paimon::Literal(paimon::FieldType::STRING, str.c_str(), str.size());
		}
	default:
		return std::nullopt;
	}
}

// Build a single-column Paimon predicate from a DuckDB TableFilter.
std::shared_ptr<paimon::Predicate> BuildPaimonPredicateForFilter(idx_t duck_col_idx, const string &field_name,
                                                                 const LogicalType &field_type,
                                                                 const TableFilter &filter) {
	auto paimon_type = PaimonFieldTypeFromLogical(field_type);
	if (paimon_type == paimon::FieldType::UNKNOWN) {
		return nullptr;
	}

	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		const auto &const_filter = filter.Cast<ConstantFilter>();
		auto literal_opt = PaimonLiteralFromValue(const_filter.constant, paimon_type);
		if (!literal_opt) {
			return nullptr;
		}
		const auto &literal = *literal_opt;
		switch (const_filter.comparison_type) {
		case ExpressionType::COMPARE_EQUAL:
			return paimon::PredicateBuilder::Equal(NumericCast<int32_t>(duck_col_idx), field_name, paimon_type,
			                                       literal);
		case ExpressionType::COMPARE_NOTEQUAL:
			return paimon::PredicateBuilder::NotEqual(NumericCast<int32_t>(duck_col_idx), field_name, paimon_type,
			                                          literal);
		case ExpressionType::COMPARE_LESSTHAN:
			return paimon::PredicateBuilder::LessThan(NumericCast<int32_t>(duck_col_idx), field_name, paimon_type,
			                                          literal);
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			return paimon::PredicateBuilder::LessOrEqual(NumericCast<int32_t>(duck_col_idx), field_name, paimon_type,
			                                             literal);
		case ExpressionType::COMPARE_GREATERTHAN:
			return paimon::PredicateBuilder::GreaterThan(NumericCast<int32_t>(duck_col_idx), field_name, paimon_type,
			                                             literal);
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return paimon::PredicateBuilder::GreaterOrEqual(NumericCast<int32_t>(duck_col_idx), field_name, paimon_type,
			                                                literal);
		default:
			return nullptr;
		}
	}
	case TableFilterType::IS_NULL: {
		return paimon::PredicateBuilder::IsNull(NumericCast<int32_t>(duck_col_idx), field_name, paimon_type);
	}
	case TableFilterType::IS_NOT_NULL: {
		return paimon::PredicateBuilder::IsNotNull(NumericCast<int32_t>(duck_col_idx), field_name, paimon_type);
	}
	case TableFilterType::IN_FILTER: {
		const auto &in_filter = filter.Cast<InFilter>();
		vector<paimon::Literal> literals;
		literals.reserve(in_filter.values.size());
		for (auto &v : in_filter.values) {
			auto lit_opt = PaimonLiteralFromValue(v, paimon_type);
			if (!lit_opt) {
				// If any IN element cannot be mapped, skip pushdown for this filter.
				return nullptr;
			}
			literals.emplace_back(*lit_opt);
		}
		return paimon::PredicateBuilder::In(NumericCast<int32_t>(duck_col_idx), field_name, paimon_type, literals);
	}
	default:
		// Other filter types (OR, struct, dynamic, expression) are not pushed down for now.
		return nullptr;
	}
}

// Build combined Paimon predicate (AND of per-column filters) from DuckDB TableFilterSet.
std::shared_ptr<paimon::Predicate> BuildPaimonPredicateFromFilters(const PaimonScanBindData &bind_data,
                                                                   optional_ptr<TableFilterSet> filters) {
	if (!filters || filters->filters.empty()) {
		return nullptr;
	}

	auto &names = bind_data.arrow_table->GetNames();
	auto &types = bind_data.arrow_table->GetTypes();

	std::vector<std::shared_ptr<paimon::Predicate>> predicates;
	predicates.reserve(filters->filters.size());

	for (auto &entry : filters->filters) {
		auto duck_col_idx = entry.first;
		if (duck_col_idx >= names.size() || duck_col_idx >= types.size()) {
			continue;
		}
		const auto &field_name = names[duck_col_idx];
		const auto &field_type = types[duck_col_idx];
		auto &filter = *entry.second;

		auto pred = BuildPaimonPredicateForFilter(duck_col_idx, field_name, field_type, filter);
		if (pred) {
			predicates.push_back(std::move(pred));
		}
	}

	if (predicates.empty()) {
		return nullptr;
	}
	if (predicates.size() == 1) {
		return predicates[0];
	}
	auto and_result = paimon::PredicateBuilder::And(predicates);
	if (!and_result.ok()) {
		return nullptr;
	}
	return and_result.value();
}

} // namespace

unique_ptr<FunctionData> PaimonScanBindData::Copy() const {
	auto copied_schema = make_shared_ptr<ArrowTableSchema>();
	const auto &columns = arrow_table->GetColumns();
	const auto &names = arrow_table->GetNames();
	for (idx_t i = 0; i < names.size(); i++) {
		if (auto it = columns.find(i); it != columns.end()) {
			copied_schema->AddColumn(i, it->second, names[i]);
		}
	}
	auto result = make_uniq<PaimonScanBindData>(table_path, std::move(copied_schema));
	if (filters) {
		auto copied_filters = filters->Copy();
		result->filters.reset(copied_filters.release());
	}
	return std::move(result);
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
	auto &bind_data = input.bind_data->CastNoConst<PaimonScanBindData>();
	auto result = make_uniq<PaimonScanGlobalState>();

	std::vector<std::shared_ptr<paimon::Split>> splits {};

	// Build Paimon predicate from DuckDB table filters for file index & format-level pushdown.
	std::shared_ptr<paimon::Predicate> paimon_predicate = BuildPaimonPredicateFromFilters(bind_data, input.filters);
	// Keep a copy of filters so we can enforce them on the output chunks for full correctness.
	if (input.filters) {
		auto copied_filters = input.filters->Copy();
		bind_data.filters.reset(copied_filters.release());
	}

	std::map<std::string, std::string> scan_options = {
	    {paimon::Options::FILE_SYSTEM, "local"},
	    {paimon::Options::MANIFEST_FORMAT, "avro"},
	    // Ensure file index is enabled for scan planning.
	    {paimon::Options::FILE_INDEX_READ_ENABLED, "true"},
	};
	paimon::ScanContextBuilder scan_context_builder(bind_data.table_path);
	scan_context_builder.SetOptions(scan_options);
	if (paimon_predicate) {
		scan_context_builder.SetPredicate(paimon_predicate);
	}
	auto scan_context_result = scan_context_builder.Finish();
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
	std::map<std::string, std::string> options = {
	    {paimon::Options::FILE_SYSTEM, "local"},
	    {paimon::Options::FILE_INDEX_READ_ENABLED, "true"},
	};
	paimon::ReadContextBuilder read_context_builder(bind_data.table_path);
	read_context_builder.SetOptions(options);
	if (paimon_predicate) {
		read_context_builder.SetPredicate(paimon_predicate);
		read_context_builder.EnablePredicateFilter(true);
	}
	auto read_context_result = read_context_builder.Finish();
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

	// Apply DuckDB filters (TableFilterSet) on the produced chunk for full correctness.
	if (bind_data.filters && !bind_data.filters->filters.empty() && output.size() > 0) {
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		idx_t sel_count = 0;
		for (idx_t row_idx = 0; row_idx < output.size(); row_idx++) {
			bool keep = true;
			for (auto &entry : bind_data.filters->filters) {
				auto col_idx = entry.first;
				if (col_idx >= output.ColumnCount()) {
					continue;
				}
				auto &filter = *entry.second;
				Value val = output.data[col_idx].GetValue(row_idx);

				switch (filter.filter_type) {
				case TableFilterType::CONSTANT_COMPARISON: {
					auto &const_filter = filter.Cast<ConstantFilter>();
					if (!const_filter.Compare(val)) {
						keep = false;
					}
					break;
				}
				case TableFilterType::IS_NULL: {
					if (!val.IsNull()) {
						keep = false;
					}
					break;
				}
				case TableFilterType::IS_NOT_NULL: {
					if (val.IsNull()) {
						keep = false;
					}
					break;
				}
				case TableFilterType::IN_FILTER: {
					auto &in_filter = filter.Cast<InFilter>();
					bool match = false;
					for (auto &candidate : in_filter.values) {
						if (Value::NotDistinctFrom(val, candidate)) {
							match = true;
							break;
						}
					}
					if (!match) {
						keep = false;
					}
					break;
				}
				default:
					// For now we don't support other filter types here; conservatively keep the row.
					break;
				}

				if (!keep) {
					break;
				}
			}
			if (keep) {
				sel.set_index(sel_count++, row_idx);
			}
		}
		if (sel_count == 0) {
			output.SetCardinality(0);
		} else if (sel_count < output.size()) {
			output.Slice(sel, sel_count);
		}
	}

	output.Verify();
}

TableFunctionSet PaimonFunctions::GetPaimonScanFunction() {
	return TableFunctionSet(GetPaimonScanTableFunction());
}

TableFunction PaimonFunctions::GetPaimonScanTableFunction() {
	auto fun = TableFunction("paimon_scan", {LogicalType::VARCHAR}, PaimonScanFunction, PaimonScanBind, PaimonScanInit,
	                         PaimonScanLocalInit);
	// Enable filter pushdown so DuckDB provides TableFilterSet to PaimonScanInit.
	fun.filter_pushdown = true;
	// We keep projection_pushdown disabled for now to avoid changing output schema handling.
	return fun;
}

} // namespace duckdb
