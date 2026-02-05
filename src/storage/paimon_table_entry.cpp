#include "arrow/status.h"
#include "storage/paimon_table_entry.hpp"
#include "storage/paimon_catalog.hpp"
#include "paimon_functions.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

PaimonTableEntry::PaimonTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                   std::string table_path, shared_ptr<ArrowTableSchema> arrow_table,
                                   int32_t num_buckets)
    : TableCatalogEntry(catalog, schema, info), table_path_(std::move(table_path)),
      arrow_table_(std::move(arrow_table)), num_buckets_(num_buckets) {
}

unique_ptr<BaseStatistics> PaimonTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	// TODO: support Statistics
	return nullptr;
}

TableFunction PaimonTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	bind_data = make_uniq<PaimonScanBindData>(table_path_, arrow_table_);
	return PaimonFunctions::GetPaimonScanTableFunction();
}

TableFunction PaimonTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                const EntryLookupInfo &lookup) {
	return GetScanFunction(context, bind_data);
}

TableStorageInfo PaimonTableEntry::GetStorageInfo(ClientContext &context) {
	// TODO: support storage info
	return TableStorageInfo();
}

virtual_column_map_t PaimonTableEntry::GetVirtualColumns() const {
	// TODO: support virtual columns
	return virtual_column_map_t();
}

vector<column_t> PaimonTableEntry::GetRowIdColumns() const {
	// TODO: support row id columns
	return vector<column_t>();
}

} // namespace duckdb
