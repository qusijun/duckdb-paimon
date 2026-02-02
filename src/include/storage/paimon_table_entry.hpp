#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

class PaimonTableEntry : public TableCatalogEntry {
public:
	PaimonTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, std::string table_path,
	                 shared_ptr<ArrowTableSchema> arrow_table);

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                              const EntryLookupInfo &lookup) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	virtual_column_map_t GetVirtualColumns() const override;
	vector<column_t> GetRowIdColumns() const override;

private:
	std::string table_path_;
	shared_ptr<ArrowTableSchema> arrow_table_;
};

} // namespace duckdb
