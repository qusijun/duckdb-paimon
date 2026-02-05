#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

class PaimonTableEntry : public TableCatalogEntry {
public:
	PaimonTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, std::string table_path,
	                 shared_ptr<ArrowTableSchema> arrow_table, int32_t num_buckets = -1);

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	/// Number of buckets: -1 for append-only, >0 for fixed-bucket primary key tables.
	int32_t GetNumBuckets() const {
		return num_buckets_;
	}
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                              const EntryLookupInfo &lookup) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	virtual_column_map_t GetVirtualColumns() const override;
	vector<column_t> GetRowIdColumns() const override;

	const string &GetTablePath() const {
		return table_path_;
	}
	const shared_ptr<ArrowTableSchema> &GetArrowTable() const {
		return arrow_table_;
	}

private:
	string table_path_;
	shared_ptr<ArrowTableSchema> arrow_table_;
	int32_t num_buckets_;
};

} // namespace duckdb
