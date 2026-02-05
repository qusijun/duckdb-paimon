#include "arrow/status.h"
#include "arrow/c/helpers.h"
#include "storage/paimon_schema_entry.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "storage/paimon_catalog.hpp"
#include "storage/paimon_table_entry.hpp"
#include "paimon_functions.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/enums/on_create_conflict.hpp"
#include "duckdb/common/string_util.hpp"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/defs.h"
#include "arrow/c/abi.h"

namespace duckdb {

PaimonSchemaEntry::PaimonSchemaEntry(Catalog &catalog, CreateSchemaInfo &info) : SchemaCatalogEntry(catalog, info) {
}

PaimonSchemaEntry::~PaimonSchemaEntry() = default;

static void ExtractPrimaryKeys(const CreateTableInfo &create_info, vector<string> &primary_keys) {
	for (const auto &constraint : create_info.constraints) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique_c = constraint->Cast<UniqueConstraint>();
		if (!unique_c.IsPrimaryKey()) {
			continue;
		}
		const auto &column_names = unique_c.GetColumnNames();
		if (!column_names.empty()) {
			for (const auto &cn : column_names) {
				primary_keys.push_back(cn);
			}
		} else if (unique_c.HasIndex()) {
			auto idx = unique_c.GetIndex();
			if (idx.index < create_info.columns.LogicalColumnCount()) {
				primary_keys.push_back(create_info.columns.GetColumn(LogicalIndex(idx.index)).Name());
			}
		}
		break; // Only one primary key constraint
	}
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &create_info = info.Base();
	auto &paimon_catalog = catalog.Cast<PaimonCatalog>();
	paimon::Catalog *pc = paimon_catalog.GetPaimonCatalog();

	// Ensure database exists
	std::map<std::string, std::string> db_options = {
	    {paimon::Options::FILE_SYSTEM, "local"},
	    {paimon::Options::MANIFEST_FORMAT, "avro"},
	};
	auto create_db_status = pc->CreateDatabase(name, db_options, /*ignore_if_exists=*/true);
	if (!create_db_status.ok()) {
		throw CatalogException("Failed to create Paimon database '%s': %s", name.c_str(),
		                       create_db_status.ToString().c_str());
	}

	// Build column types and names (physical columns only, exclude generated)
	vector<LogicalType> types;
	vector<string> names;
	for (const auto &col : create_info.columns.Physical()) {
		if (col.Generated()) {
			continue;
		}
		names.push_back(col.Name());
		types.push_back(col.Type());
	}
	if (names.empty()) {
		throw CatalogException("CREATE TABLE: table must have at least one column");
	}

	// Convert to Arrow schema
	ArrowSchemaWrapper arrow_schema_wrapper;
	auto &context = transaction.GetContext();
	auto client_properties = context.GetClientProperties();
	ArrowConverter::ToArrowSchema(&arrow_schema_wrapper.arrow_schema, types, names, client_properties);

	// Extract primary keys
	vector<string> primary_keys;
	ExtractPrimaryKeys(create_info, primary_keys);

	// Partition keys - not supported from DuckDB CREATE TABLE yet
	std::vector<std::string> partition_keys;

	// Table options
	std::map<std::string, std::string> table_options = {
	    {paimon::Options::FILE_SYSTEM, "local"},
	    {paimon::Options::MANIFEST_FORMAT, "avro"},
	    {paimon::Options::FILE_FORMAT, "parquet"},
	};

	// Primary key tables require bucket > 0 (fixed bucket mode); append-only tables use bucket -1
	if (!primary_keys.empty()) {
		table_options[paimon::Options::BUCKET] = "1";
		// bucket-key defaults to primary key when not specified; set explicitly for clarity
		table_options[paimon::Options::BUCKET_KEY] = StringUtil::Join(primary_keys, ",");
	} else {
		table_options[paimon::Options::BUCKET] = "-1";
	}

	bool ignore_if_exists = (create_info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT);
	if (create_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw NotImplementedException("CREATE OR REPLACE TABLE is not supported for Paimon catalog");
	}

	paimon::Identifier identifier(name, create_info.table);
	auto create_table_status = pc->CreateTable(identifier, &arrow_schema_wrapper.arrow_schema, partition_keys,
	                                           primary_keys, table_options, ignore_if_exists);
	if (!create_table_status.ok()) {
		throw CatalogException("Failed to create Paimon table '%s.%s': %s", name.c_str(), create_info.table.c_str(),
		                       create_table_status.ToString().c_str());
	}

	// If ignore_if_exists and table already existed, we need to return the existing entry
	// CreateTable with ignore_if_exists=true returns OK without creating - table may already exist
	// Return the table entry (fetch it)
	return GetTableEntry(context, create_info.table);
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("CREATE VIEW is not supported for Paimon catalog (read-only)");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw NotImplementedException("CREATE SEQUENCE is not supported for Paimon catalog (read-only)");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                  CreateTableFunctionInfo &info) {
	throw NotImplementedException("CREATE TABLE FUNCTION is not supported for Paimon catalog (read-only)");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw NotImplementedException("CREATE FUNCTION is not supported for Paimon catalog (read-only)");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                          TableCatalogEntry &table) {
	throw NotImplementedException("CREATE INDEX is not supported for Paimon catalog (read-only)");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                 CreateCopyFunctionInfo &info) {
	throw NotImplementedException("CREATE COPY FUNCTION is not supported for Paimon catalog (read-only)");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                   CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("CREATE PRAGMA FUNCTION is not supported for Paimon catalog (read-only)");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                              CreateCollationInfo &info) {
	throw NotImplementedException("CREATE COLLATION is not supported for Paimon catalog (read-only)");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("CREATE TYPE is not supported for Paimon catalog (read-only)");
}

void PaimonSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("ALTER is not supported for Paimon catalog (read-only)");
}

void PaimonSchemaEntry::Scan(ClientContext &context, CatalogType type,
                             const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	const auto &paimon_catalog = catalog.Cast<PaimonCatalog>();
	const paimon::Catalog *pc = paimon_catalog.GetPaimonCatalog();
	const auto list_result = pc->ListTables(name);
	if (!list_result.ok()) {
		throw CatalogException("Failed to list Paimon tables in schema '%s': %s", name.c_str(),
		                       list_result.status().ToString().c_str());
	}
	for (const auto &table_name : list_result.value()) {
		if (auto entry = GetTableEntry(context, table_name)) {
			callback(*entry);
		}
	}
}

void PaimonSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Paimon schema Scan without context is not supported");
}

void PaimonSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("DROP is not supported for Paimon catalog (read-only)");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                          const EntryLookupInfo &lookup_info) {
	if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	return GetTableEntry(transaction.GetContext(), lookup_info.GetEntryName());
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::GetTableEntry(ClientContext &context, const string &table_name) {
	const auto &catalog_paimon_wrapper = catalog.Cast<PaimonCatalog>();
	const paimon::Catalog *paimon_catalog = catalog_paimon_wrapper.GetPaimonCatalog();
	const paimon::Identifier identifier(name, table_name);
	const auto exists_result = paimon_catalog->TableExists(identifier);
	if (!exists_result.ok()) {
		throw CatalogException("Paimon table '%s.%s' lookup failed: %s", name.c_str(), table_name.c_str(),
		                       exists_result.status().ToString().c_str());
	}
	if (!exists_result.value()) {
		return nullptr;
	}
	const auto schema_result = paimon_catalog->LoadTableSchema(identifier);
	if (!schema_result.ok()) {
		throw CatalogException("Paimon table '%s.%s' load schema failed: %s", name.c_str(), table_name.c_str(),
		                       schema_result.status().ToString().c_str());
	}
	auto arrow_schema_result = schema_result.value()->GetArrowSchema();
	if (!arrow_schema_result.ok()) {
		throw CatalogException("Paimon table '%s.%s' get Arrow schema failed: %s", name.c_str(), table_name.c_str(),
		                       arrow_schema_result.status().ToString().c_str());
	}
	const auto arrow_schema_ptr = std::move(arrow_schema_result).value();
	if (!arrow_schema_ptr || !arrow_schema_ptr->release) {
		throw CatalogException("Paimon table '%s.%s' invalid Arrow schema", name.c_str(), table_name.c_str());
	}
	// Use RAII wrapper to automatically release Arrow schema
	ArrowSchemaWrapper schema_wrapper;
	ArrowSchemaMove(arrow_schema_ptr.get(), &schema_wrapper.arrow_schema);
	shared_ptr<ArrowTableSchema> arrow_table =
	    PaimonArrowTableSchemaFromArrowSchema(context, schema_wrapper.arrow_schema);
	string table_path = paimon_catalog->GetTableLocation(identifier);
	int32_t num_buckets = schema_result.value()->NumBuckets();

	CreateTableInfo info;
	info.catalog = catalog_paimon_wrapper.GetAttachName();
	info.schema = name;
	info.table = table_name;
	for (idx_t i = 0; i < arrow_table->GetNames().size(); i++) {
		info.columns.AddColumn(ColumnDefinition(arrow_table->GetNames()[i], arrow_table->GetTypes()[i]));
	}

	auto table_entry =
	    make_uniq<PaimonTableEntry>(catalog, *this, info, std::move(table_path), std::move(arrow_table), num_buckets);
	std::lock_guard lock(table_mutex_);
	table_entries_[table_name] = std::move(table_entry);
	return table_entries_[table_name].get();
}

} // namespace duckdb
