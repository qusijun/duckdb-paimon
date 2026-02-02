#include "arrow/status.h"
#include "arrow/c/helpers.h"
#include "storage/paimon_schema_entry.hpp"
#include "storage/paimon_catalog.hpp"
#include "paimon_functions.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"

namespace duckdb {

PaimonSchemaEntry::PaimonSchemaEntry(Catalog &catalog, CreateSchemaInfo &info) : SchemaCatalogEntry(catalog, info) {
}

PaimonSchemaEntry::~PaimonSchemaEntry() = default;

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	throw NotImplementedException("CREATE TABLE is not supported for Paimon catalog (read-only)");
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

	CreateTableInfo info;
	info.catalog = catalog_paimon_wrapper.GetAttachName();
	info.schema = name;
	info.table = table_name;
	for (idx_t i = 0; i < arrow_table->GetNames().size(); i++) {
		info.columns.AddColumn(ColumnDefinition(arrow_table->GetNames()[i], arrow_table->GetTypes()[i]));
	}

	auto table_entry = make_uniq<PaimonTableEntry>(catalog, *this, info, std::move(table_path), std::move(arrow_table));
	std::lock_guard lock(table_mutex_);
	table_entries_[table_name] = std::move(table_entry);
	return table_entries_[table_name].get();
}

} // namespace duckdb
