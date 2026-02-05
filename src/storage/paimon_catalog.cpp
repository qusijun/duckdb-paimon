// Ensure Arrow's RETURN_NOT_OK is defined before any Paimon header (avoids macro conflict).
#include "arrow/status.h"
#include "storage/paimon_catalog.hpp"
#include "storage/paimon_schema_entry.hpp"
#include "storage/paimon_table_entry.hpp"
#include "paimon_insert.hpp"
#include "paimon_storage_extension.hpp"
#include "paimon/catalog/catalog.h"
#include "duckdb/common/enums/on_create_conflict.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "storage/paimon_catalog_factory.hpp"

#include <map>
#include <string>

namespace duckdb {

PaimonCatalog::PaimonCatalog(AttachedDatabase &db, const string &name, const string &warehouse_path,
                             const std::map<std::string, std::string> &attach_options)
    : Catalog(db), attach_name_(name), warehouse_path_(warehouse_path), attach_options_(attach_options) {
	auto [catalog, catalog_type] = PaimonCatalogFactoryCreate(warehouse_path, attach_options);
	catalog_type_ = std::move(catalog_type);
	paimon_catalog_ = std::move(catalog);
	PaimonCatalogMapRegister(attach_name_, paimon_catalog_.get());
}

PaimonCatalog::~PaimonCatalog() {
	PaimonCatalogMapUnregister(attach_name_);
}

void PaimonCatalog::Initialize(bool load_builtin) {
	(void)load_builtin;
}

void PaimonCatalog::OnDetach(ClientContext &context) {
	PaimonCatalogMapUnregister(attach_name_);
	Catalog::OnDetach(context);
}

bool PaimonCatalog::InMemory() {
	return false;
}

string PaimonCatalog::GetDBPath() {
	return warehouse_path_;
}

void PaimonCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	const auto *pc = paimon_catalog_.get();
	if (!pc) {
		return;
	}
	auto db_list_result = pc->ListDatabases();
	if (!db_list_result.ok()) {
		throw CatalogException("Failed to list Paimon databases: %s", db_list_result.status().ToString().c_str());
	}
	for (const auto &db_name : db_list_result.value()) {
		CreateSchemaInfo info;
		info.schema = db_name;
		info.internal = false;
		PaimonSchemaEntry schema_entry(*this, info);
		callback(schema_entry);
	}
}

optional_ptr<SchemaCatalogEntry> PaimonCatalog::LookupSchema(CatalogTransaction transaction,
                                                             const EntryLookupInfo &schema_lookup,
                                                             const OnEntryNotFound if_not_found) {
	auto &schema_name = schema_lookup.GetEntryName();
	{
		std::lock_guard lock(schema_mutex_);
		if (const auto it = schema_entries_.find(schema_name); it != schema_entries_.end()) {
			return it->second.get();
		}
	}
	const auto *paimon_catalog = paimon_catalog_.get();
	if (!paimon_catalog) {
		return nullptr;
	}
	const auto db_exists_result = paimon_catalog->DatabaseExists(schema_name);
	if (!db_exists_result.ok()) {
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		throw CatalogException("Paimon database '%s' lookup failed: %s", schema_name.c_str(),
		                       db_exists_result.status().ToString().c_str());
	}
	if (!db_exists_result.value()) {
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		throw CatalogException("Schema (database) '%s' does not exist in Paimon catalog", schema_name.c_str());
	}
	CreateSchemaInfo info;
	info.schema = schema_name;
	info.internal = false;
	auto schema_entry = make_uniq<PaimonSchemaEntry>(*this, info);
	std::lock_guard lock(schema_mutex_);
	schema_entries_[schema_name] = std::move(schema_entry);
	return schema_entries_[schema_name].get();
}

optional_ptr<CatalogEntry> PaimonCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	paimon::Catalog *pc = paimon_catalog_.get();
	if (!pc) {
		throw CatalogException("Paimon catalog not initialized");
	}

	std::map<std::string, std::string> options = {
	    {paimon::Options::FILE_SYSTEM, "local"},
	    {paimon::Options::MANIFEST_FORMAT, "avro"},
	};

	bool ignore_if_exists = (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT);
	auto status = pc->CreateDatabase(info.schema, options, ignore_if_exists);
	if (!status.ok()) {
		throw CatalogException("Failed to create Paimon database '%s': %s", info.schema.c_str(),
		                       status.ToString().c_str());
	}

	CreateSchemaInfo schema_info;
	schema_info.schema = info.schema;
	schema_info.internal = info.internal;
	auto schema_entry = make_uniq<PaimonSchemaEntry>(*this, schema_info);
	std::lock_guard lock(schema_mutex_);
	schema_entries_[info.schema] = std::move(schema_entry);
	return schema_entries_[info.schema].get();
}

void PaimonCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("DROP SCHEMA is not supported for Paimon catalog (read-only)");
}

DatabaseSize PaimonCatalog::GetDatabaseSize(ClientContext &context) {
	return DatabaseSize();
}

PhysicalOperator &PaimonCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("CREATE TABLE AS is not supported for Paimon catalog (read-only)");
}

PhysicalOperator &PaimonCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                            optional_ptr<PhysicalOperator> plan) {
	auto &table = op.table;
	auto *paimon_table = dynamic_cast<PaimonTableEntry *>(&table);
	if (!paimon_table) {
		throw InternalException("PlanInsert: expected PaimonTableEntry");
	}

	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw NotImplementedException("INSERT with ON CONFLICT is not supported for Paimon catalog");
	}
	if (op.return_chunk) {
		throw NotImplementedException("INSERT ... RETURNING is not supported for Paimon catalog");
	}

	D_ASSERT(plan);
	if (!op.column_index_map.empty()) {
		plan = planner.ResolveDefaultsProjection(op, *plan);
	}

	auto &insert = planner.Make<PhysicalPaimonInsert>(op.types, *paimon_table, op.estimated_cardinality);
	insert.children.push_back(*plan);
	return insert;
}

PhysicalOperator &PaimonCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                            PhysicalOperator &plan) {
	throw NotImplementedException("DELETE is not supported for Paimon catalog (read-only)");
}

PhysicalOperator &PaimonCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                            PhysicalOperator &plan) {
	throw NotImplementedException("UPDATE is not supported for Paimon catalog (read-only)");
}

unique_ptr<Catalog> PaimonCatalog::Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                          AttachedDatabase &db, const string &name, AttachInfo &info,
                                          AttachOptions &options) {
	const string warehouse_path = info.path;
	std::map<std::string, std::string> attach_options_map;
	for (const auto &[fst, snd] : options.options) {
		attach_options_map[fst] = StringValue::Get(snd.DefaultCastAs(LogicalType::VARCHAR));
	}
	return make_uniq<PaimonCatalog>(db, name, warehouse_path, attach_options_map);
}

} // namespace duckdb
