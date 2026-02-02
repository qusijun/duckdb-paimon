#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "storage/paimon_schema_entry.hpp"
#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace paimon {
class Catalog;
}

namespace duckdb {

struct CreateSchemaInfo;
class PhysicalOperator;
class PhysicalPlanGenerator;
class LogicalCreateTable;
class LogicalInsert;
class LogicalDelete;
class LogicalUpdate;

/// Paimon catalog type: "filesystem" (default) or "rest" (reserved for future).
inline constexpr auto PAIMON_OPTION_CATALOG_TYPE = "catalog_type";
inline constexpr auto PAIMON_CATALOG_TYPE_FILESYSTEM = "filesystem";
inline constexpr auto PAIMON_CATALOG_TYPE_REST = "rest";

class PaimonCatalog : public Catalog {
public:
	explicit PaimonCatalog(AttachedDatabase &db, const string &name, const string &warehouse_path,
	                       const std::map<std::string, std::string> &attach_options);
	~PaimonCatalog() override;

	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "paimon";
	}
	bool InMemory() override;
	string GetDBPath() override;

	paimon::Catalog *GetPaimonCatalog() const {
		return paimon_catalog_.get();
	}
	const string &GetAttachName() const {
		return attach_name_;
	}
	const string &GetPaimonCatalogType() const {
		return catalog_type_;
	}
	const std::map<std::string, std::string> &GetAttachOptions() const {
		return attach_options_;
	}

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	void DropSchema(ClientContext &context, DropInfo &info) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	static unique_ptr<Catalog> Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
	                                  AttachedDatabase &db, const string &name, AttachInfo &info,
	                                  AttachOptions &options);

	void OnDetach(ClientContext &context) override;

private:
	string attach_name_;
	string warehouse_path_;
	string catalog_type_;
	std::map<std::string, std::string> attach_options_;
	std::unique_ptr<paimon::Catalog> paimon_catalog_;
	case_insensitive_map_t<unique_ptr<PaimonSchemaEntry>> schema_entries_;
	std::mutex schema_mutex_;
};

} // namespace duckdb
