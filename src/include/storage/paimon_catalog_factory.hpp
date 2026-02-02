#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include <map>
#include <memory>
#include <string>

namespace paimon {
class Catalog;
}

namespace duckdb {

struct PaimonCatalogFactoryResult {
	std::unique_ptr<paimon::Catalog> catalog;
	std::string catalog_type;
};

/// Factory for creating paimon::Catalog from warehouse_path and attach options.
/// Resolves catalog_type from options (default "filesystem"), builds paimon options, then creates:
/// - "filesystem" -> paimon FileSystemCatalog (paimon::Catalog::Create)
/// - "rest" -> reserved for future RestCatalog
PaimonCatalogFactoryResult PaimonCatalogFactoryCreate(const std::string &warehouse_path,
                                                      const std::map<std::string, std::string> &options);

} // namespace duckdb
