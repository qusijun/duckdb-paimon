#include "arrow/status.h"
#include "paimon_storage_extension.hpp"
#include "paimon/catalog/catalog.h"
#include <mutex>

namespace duckdb {

static std::mutex g_paimon_catalog_map_mutex;
static std::map<std::string, paimon::Catalog *> g_paimon_catalog_map;

void PaimonCatalogMapRegister(const std::string &name, paimon::Catalog *catalog) {
	std::lock_guard lock(g_paimon_catalog_map_mutex);
	g_paimon_catalog_map[name] = catalog;
}

void PaimonCatalogMapUnregister(const std::string &name) {
	std::lock_guard lock(g_paimon_catalog_map_mutex);
	g_paimon_catalog_map.erase(name);
}

paimon::Catalog *PaimonCatalogMapGet(const std::string &name) {
	std::lock_guard lock(g_paimon_catalog_map_mutex);
	const auto it = g_paimon_catalog_map.find(name);
	if (it == g_paimon_catalog_map.end()) {
		return nullptr;
	}
	return it->second;
}

} // namespace duckdb
