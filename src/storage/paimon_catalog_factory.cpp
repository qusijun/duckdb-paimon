#include "arrow/status.h"
#include "storage/paimon_catalog_factory.hpp"
#include "storage/paimon_catalog.hpp"
#include "paimon/catalog/catalog.h"
#include "paimon/defs.h"
#include "duckdb/common/exception.hpp"
#include <map>
#include <string>

namespace duckdb {

PaimonCatalogFactoryResult PaimonCatalogFactoryCreate(const std::string &warehouse_path,
                                                      const std::map<std::string, std::string> &options) {
	std::string catalog_type = PAIMON_CATALOG_TYPE_FILESYSTEM;
	if (const auto it = options.find(PAIMON_OPTION_CATALOG_TYPE); it != options.end()) {
		catalog_type = it->second;
		StringUtil::Lower(catalog_type);
	}

	// Paimon catalog options: defaults then overwritten by attach options
	std::map<std::string, std::string> paimon_options = {
	    {paimon::Options::FILE_SYSTEM, "local"},
	    {paimon::Options::MANIFEST_FORMAT, "avro"},
	};
	for (const auto &[fst, snd] : options) {
		// Do not pass this to paimon-cpp
		if (fst == PAIMON_OPTION_CATALOG_TYPE) {
			continue;
		}
		paimon_options[fst] = snd;
	}

	if (catalog_type == PAIMON_CATALOG_TYPE_FILESYSTEM) {
		auto result = paimon::Catalog::Create(warehouse_path, paimon_options);
		if (!result.ok()) {
			throw InvalidInputException("Failed to create Paimon FilesystemCatalog: %s",
			                            result.status().ToString().c_str());
		}
		return {std::move(result).value(), catalog_type};
	}
	if (catalog_type == PAIMON_CATALOG_TYPE_REST) {
		throw InvalidInputException("Paimon RestCatalog is not yet supported, use catalog_type 'filesystem'");
	}
	throw InvalidInputException("Paimon catalog_type must be 'filesystem' or 'rest', got: %s", catalog_type.c_str());
}

} // namespace duckdb
