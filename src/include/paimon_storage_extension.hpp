#pragma once

#include "duckdb/main/config.hpp"
#include <string>

namespace paimon {
class Catalog;
}

namespace duckdb {

void PaimonCatalogMapRegister(const std::string &name, paimon::Catalog *catalog);
void PaimonCatalogMapUnregister(const std::string &name);
paimon::Catalog *PaimonCatalogMapGet(const std::string &name);

} // namespace duckdb
