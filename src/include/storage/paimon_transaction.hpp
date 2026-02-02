#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "storage/paimon_catalog.hpp"

namespace duckdb {

class PaimonTransaction : public Transaction {
public:
	PaimonTransaction(PaimonCatalog &paimon_catalog, TransactionManager &manager, ClientContext &context);
	~PaimonTransaction() override;

	static PaimonTransaction &Get(ClientContext &context, Catalog &catalog);
	PaimonCatalog &GetCatalog() {
		return paimon_catalog_;
	}

private:
	PaimonCatalog &paimon_catalog_;
};

} // namespace duckdb
