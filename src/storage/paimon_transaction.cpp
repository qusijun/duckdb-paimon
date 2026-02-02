#include "storage/paimon_transaction.hpp"
#include "storage/paimon_catalog.hpp"

namespace duckdb {

PaimonTransaction::PaimonTransaction(PaimonCatalog &paimon_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), paimon_catalog_(paimon_catalog) {
}

PaimonTransaction::~PaimonTransaction() = default;

PaimonTransaction &PaimonTransaction::Get(ClientContext &context, Catalog &catalog) {
	auto &transaction = Transaction::Get(context, catalog);
	return transaction.Cast<PaimonTransaction>();
}

} // namespace duckdb
