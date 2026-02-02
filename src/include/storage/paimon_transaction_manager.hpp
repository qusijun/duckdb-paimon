#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/paimon_catalog.hpp"
#include "storage/paimon_transaction.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

class PaimonTransactionManager : public TransactionManager {
public:
	PaimonTransactionManager(AttachedDatabase &db, PaimonCatalog &paimon_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	PaimonCatalog &paimon_catalog_;
	std::mutex transaction_lock_;
	reference_map_t<Transaction, unique_ptr<PaimonTransaction>> transactions_;
};

} // namespace duckdb
