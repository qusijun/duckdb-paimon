#include "arrow/status.h"
#include "storage/paimon_transaction_manager.hpp"
#include "storage/paimon_transaction.hpp"
#include <mutex>

namespace duckdb {

// TODO: support txn, currently duckdb-paimon only support read-only catalog
PaimonTransactionManager::PaimonTransactionManager(AttachedDatabase &db, PaimonCatalog &paimon_catalog)
    : TransactionManager(db), paimon_catalog_(paimon_catalog) {
}

Transaction &PaimonTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<PaimonTransaction>(paimon_catalog_, *this, context);
	auto &result = *transaction;
	std::lock_guard lock(transaction_lock_);
	transactions_[result] = std::move(transaction);
	return result;
}

ErrorData PaimonTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	std::lock_guard lock(transaction_lock_);
	transactions_.erase(transaction);
	return ErrorData();
}

void PaimonTransactionManager::RollbackTransaction(Transaction &transaction) {
	std::lock_guard lock(transaction_lock_);
	transactions_.erase(transaction);
}

void PaimonTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// No WAL, no checkpoint, just do nothing
}

} // namespace duckdb
