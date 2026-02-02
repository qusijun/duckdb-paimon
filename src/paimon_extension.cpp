#define DUCKDB_EXTENSION_MAIN

#include "arrow/status.h"
#include "paimon_extension.hpp"
#include "paimon_functions.hpp"
#include "storage/paimon_catalog.hpp"
#include "storage/paimon_transaction_manager.hpp"
#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

static unique_ptr<TransactionManager> CreatePaimonTransactionManagerFn(optional_ptr<StorageExtensionInfo> storage_info,
                                                                       AttachedDatabase &db, Catalog &catalog) {
	(void)storage_info;
	auto &paimon_catalog = catalog.Cast<PaimonCatalog>();
	return make_uniq<PaimonTransactionManager>(db, paimon_catalog);
}

class PaimonStorageExtension : public StorageExtension {
public:
	PaimonStorageExtension() {
		attach = PaimonCatalog::Attach;
		create_transaction_manager = CreatePaimonTransactionManagerFn;
	}
};

inline void PaimonScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Paimon " + name.GetString() + " 🐬");
	});
}

inline void PaimonOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Paimon " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(instance);
	config.storage_extensions["paimon"] = make_uniq<PaimonStorageExtension>();

	// Register a scalar function
	auto paimon_scalar_function =
	    ScalarFunction("paimon", {LogicalType::VARCHAR}, LogicalType::VARCHAR, PaimonScalarFun);
	loader.RegisterFunction(paimon_scalar_function);

	// Register another scalar function
	auto paimon_openssl_version_scalar_function = ScalarFunction("paimon_openssl_version", {LogicalType::VARCHAR},
	                                                             LogicalType::VARCHAR, PaimonOpenSSLVersionScalarFun);
	loader.RegisterFunction(paimon_openssl_version_scalar_function);
}

void PaimonExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string PaimonExtension::Name() {
	return "paimon";
}

std::string PaimonExtension::Version() const {
#ifdef EXT_VERSION_PAIMON
	return EXT_VERSION_PAIMON;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(paimon, loader) {
	duckdb::LoadInternal(loader);
}
}
