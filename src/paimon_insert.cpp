#include "paimon_insert.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_context.hpp"
#include "paimon/api.h"
#include "paimon/commit_context.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/record_batch.h"
#include "paimon/write_context.h"

namespace duckdb {

class PaimonInsertGlobalState : public GlobalSinkState {
public:
	explicit PaimonInsertGlobalState(string table_path, vector<LogicalType> types)
	    : table_path(std::move(table_path)), insert_types(std::move(types)), insert_count(0) {
	}

	string table_path;
	vector<LogicalType> insert_types;
	idx_t insert_count;
	std::unique_ptr<paimon::FileStoreWrite> writer;
};

PhysicalPaimonInsert::PhysicalPaimonInsert(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                           PaimonTableEntry &table, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::INSERT, std::move(types), estimated_cardinality),
      insert_table(table), insert_types(table.GetTypes()) {
}

unique_ptr<GlobalSinkState> PhysicalPaimonInsert::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<PaimonInsertGlobalState>(insert_table.GetTablePath(), insert_types);

	std::map<std::string, std::string> options = {
	    {paimon::Options::FILE_SYSTEM, "local"},
	    {paimon::Options::MANIFEST_FORMAT, "avro"},
	    {paimon::Options::FILE_FORMAT, "parquet"},
	};

	std::string commit_user = "duckdb";
	paimon::WriteContextBuilder context_builder(state->table_path, commit_user);
	auto write_context_result = context_builder.SetOptions(options).Finish();
	if (!write_context_result.ok()) {
		throw InvalidInputException("Failed to create Paimon write context: %s",
		                            write_context_result.status().ToString().c_str());
	}

	auto writer_result = paimon::FileStoreWrite::Create(std::move(write_context_result).value());
	if (!writer_result.ok()) {
		throw InvalidInputException("Failed to create Paimon writer: %s", writer_result.status().ToString().c_str());
	}
	state->writer = std::move(writer_result).value();

	return state;
}

unique_ptr<LocalSinkState> PhysicalPaimonInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<LocalSinkState>();
}

SinkResultType PhysicalPaimonInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<PaimonInsertGlobalState>();
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	chunk.Flatten();

	auto &client_context = context.client;
	auto client_properties = client_context.GetClientProperties();
	auto extension_type_cast = ArrowTypeExtensionData::GetExtensionTypes(client_context, chunk.GetTypes());

	ArrowArrayWrapper arrow_array;
	ArrowConverter::ToArrowArray(chunk, &arrow_array.arrow_array, client_properties, extension_type_cast);

	paimon::RecordBatchBuilder batch_builder(&arrow_array.arrow_array);
	// Fixed-bucket primary key tables require an explicit bucket ID per batch
	int32_t num_buckets = insert_table.GetNumBuckets();
	if (num_buckets > 0) {
		if (num_buckets > 1) {
			throw NotImplementedException("INSERT into multi-bucket primary key tables is not yet supported");
		}
		batch_builder.SetBucket(0);
	}
	auto batch_result = batch_builder.Finish();
	if (!batch_result.ok()) {
		throw InvalidInputException("Failed to build Paimon record batch: %s",
		                            batch_result.status().ToString().c_str());
	}

	auto write_status = gstate.writer->Write(std::move(batch_result).value());
	if (!write_status.ok()) {
		throw InvalidInputException("Failed to write to Paimon table: %s", write_status.ToString().c_str());
	}

	gstate.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalPaimonInsert::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalPaimonInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<PaimonInsertGlobalState>();

	auto commit_messages_result = gstate.writer->PrepareCommit();
	if (!commit_messages_result.ok()) {
		throw InvalidInputException("Failed to prepare Paimon commit: %s",
		                            commit_messages_result.status().ToString().c_str());
	}
	auto commit_messages = std::move(commit_messages_result).value();

	auto close_status = gstate.writer->Close();
	if (!close_status.ok()) {
		throw InvalidInputException("Failed to close Paimon writer: %s", close_status.ToString().c_str());
	}

	if (commit_messages.empty()) {
		return SinkFinalizeType::READY;
	}

	std::map<std::string, std::string> options = {
	    {paimon::Options::FILE_SYSTEM, "local"},
	    {paimon::Options::MANIFEST_FORMAT, "avro"},
	};

	std::string commit_user = "duckdb";
	paimon::CommitContextBuilder commit_context_builder(gstate.table_path, commit_user);
	auto commit_context_result = commit_context_builder.SetOptions(options).Finish();
	if (!commit_context_result.ok()) {
		throw InvalidInputException("Failed to create Paimon commit context: %s",
		                            commit_context_result.status().ToString().c_str());
	}

	auto committer_result = paimon::FileStoreCommit::Create(std::move(commit_context_result).value());
	if (!committer_result.ok()) {
		throw InvalidInputException("Failed to create Paimon committer: %s",
		                            committer_result.status().ToString().c_str());
	}
	auto committer = std::move(committer_result).value();

	auto commit_status = committer->Commit(commit_messages);
	if (!commit_status.ok()) {
		throw InvalidInputException("Failed to commit to Paimon table: %s", commit_status.ToString().c_str());
	}

	return SinkFinalizeType::READY;
}

string PhysicalPaimonInsert::GetName() const {
	return "PAIMON_INSERT";
}

InsertionOrderPreservingMap<string> PhysicalPaimonInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table"] =
	    StringUtil::Format("[%s.%s.%s]", insert_table.schema.name, insert_table.name, insert_table.GetTablePath());
	return result;
}

unique_ptr<GlobalSourceState> PhysicalPaimonInsert::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<GlobalSourceState>();
}

SourceResultType PhysicalPaimonInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<PaimonInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.insert_count)));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
