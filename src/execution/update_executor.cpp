//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  try {
    bool is_locked = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!is_locked) {
      throw ExecutionException("Update Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException e) {
    throw ExecutionException("Update Executor Get Table Lock Failed");
  }
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  Tuple old_tuple{};
  RID old_rid;
  int32_t update_count = 0;

  while (child_executor_->Next(&old_tuple, &old_rid)) {
    try {
      bool is_locked = exec_ctx_->GetLockManager()->LockRow(
          exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, table_info_->oid_, old_rid);
      if (!is_locked) {
        throw ExecutionException("Update Executor Get Row Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("Update Executor Get Row Lock Failed");
    }

    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
    }

    auto to_update_tuple = Tuple{values, &child_executor_->GetOutputSchema()};

    bool updated = table_info_->table_->UpdateTuple(to_update_tuple, old_rid, exec_ctx_->GetTransaction());

    if (updated) {
      // std::for_each(table_indexes_.begin(), table_indexes_.end(),
      //               [&old_tuple, &rid, &table_info = table_info_, &exec_ctx = exec_ctx_](IndexInfo *index) {
      //                 index->index_->DeleteEntry(old_tuple.KeyFromTuple(table_info->schema_, index->key_schema_,
      //                                                                         index->index_->GetKeyAttrs()),
      //                                            *rid, exec_ctx->GetTransaction());
      //               });
      // std::for_each(table_indexes_.begin(), table_indexes_.end(),
      //               [&to_update_tuple, &old_rid, &table_info = table_info_, &exec_ctx = exec_ctx_](IndexInfo *index)
      //               {
      //                 index->index_->InsertEntry(to_update_tuple.KeyFromTuple(table_info->schema_,
      //                 index->key_schema_,
      //                                                                         index->index_->GetKeyAttrs()),
      //                                            old_rid, exec_ctx->GetTransaction());
      //               });
      update_count++;
    }
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, update_count);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
