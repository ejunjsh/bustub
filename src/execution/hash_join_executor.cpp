//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      left_executor_{std::move(left_child)},
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  Tuple tmp_tuple{};
  RID rid;
  while (right_executor_->Next(&tmp_tuple, &rid)) {
    auto join_key = plan_->RightJoinKeyExpression().Evaluate(&tmp_tuple, plan_->GetRightPlan()->OutputSchema());
    hash_join_table_[HashUtil::HashValue(&join_key)].push_back(tmp_tuple);
  }

  while (left_executor_->Next(&tmp_tuple, &rid)) {
    auto join_key = plan_->LeftJoinKeyExpression().Evaluate(&tmp_tuple, plan_->GetLeftPlan()->OutputSchema());
    if (hash_join_table_.count(HashUtil::HashValue(&join_key)) > 0) {
      auto right_tuples = hash_join_table_[HashUtil::HashValue(&join_key)];
      for (const auto &tuple : right_tuples) {
        auto right_join_key = plan_->RightJoinKeyExpression().Evaluate(&tuple, plan_->GetRightPlan()->OutputSchema());
        if (right_join_key.CompareEquals(join_key) == CmpBool::CmpTrue) {
          std::vector<Value> values{};
          values.reserve(plan_->GetLeftPlan()->OutputSchema().GetColumnCount() +
                         plan_->GetRightPlan()->OutputSchema().GetColumnCount());
          for (uint32_t col_idx = 0; col_idx < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); col_idx++) {
            values.push_back(tmp_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), col_idx));
          }
          for (uint32_t col_idx = 0; col_idx < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); col_idx++) {
            values.push_back(tuple.GetValue(&plan_->GetRightPlan()->OutputSchema(), col_idx));
          }
          output_tuples_.emplace_back(values, &GetOutputSchema());
        }
      }
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values{};
      values.reserve(plan_->GetLeftPlan()->OutputSchema().GetColumnCount() +
                     plan_->GetRightPlan()->OutputSchema().GetColumnCount());
      for (uint32_t col_idx = 0; col_idx < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); col_idx++) {
        values.push_back(tmp_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), col_idx));
      }
      for (uint32_t col_idx = 0; col_idx < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); col_idx++) {
        values.push_back(
            ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(col_idx).GetType()));
      }
      output_tuples_.emplace_back(values, &GetOutputSchema());
    }
  }

  output_tuples_iter_ = output_tuples_.cbegin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (output_tuples_iter_ == output_tuples_.cend()) {
    return false;
  }
  *tuple = *output_tuples_iter_;
  ++output_tuples_iter_;
  return true;
}

}  // namespace bustub
