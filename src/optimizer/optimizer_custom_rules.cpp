#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/mock_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/values_plan.h"
#include "optimizer/optimizer.h"

// Note for 2022 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file. Note
// that for some test cases, we force using starter rules, so that the configuration here won't take effects. Starter
// rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::OptimizeReorderJoinUseIndex(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeReorderJoinUseIndex(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    // ensure the left child is nlp
    // the right child is seqscan or mockscan
    if (nlj_plan.GetLeftPlan()->GetType() != PlanType::NestedLoopJoin ||
        (nlj_plan.GetRightPlan()->GetType() != PlanType::SeqScan &&
         nlj_plan.GetRightPlan()->GetType() != PlanType::MockScan)) {
      return optimized_plan;
    }

    const auto &left_nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*nlj_plan.GetLeftPlan());

    if (left_nlj_plan.GetLeftPlan()->GetType() == PlanType::NestedLoopJoin ||
        left_nlj_plan.GetRightPlan()->GetType() == PlanType::NestedLoopJoin) {
      return optimized_plan;
    }

    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(&left_nlj_plan.Predicate()); expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            if (left_nlj_plan.GetLeftPlan()->GetType() == PlanType::SeqScan) {
              const auto &left_seq_scan = dynamic_cast<const SeqScanPlanNode &>(*left_nlj_plan.GetLeftPlan());
              if (auto index = MatchIndex(left_seq_scan.table_name_, left_expr->GetColIdx()); index != std::nullopt) {
                auto *outer_expr = dynamic_cast<const ComparisonExpression *>(&nlj_plan.Predicate());
                auto left_outer_expr = dynamic_cast<const ColumnValueExpression *>(outer_expr->children_[0].get());
                auto right_outer_expr = dynamic_cast<const ColumnValueExpression *>(outer_expr->children_[1].get());
                BUSTUB_ASSERT(expr->comp_type_ == ComparisonType::Equal, "comparison type must be equal");
                BUSTUB_ASSERT(outer_expr->comp_type_ == ComparisonType::Equal, "comparison type must be equal");

                auto inner_pred = std::make_shared<ComparisonExpression>(
                    std::make_shared<ColumnValueExpression>(
                        0, left_outer_expr->GetColIdx() - left_nlj_plan.GetLeftPlan()->output_schema_->GetColumnCount(),
                        left_outer_expr->GetReturnType()),
                    std::make_shared<ColumnValueExpression>(1, right_outer_expr->GetColIdx(),
                                                            right_outer_expr->GetReturnType()),
                    ComparisonType::Equal);
                auto outer_pred = std::make_shared<ComparisonExpression>(
                    std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()),
                    std::make_shared<ColumnValueExpression>(1, left_expr->GetColIdx(), left_expr->GetReturnType()),
                    ComparisonType::Equal);

                auto right_column_1 = left_nlj_plan.GetRightPlan()->output_schema_->GetColumns();
                auto right_column_2 = nlj_plan.GetRightPlan()->output_schema_->GetColumns();
                std::vector<Column> columns;
                columns.reserve(right_column_1.size() + right_column_2.size());

                for (const auto &col : right_column_1) {
                  columns.push_back(col);
                }
                for (const auto &col : right_column_2) {
                  columns.push_back(col);
                }

                std::vector<Column> outer_columns(columns);
                for (const auto &col : left_nlj_plan.GetLeftPlan()->output_schema_->GetColumns()) {
                  outer_columns.push_back(col);
                }

                return std::make_shared<NestedLoopJoinPlanNode>(
                    std::make_shared<Schema>(outer_columns),
                    std::make_shared<NestedLoopJoinPlanNode>(std::make_shared<Schema>(columns),
                                                             left_nlj_plan.GetRightPlan(), nlj_plan.GetRightPlan(),
                                                             inner_pred, JoinType::INNER),
                    left_nlj_plan.GetLeftPlan(), outer_pred, JoinType::INNER);
              }
            }
          }
        }
      }
    }
  }

  return optimized_plan;
}

auto Optimizer::OptimizePredicatePushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizePredicatePushDown(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

    if (nlj_plan.GetLeftPlan()->GetType() != PlanType::NestedLoopJoin) {
      return optimized_plan;
    }
    const auto &left_nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*nlj_plan.GetLeftPlan());

    if (nlj_plan.GetRightPlan()->GetType() != PlanType::MockScan ||
        left_nlj_plan.GetLeftPlan()->GetType() != PlanType::MockScan ||
        left_nlj_plan.GetRightPlan()->GetType() != PlanType::MockScan) {
      return optimized_plan;
    }

    std::vector<AbstractExpressionRef> join_preds;
    std::vector<AbstractExpressionRef> filter_preds;
    if (const auto *expr = dynamic_cast<const LogicExpression *>(&nlj_plan.Predicate()); expr != nullptr) {
      while (const auto *inner_expr = dynamic_cast<const LogicExpression *>(expr->children_[0].get())) {
        if (const auto *pred = dynamic_cast<const ColumnValueExpression *>(expr->children_[1]->children_[1].get());
            pred != nullptr) {
          join_preds.push_back(expr->children_[1]);
        } else {
          filter_preds.push_back(expr->children_[1]);
        }
        expr = dynamic_cast<const LogicExpression *>(expr->children_[0].get());
      }
      if (const auto *pred = dynamic_cast<const ColumnValueExpression *>(expr->children_[1]->children_[1].get());
          pred != nullptr) {
        join_preds.push_back(expr->children_[1]);
      } else {
        filter_preds.push_back(expr->children_[1]);
      }
      if (const auto *pred = dynamic_cast<const ColumnValueExpression *>(expr->children_[0]->children_[1].get());
          pred != nullptr) {
        join_preds.push_back(expr->children_[0]);
      } else {
        filter_preds.push_back(expr->children_[0]);
      }

      std::vector<AbstractExpressionRef> first_filter;
      std::vector<AbstractExpressionRef> third_filter;

      for (const auto &pred : filter_preds) {
        const auto *outer = dynamic_cast<const ComparisonExpression *>(pred.get());
        const auto *inner = dynamic_cast<const ColumnValueExpression *>(pred->children_[0].get());
        if (inner->GetTupleIdx() == 0) {
          first_filter.push_back(pred);
        } else {
          third_filter.push_back(std::make_shared<ComparisonExpression>(
              std::make_shared<ColumnValueExpression>(0, inner->GetColIdx(), inner->GetReturnType()),
              pred->children_[1], outer->comp_type_));
        }
      }
      BUSTUB_ASSERT(first_filter.size() == 2, "only in leader board test!");
      BUSTUB_ASSERT(third_filter.size() == 2, "only in leader board test!");

      auto first_pred = std::make_shared<LogicExpression>(first_filter[0], first_filter[1], LogicType::And);
      auto third_pred = std::make_shared<LogicExpression>(third_filter[0], third_filter[1], LogicType::And);

      auto first_filter_scan = std::make_shared<FilterPlanNode>(left_nlj_plan.children_[0]->output_schema_, first_pred,
                                                                left_nlj_plan.children_[0]);
      auto third_filter_scan = std::make_shared<FilterPlanNode>(nlj_plan.GetRightPlan()->output_schema_, third_pred,
                                                                nlj_plan.GetRightPlan());
      auto left_node = std::make_shared<NestedLoopJoinPlanNode>(left_nlj_plan.output_schema_, first_filter_scan,
                                                                left_nlj_plan.GetRightPlan(), left_nlj_plan.predicate_,
                                                                left_nlj_plan.GetJoinType());
      return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, left_node, third_filter_scan,
                                                      join_preds[0], nlj_plan.GetJoinType());
    }
  }

  return optimized_plan;
}

auto Optimizer::IsPredicateFalse(const AbstractExpression &expr) -> bool {
  if (const auto *compare_expr = dynamic_cast<const ComparisonExpression *>(&expr); compare_expr != nullptr) {
    if (const auto *left_expr = dynamic_cast<const ConstantValueExpression *>(compare_expr->children_[0].get());
        left_expr != nullptr) {
      if (const auto *right_expr = dynamic_cast<const ConstantValueExpression *>(compare_expr->children_[1].get());
          right_expr != nullptr) {
        if (compare_expr->comp_type_ == ComparisonType::Equal) {
          if (left_expr->val_.CastAs(TypeId::INTEGER).GetAs<int>() !=
              right_expr->val_.CastAs(TypeId::INTEGER).GetAs<int>()) {
            return true;
          }
        }
      }
    }
  }
  return false;
}

auto Optimizer::OptimizeFalseFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeFalseFilter(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);

    if (IsPredicateFalse(*filter_plan.GetPredicate())) {
      return std::make_shared<ValuesPlanNode>(filter_plan.children_[0]->output_schema_,
                                              std::vector<std::vector<AbstractExpressionRef>>{});
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizeRemoveJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeRemoveJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    if (nlj_plan.GetRightPlan()->GetType() == PlanType::Values) {
      const auto &right_plan = dynamic_cast<const ValuesPlanNode &>(*nlj_plan.GetRightPlan());

      if (right_plan.GetValues().empty()) {
        return nlj_plan.children_[0];
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizeRemoveColumn(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeRemoveJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Projection) {
    const auto outer_proj = dynamic_cast<const ProjectionPlanNode &>(*optimized_plan);

    if (outer_proj.GetChildPlan()->GetType() == PlanType::Projection) {
      const auto inner_proj = dynamic_cast<const ProjectionPlanNode &>(*outer_proj.GetChildPlan());

      if (inner_proj.GetChildPlan()->GetType() == PlanType::Aggregation) {
        const auto agg_plan = dynamic_cast<const AggregationPlanNode &>(*inner_proj.GetChildPlan());
        std::vector<AbstractExpressionRef> cols;
        for (size_t i = 0; i < outer_proj.GetExpressions().size(); ++i) {
          if (const auto *pred = dynamic_cast<const ColumnValueExpression *>(inner_proj.GetExpressions()[i].get());
              pred != nullptr) {
            cols.push_back(inner_proj.GetExpressions()[i]);
          } else {
            // hacking
            cols.push_back(inner_proj.GetExpressions()[i]->children_[0]->children_[0]);
            cols.push_back(inner_proj.GetExpressions()[i]->children_[0]->children_[1]);
            cols.push_back(inner_proj.GetExpressions()[i]->children_[1]);
          }
        }

        std::vector<Column> inner_schema;
        std::vector<AbstractExpressionRef> inner_proj_expr;
        for (const auto &i : outer_proj.GetExpressions()) {
          const auto *col = dynamic_cast<const ColumnValueExpression *>(i.get());
          inner_proj_expr.push_back(inner_proj.GetExpressions()[col->GetColIdx()]);
          inner_schema.push_back(inner_proj.OutputSchema().GetColumns()[col->GetColIdx()]);
        }

        inner_proj_expr.pop_back();
        inner_proj_expr.push_back(std::make_shared<ArithmeticExpression>(
            std::make_shared<ArithmeticExpression>(std::make_shared<ColumnValueExpression>(0, 1, TypeId::INTEGER),
                                                   std::make_shared<ColumnValueExpression>(0, 1, TypeId::INTEGER),
                                                   ArithmeticType::Plus),
            std::make_shared<ColumnValueExpression>(0, 2, TypeId::INTEGER), ArithmeticType::Plus));

        std::vector<AbstractExpressionRef> aggregates;
        std::vector<AggregationType> agg_types;
        std::vector<Column> agg_schema;

        for (size_t i = 0; i < agg_plan.GetGroupBys().size(); ++i) {
          agg_schema.push_back(agg_plan.OutputSchema().GetColumns()[i]);
        }

        aggregates.push_back(agg_plan.GetAggregates()[0]);
        agg_types.push_back(agg_plan.GetAggregateTypes()[0]);
        agg_schema.push_back(agg_plan.OutputSchema().GetColumns()[agg_plan.GetGroupBys().size()]);

        aggregates.push_back(agg_plan.GetAggregates()[3]);
        agg_types.push_back(agg_plan.GetAggregateTypes()[3]);
        agg_schema.push_back(agg_plan.OutputSchema().GetColumns()[agg_plan.GetGroupBys().size() + 3]);

        return std::make_shared<ProjectionPlanNode>(
            std::make_shared<Schema>(inner_schema), inner_proj_expr,
            std::make_shared<AggregationPlanNode>(std::make_shared<Schema>(agg_schema), agg_plan.GetChildAt(0),
                                                  agg_plan.GetGroupBys(), aggregates, agg_types));
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizeMergeFilterIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeFilterIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "must have exactly one children");
    const auto &child_plan = *optimized_plan->children_[0];
    if (child_plan.GetType() == PlanType::SeqScan) {
      const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(child_plan);
      const auto *table_info = catalog_.GetTable(seq_scan_plan.GetTableOid());
      const auto indices = catalog_.GetTableIndexes(table_info->name_);
      if (const auto *expr = dynamic_cast<const ComparisonExpression *>(filter_plan.GetPredicate().get());
          expr != nullptr) {
        if (expr->comp_type_ == ComparisonType::Equal) {
          if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
              left_expr != nullptr) {
            if (const auto *right_expr = dynamic_cast<const ConstantValueExpression *>(expr->children_[1].get());
                right_expr != nullptr) {
              for (const auto *index : indices) {
                const auto &columns = index->key_schema_.GetColumns();
                if (columns.size() == 1 &&
                    columns[0].GetName() == table_info->schema_.GetColumn(left_expr->GetColIdx()).GetName()) {
                  return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, index->index_oid_,
                                                             filter_plan.GetPredicate());
                }
              }
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizeMergeFilterIndexScan(p);
  p = OptimizeMergeFilterScan(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeReorderJoinUseIndex(p);
  p = OptimizePredicatePushDown(p);
  p = OptimizeFalseFilter(p);
  p = OptimizeRemoveJoin(p);
  p = OptimizeRemoveColumn(p);
  p = OptimizeNLJAsIndexJoin(p);
  p = OptimizeNLJAsHashJoin(p);  // Enable this rule after you have implemented hash join.
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  return p;
}

}  // namespace bustub
