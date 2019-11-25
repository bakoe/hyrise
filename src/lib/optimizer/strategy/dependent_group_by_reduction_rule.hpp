#pragma once

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/lqp_column_expression.hpp"

namespace opossum {

class AbstractLQPNode;

// Removes columns from list of group by column when they are functionally dependent from column(s) of the grouping.
// This is currently only the case, when both the primary key and other columns of the same table are grouped.
// This is regularly the case, when the columns are supposed to be later accessed (e.g., see usage of "c_acctbal" in
// TPC-H query 10). As "c_acctbal" does not need to be in the grouping (since the primary key c_custkey is present),
// the column in added as `ANY(c_acctbal)` to the aggregation list. ANY() selects "any" value from the list of values
// per group (since we group by the primary key in this case, the group is ensured to be of the size one).
// Columns that have initially been grouped, been ANY()'d, and are later NON referenced anymore are added to the
// aggregates nonetheless as we expect the column pruning rule to remove them.
//
// This rule implements choke point 1.4 of "TPC-H Analyzed: Hidden Messages and Lessons Learned from an Influential
// Benchmark" (Boncz et al.). Due to the current lack of foreign key suppport, not all queries can be optimized.
//
// When only a single table is aggregated and the full primary key is grouped, one could theoretically replace
// aggregate functions such as `SUM(dependent_column)` with `ANY(dependent_column)` as the group will only include a
// single tuple. We do not do that as aggregate functions for single tuples should only be neglectably more expensive
// than ANY().

// When a group by reduction changes the column order and the column names due to using an ANY(), an alias node
// (renaming) as well as a projection node (column order) need to be appended to restored the expected result.
class DependentGroupByReductionRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const override;

 private:
  /**
   * Takes a list of expressions and wraps each occurence of @param node_to_wrap in an ANY() aggregate function. Does
   * not wrap @param node_to_wrap if it is already wrapped in ANY().
   */
  // void wrap_node_in_any(std::vector<std::shared_ptr<AbstractExpression>>& expressions,
  // 						const std::shared_ptr<LQPColumnExpression>& node_to_wrap) const;

  /**
   * Searches for succeeding alias nodes (starting from @param node upwards) and adapts them to remove any mention of
   * ANY() functions. If no alias node is found, creates a new root node which does the same.
   */
  // void restore_column_names(const std::shared_ptr<AbstractLQPNode>& node) const;
};

}  // namespace opossum