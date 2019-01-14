#pragma once

#include <memory>
#include <queue> // NEEDEDINCLUDE
#include <set>
#include <unordered_set>

#include "abstract_lqp_node.hpp"

namespace opossum {

class AbstractExpression;
enum class LQPInputSide;

using LQPMismatch = std::pair<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<const AbstractLQPNode>>;

/**
 * For two equally structured LQPs lhs and rhs, create a mapping for each node in lhs pointing to its equivalent in rhs.
 */
LQPNodeMapping lqp_create_node_mapping(const std::shared_ptr<AbstractLQPNode>& lhs,
                                       const std::shared_ptr<AbstractLQPNode>& rhs);

/**
 * Perform a deep equality check of two LQPs.
 * @return std::nullopt if the LQPs were equal. A pair of a node in this LQP and a node in the rhs LQP that were first
 *         discovered to differ.
 */
std::optional<LQPMismatch> lqp_find_subplan_mismatch(const std::shared_ptr<const AbstractLQPNode>& lhs,
                                                     const std::shared_ptr<const AbstractLQPNode>& rhs);

void lqp_replace_node(const std::shared_ptr<AbstractLQPNode>& original_node,
                      const std::shared_ptr<AbstractLQPNode>& replacement_node);

void lqp_remove_node(const std::shared_ptr<AbstractLQPNode>& node);

void lqp_insert_node(const std::shared_ptr<AbstractLQPNode>& parent_node, const LQPInputSide input_side,
                     const std::shared_ptr<AbstractLQPNode>& node);

/**
 * @return whether all paths to all leaves contain a Validate node - i.e. the LQP can be used in an MVCC aware context
 */
bool lqp_is_validated(const std::shared_ptr<AbstractLQPNode>& lqp);

/**
 * @return all names of tables that have been accessed in modifying nodes (e.g., InsertNode, UpdateNode)
 */
std::set<std::string> lqp_find_modified_tables(const std::shared_ptr<AbstractLQPNode>& lqp);

/**
 * Create a boolean expression from an LQP by considering PredicateNodes and UnionNodes
 * @return      the expression, or nullptr if no expression could be created
 */
std::shared_ptr<AbstractExpression> lqp_subplan_to_boolean_expression(const std::shared_ptr<AbstractLQPNode>& lqp);

enum class LQPVisitation { VisitInputs, DoNotVisitInputs };

/**
 * Calls the passed @param visitor on each node of the @param lqp. This will NOT visit subselects.
 * The visitor returns `ExpressionVisitation`, indicating whether the current nodes's input should be visited
 * as well.
 * Each node is visited exactly once.
 *
 * @tparam Visitor      Functor called with every node as a param.
 *                      Returns `LQPVisitation`
 */
template <typename Visitor>
void visit_lqp(const std::shared_ptr<AbstractLQPNode>& lqp, Visitor visitor) {
  std::queue<std::shared_ptr<AbstractLQPNode>> node_queue;
  node_queue.push(lqp);

  std::unordered_set<std::shared_ptr<AbstractLQPNode>> visited_nodes;

  while (!node_queue.empty()) {
    auto node = node_queue.front();
    node_queue.pop();

    if (!visited_nodes.emplace(node).second) continue;

    if (visitor(node) == LQPVisitation::VisitInputs) {
      if (node->left_input()) node_queue.push(node->left_input());
      if (node->right_input()) node_queue.push(node->right_input());
    }
  }
}

/**
 * @return The node @param lqp as well as the root nodes of all LQPs in subselects and, recursively, LQPs in their
 *         subselects
 */
std::vector<std::shared_ptr<AbstractLQPNode>> lqp_find_subplan_roots(const std::shared_ptr<AbstractLQPNode>& lqp);

}  // namespace opossum
