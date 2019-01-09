#include "cost_model_adaptive.hpp"

#include "cost_model/feature_extractor/cost_model_feature_extractor.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

CostModelAdaptive::CostModelAdaptive(const std::unordered_map<const TableScanModelGroup, const ModelCoefficients,
                                                              TableScanModelGroupHash>& all_coefficients)
    : _table_scan_models({}) {
  // Initialize all LinearRegression Models
  for (const auto& [group, coefficients] : all_coefficients) {
    _table_scan_models[group] = std::make_shared<LinearRegressionModel>(coefficients);
  }
}

Cost CostModelAdaptive::_estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto output_row_count = node->get_statistics()->row_count();
  const auto left_input_row_count = node->left_input() ? node->left_input()->get_statistics()->row_count() : 0.0f;
  const auto right_input_row_count = node->right_input() ? node->right_input()->get_statistics()->row_count() : 0.0f;

  switch (node->type) {
    case LQPNodeType::Join:
      // Covers predicated and unpredicated joins. For cross joins, output_row_count will be
      // left_input_row_count * right_input_row_count
      return left_input_row_count + right_input_row_count + output_row_count;

    case LQPNodeType::Sort:
      return left_input_row_count * std::log(left_input_row_count);

    case LQPNodeType::Union: {
      const auto union_node = std::static_pointer_cast<UnionNode>(node);

      switch (union_node->union_mode) {
        case UnionMode::Positions:
          return left_input_row_count * std::log(left_input_row_count) +
                 right_input_row_count * std::log(right_input_row_count);
        default:
          Fail("GCC thinks this is reachable");
      }
    }

    case LQPNodeType::Predicate: {
      const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);
      return _predict_predicate(predicate_node);
      // return left_input_row_count * _get_expression_cost_multiplier(predicate_node->predicate()) + output_row_count;
    }

    default:
      return left_input_row_count + output_row_count;
  }
}

Cost CostModelAdaptive::_predict_predicate(const std::shared_ptr<PredicateNode>& node) const {
  const auto left_input_node = node->left_input();
  if (!left_input_node) {
    return Cost{0.0f};
  }

  const auto& predicate = node->predicate();
  const auto& predicate_arguments = predicate->arguments;
  const auto& first_argument = predicate_arguments[0];

  std::cout << first_argument << std::endl;

  // TODO(Sven): actual data type
  const auto first_column_data_type = DataType::Int;
//    data_type_to_string.right.at(first_column_data_type);

  const auto feature_map = cost_model::CostModelFeatureExtractor::extract_features(node).serialize();

  const auto reference_segment = feature_map.at("first_column_is_segment_reference_segment_True");
//  const auto first_column_data_type = feature_map.at("first_column_data_type");
  const auto is_small_table = feature_map.at("is_small_table_True");

  std::cout << reference_segment << std::endl;
  std::cout << is_small_table << std::endl;

  // find correct LR Model based on data type, first_segment_is_reference_segment, and is_small_table
    const TableScanModelGroup group{OperatorType::TableScan, first_column_data_type,
                                    static_cast<bool>(get<int32_t>(reference_segment)),
                                    static_cast<bool>(get<int32_t>(is_small_table))};

    const auto model = _table_scan_models.at(group);
//    return model->predict(feature_map);
  return Cost{0.0f};
}

}  // namespace opossum