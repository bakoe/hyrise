#pragma once

#include <string>
#include <vector>

#include "abstract_features.hpp"
#include "all_type_variant.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {
namespace cost_model {

struct ColumnFeatures : public AbstractFeatures {
  explicit ColumnFeatures(const std::string& prefix);

  // TODO(Sven): Initialize all values with NullValue. makes it easier to print to CSV later. Less error-prone
  float column_segment_encoding_Unencoded_percentage = 0.0;
  float column_segment_encoding_Dictionary_percentage = 0.0;
  float column_segment_encoding_RunLength_percentage = 0.0;
  float column_segment_encoding_FixedStringDictionary_percentage = 0.0;
  float column_segment_encoding_FrameOfReference_percentage = 0.0;
  float column_segment_encoding_LZ4_percentage = 0.0;

  float column_segment_vector_compression_FSBA_percentage = 0.0;
  float column_segment_vector_compression_SimdBp128_percentage = 0.0;
  // TODO(Sven): change feature extractor
  bool column_is_reference_segment = false;
  std::optional<DataType> column_data_type = {};
  size_t column_memory_usage_bytes = 0;
  // TODO(Sven): How to calculate from segment_distinct_value_count?
  size_t column_distinct_value_count = 0;

  const std::map<std::string, AllTypeVariant> serialize() const override;
  const std::unordered_map<std::string, float> to_cost_model_features() const override;

 private:
  std::string _prefix;
};

}  // namespace cost_model
}  // namespace opossum