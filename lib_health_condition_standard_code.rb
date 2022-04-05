class LibHealthConditionStandardCode < ActiveRecord::Base
  belongs_to :lib_health_conditions, foreign_key: "health_condition_id"
  belongs_to :standard, foreign_key: "standard_id"
end
