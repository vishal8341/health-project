class LibHealthConditionRoleType < ActiveRecord::Base
  belongs_to :role_types
  belongs_to :lib_health_conditions, foreign_key: "lib_health_condition_id"
end
