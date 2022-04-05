class HealthConditionRoleType < ActiveRecord::Base
  belongs_to :role_types
  belongs_to :health_conditions
end
