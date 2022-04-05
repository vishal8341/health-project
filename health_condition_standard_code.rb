class HealthConditionStandardCode < ActiveRecord::Base
  belongs_to :health_conditions
  belongs_to :standards
end
