class LibHealthConditionOrganization < ActiveRecord::Base
  belongs_to :lib_health_conditions,  foreign_key: "health_condition_id"
end
