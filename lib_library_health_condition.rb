class LibLibraryHealthCondition < ActiveRecord::Base
  belongs_to :lib_library_template, foreign_key: "library_template_id"
  belongs_to :health_condition
  belongs_to :lib_health_condition, foreign_key: "health_condition_id"
    
  
 # def self.fetch_heath_conditions(library_template_id,organization_id)
 #   joins("join health_conditions hc on hc.id=library_health_conditions.health_condition_id")
 #   .where("library_template_id=? and hc.organization_id=? ",library_template_id,organization_id)
 #   .select("GROUP_CONCAT(hc.name) as name")
 # end
end
