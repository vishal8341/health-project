class LibraryHealthCondition < ActiveRecord::Base
  belongs_to :library_template
  belongs_to :health_condition
  
  def self.fetch_heath_conditions(library_template_id,organization_id)
    joins("join health_conditions hc on hc.id=library_health_conditions.health_condition_id")
    .where("library_template_id=? and hc.organization_id=? ",library_template_id,organization_id)
    .select("GROUP_CONCAT(hc.name) as name")
  end
      # query created for lib health condition API
  def self.fetch_library_health_condition_by_healtcondition_id(healthconditionId)
    self.select("library_health_conditions.id,library_health_conditions.library_template_id,library_health_conditions.health_condition_id,library_health_conditions.created_at,library_health_conditions.updated_at,library_health_conditions.Ref_Lib_Health,health_conditions.name as healt_con_name,organizations.name as org_name,library_templates.name as lib_name")
                          .joins("left join library_templates on library_health_conditions.library_template_id = library_templates.id")
                          .joins("left join health_conditions on library_health_conditions.health_condition_id = health_conditions.id")
                          .joins("left join organizations on health_conditions.organization_id = organizations.id")
                          .where("health_condition_id = "+healthconditionId)
  end
end
