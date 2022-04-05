class LibLibraryOrganization < ActiveRecord::Base
  
  def self.fetch_library_status(org_id,temp_id)
    
   Organization.select("organizations.id as org_id, organizations.name as org_name ")
    .where("deactivated = 0 and id not in(SELECT 
llo.`organization_id`
FROM `lib_library_health_conditions` lllh
JOIN `lib_library_organizations` llo ON llo.`library_template_id` = lllh.`library_template_id`
WHERE lllh.`health_condition_id` =  (SELECT  llh_c.`health_condition_id` 
FROM `lib_library_templates` ll_t 
JOIN `lib_library_health_conditions` llh_c ON llh_c.`library_template_id` =ll_t.id
WHERE ll_t.id= "+temp_id.to_s+"
 ) )AND organizations.deactivated=0")
    
  end
end


