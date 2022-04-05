class LibraryOrganization < ActiveRecord::Base
    
    # query created for lib organization API

  def self.fetch_all_library_organization(organizationId)
    self.select("library_organizations.id,library_templates.name as lib_name, organizations.name as org_name, library_organizations.created_at,library_organizations.updated_at,library_organizations.Ref_Lib_Org_Id ")
                       .joins("left join organizations on library_organizations.organization_id = organizations.id ")
                       .joins("left join library_templates on library_organizations.library_template_id = library_templates.id ")
                       .where("organization_id= "+organizationId)
  end
  
end
