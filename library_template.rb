class LibraryTemplate < ActiveRecord::Base
  has_many :library_health_conditions
  has_many :health_conditions, through: :library_health_conditions
  has_many :library_playlists
  has_many :playlists, through: :library_playlists
  has_many :library_organizations
  has_many :organizations, through: :library_organizations
  
  def self.find_by_organization_id(organization_id)
    joins('join library_organizations lo ON lo.library_template_id=library_templates.id')
    .where('lo.organization_id=? and library_templates.copied_from_library IS NULL ',organization_id)
    
  end
  
  def self.find_procedure_by_template_id(template_id,organization_id)
    joins('join library_health_conditions lhc on (lhc.library_template_id=library_templates.id 
    and library_templates.id in (select id from library_templates where copied_from_library = '+ template_id +' ))')
    .joins('join health_conditions hc on (hc.id = lhc.health_condition_id)')
    .where('hc.organization_id=?',organization_id)
    .select('hc.name')    
  end
  
  def self.fetch_playlists_of_health_condition(health_condition_id,decision_making=0,cohort_id=0,role_id)
   
    LibraryPlaylist.joins("join library_health_conditions lhc on lhc.library_template_id=library_playlists.library_template_id")
    .joins("join playlists p on p.id=library_playlists.playlist_id")
    .joins("join library_role_types lrt on lrt.library_template_id=library_playlists.library_template_id")
    .where("lhc.health_condition_id=? and lrt.role_type_id=? and (p.decision_making = ? or p.decision_making = 0) and (p.cohort_id = ? or p.cohort_id = 0)",health_condition_id, Role.find_by_id(role_id).try(:role_type_id),decision_making,cohort_id)
  end

end
