class LibHealthCondition < ActiveRecord::Base
  has_many :lib_health_condition_standard_codes, foreign_key: "health_condition_id"
  belongs_to :department, foreign_key: "department_id"
  has_many :lib_library_health_conditions, foreign_key: "health_condition_id"
  has_many :lib_library_templates, through: :lib_library_health_conditions
  has_many :lib_health_condition_organizations, foreign_key: "health_condition_id"
  has_many :lib_health_condition_role_types, foreign_key: "lib_health_condition_id"
  accepts_nested_attributes_for :lib_health_condition_standard_codes
  extend FriendlyId
  friendly_id :name, use: :slugged
  #Reuse for Healthcondition API
  def self.fetch_by_organization_id(organization_id)
    where("organization_id = ?",organization_id)
  end
  
  def self.fetch_health_condition
    self.all
  end
  
  def self.fetch_health_condition_library(order_by,sort_direction,org_id)
     whereclouse = "lib_health_conditions.id "
     if !org_id.blank?
       whereclouse = whereclouse+" and lib_health_conditions.organization_id = "+org_id.to_s
     end
    
    self.select('lib_health_conditions.id,lib_health_conditions.name, lib_health_conditions.icon, d.name departmentName, department_id')
    .joins("LEFT JOIN departments d ON d.id = lib_health_conditions.department_id ")    
    .order("lib_health_conditions." + order_by + " " + sort_direction)
    .where(whereclouse)   
  end
  
  def update=(val)
     update = val
  end
  
  def update
  end

  def self.fetch_health_condition_alias(health_condition_id)
    select("lib_health_conditions.id,lhco.alias as alias,lhco.id as lib_health_alias_id,lhco.organization_id as organization_id,o.name as organization_name")
    .joins("LEFT JOIN lib_health_condition_organizations lhco on lhco.health_condition_id =lib_health_conditions.id")
    .joins("LEFT JOIN organizations o on o.id = lhco.organization_id ")
    .where(" lib_health_conditions.id=?",health_condition_id)
  end
end
