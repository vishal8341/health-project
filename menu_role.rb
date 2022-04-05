class MenuRole < ActiveRecord::Base

  #Associations
  belongs_to :menu
  belongs_to :role
  
  
  def self.fetch_menu_by_role(user,organization_id)
    
    
    where("(organization_id is null or organization_id=?) and role_id in (?)",organization_id,user.roles.select("role_id").collect(&:role_id))
    .select("menu_id").collect(&:menu_id)
  end


end
