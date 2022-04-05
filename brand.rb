class Brand < ActiveRecord::Base
  
  def self.fetch_brand_by_user_id(user_id)
    
    self.where("b.user_id = ?",user_id)
    joins("join user_brands ub on b.id=ub.brand_id")
  end
  
  def self.fetch_brands_organization(org_id)
    
    self.joins("join organization_brands on organization_brands.brand_id = brands.id")
    .where("organization_brands.organization_id = ?",org_id)
    .select("brands.id AS brand_id,brands.name")    
    
  end
  
  def self.fetch_other_brand()    
    self.where("brand_type = 3").select("id").first
  end

  
end
