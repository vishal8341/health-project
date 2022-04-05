class Ethnicity < ActiveRecord::Base
  

  
  def self.fetch_ethinicity_as_string
    self.select("concat(\"'\",group_concat(ethinicity SEPARATOR \"','\"),\"'\") AS eth_string")    
  end
  
  def self.select_race(lang)
    joins("LEFT JOIN ethnicities_locales e on e.ethnicities_id = ethnicities.id").joins("LEFT JOIN locales l on l.id = e.locale_id").where("ethnicities.deactivated = 0 and l.name ='" + lang.to_s + "'").order("ethnicities.seq_no ASC").select("e.ethnicities_id as id,e.ethinicity as ethinicity")
   end
end
