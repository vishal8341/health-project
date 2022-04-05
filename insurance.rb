class Insurance < ActiveRecord::Base

  
  def self.select_insurance(lang)
    joins("LEFT JOIN insurances_locales i on i.insurance_id = insurances.id").joins("LEFT JOIN locales l on l.id = i.locale_id").where("l.name ='" + lang.to_s + "'").select("i.insurance as insurance,i.insurance_id as id ")
  end
end
