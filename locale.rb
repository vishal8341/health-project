class Locale < ActiveRecord::Base

  #Validations
  validates :name,
            :presence => true
  validates :language,
            :presence => true

end
