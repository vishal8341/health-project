class MenuAlias < ActiveRecord::Base

  #Associations
  belongs_to :menu
  belongs_to :organization

end
