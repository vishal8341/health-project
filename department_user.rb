class DepartmentUser < ActiveRecord::Base
  #Association
    belongs_to :user
    belongs_to :department
    
    #Validations
    validates :department_id,
            :presence => true
end
