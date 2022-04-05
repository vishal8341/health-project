class Menu < ActiveRecord::Base

  #Associations
  has_many :restricted_menus
  has_many :organizations, through: :restricted_menus
  has_many :menu_roles
  has_many :roles, through: :menu_roles

  #Validations
  validates :name,
            :presence => true
            
  # constants
      MENU = {
        'patient' => {name:"Patient",page: "/patients",path:"patients_path"},
        'dashboard' => {name:"Dashboard",page: "/dashboard",path: "dashboard_index_path"},
        'library' => {name:"Library",page: "/library",path: "library_index_path"}
      }

end
