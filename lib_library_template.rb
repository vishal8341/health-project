class LibLibraryTemplate < ActiveRecord::Base
  has_many :lib_library_health_conditions, foreign_key: "library_template_id"
  has_many :lib_health_conditions, through: :lib_library_health_conditions
  has_many :lib_library_playlists
  has_many :lib_playlists, through: :lib_library_playlists
  has_many :lib_library_organizations
  has_many :organizations, through: :lib_library_organizations
  has_many :library_publishes, foreign_key: "library_id"
  has_many :lib_role_types, foreign_key: "library_template_id"
  
  
  
  def self.find_playlists_by_template(template_id)
    joins('join lib_library_playlists ON  lib_playlists.id = lib_library_playlists.playlist_id')
    .where('lib_library_playlists.library_template_id = ',template_id)
    
  end
  
  def self.publishLibrary(library_id, role_type_id,message,isSucceed)
    ActiveRecord::Base.connection.execute("call PublishLibraryMulti(" + library_id.to_s + ","+role_type_id.to_s+",@message,@isSuceed)")
    return ActiveRecord::Base.connection.execute("select @isSuceed  as message").first
  end
  
  def self.deleteLibrary(library_id)
    ActiveRecord::Base.connection.execute("call DeleteLibrary(" + library_id.to_s + ")")    
  end

  def self.assign_new_health_conditions_to_users(health_condition_id,org_id)

    ActiveRecord::Base.connection.execute("insert into health_conditions_users(user_id,health_condition_id,
    created_at,updated_at)
    select t.user_id,"+health_condition_id.to_s+" ,now(),now()
    from 
    (select 
    distinct ou.user_id
    from  organization_users ou 
      join health_conditions_users hcu on hcu .user_id = ou.user_id
      join roles_users ru on ru.user_id = ou.user_id
      join health_conditions hc on hc.id = hcu.health_condition_id
      where ru.role_id in (1,16) and ou.organization_id = "+org_id.to_s+" and ou.user_id not in 
      (select 
      distinct ou.user_id
      from organization_users ou 
      join health_conditions_users hcu on hcu .user_id = ou.user_id
      join roles_users ru on ru.user_id = ou.user_id
      join health_conditions hc on hc.id = hcu.health_condition_id
      where ru.role_id in (1,17) and ou.organization_id = "+org_id.to_s+" and hcu.health_condition_id = "+health_condition_id.to_s+"))t")
    end

end
