class HealthCondition < ActiveRecord::Base

  #Associations
  belongs_to :organization
  has_many :health_conditions_users
  has_many :users, through: :health_conditions_users
  has_many :health_conditions_playlists
  has_many :playlists, through: :health_conditions_playlists
  has_many :user_assignments
  has_many :health_condition_standard_codes
  has_many :user_recommended_health_conditions
  
  
  extend FriendlyId
  friendly_id :name, use: :slugged

  #Validations
  validates :name,
            :presence => true
  
  # attr_writer
  attr_accessor :healthcondition_url
  
  # reuse for API
  def self.fetch_by_organization_id(organization_id)
    where("organization_id = ? and deactivated = 0",organization_id)
  end
  
# new method of API for health condition with facility

def self.fetch_by_organization_id_new_with_facility(organization_id,parent_id)
  puts parent_id.inspect;
  whereclause = ""
  if parent_id == 0
    whereclause = "or health_conditions.organization_id in (select id from organizations where parent_id = #{organization_id} )" 
  end   
  HealthCondition.select("health_conditions.id as TopicId,health_conditions.name,health_conditions.description,health_conditions.organization_id as facilityId,health_conditions.icon,health_conditions.department_id,case when dodn.id is null then d.name else dodn.alias end as department_name,op.name as facilityName,case when opp.id is null then op.name else opp.name end as Organization,health_conditions.name as healthcondition_url")
  .joins("left join departments d on d.id = health_conditions.department_id")
  .joins("left join department_organization_display_names dodn on dodn.department_id = d.id and dodn.organization_id = health_conditions.organization_id")
  .joins("left join organizations op on op.id = health_conditions.organization_id")
  .joins("left join organizations opp on opp.id = op.parent_id ")
  .where("(health_conditions.organization_id = ? #{whereclause}) and health_conditions.deactivated = 0",organization_id)
end

def self.validate_healthcondition_map(healthconditionid,organizationid,parent_id)
  whereclause = ""
  if parent_id.nil? 
    whereclause = "or health_conditions.organization_id in (select id from organizations where parent_id = #{organizationid} )" 
  end   
  HealthCondition.select("health_conditions.id" )
  .where("(health_conditions.organization_id = ? #{whereclause}) and health_conditions.id = ? and health_conditions.deactivated = 0 ",organizationid,healthconditionid).first
end
#end
  def self.fetch_by_organization_id_dropdown(org_id)
    where("organization_id = ? and (name like 'Cardiac Catheterization' or name like 'Cardiac Catheterization' or name like 'Diabetes') ",org_id)
  end
  
  def self.fetch_health_condition
    self.all
  end
  def self.count_health_ids(id) 
    ActiveRecord::Base.connection.execute(" SELECT count(user_id)as count FROM health_conditions_users where (user_id='"+id.to_s+"')")
  end
  
  def self.fetch_health_condition_library_count(condition,dept,org_id)
    
    whereclouse = "d.id "
     if !condition.blank?
       whereclouse = whereclouse+" and lib_health_conditions.id = "+condition.to_s
     end
     if !org_id.blank?
       whereclouse = whereclouse+" and lib_health_conditions.organization_id = "+org_id.to_s
     end
     
      if !dept.blank?
       whereclouse = whereclouse+" and d.id = "+dept.to_s
     end
    LibHealthCondition.select('COUNT(lib_health_conditions.id) as h_count')
    .joins("LEFT JOIN departments d ON d.id = lib_health_conditions.department_id ") 
    .where(whereclouse)   
    
  end
  
   def self.fetch_health_condition_library(order_by,sort_direction,condition,dept,page,no_of_records,org_id)
     
     whereclouse = "dept.id "
     if !org_id.blank?
       whereclouse = whereclouse+" and lib_health_conditions.organization_id = "+org_id.to_s
     end
     if !condition.blank?
       whereclouse = whereclouse+" and lib_health_conditions.id = "+condition.to_s
     end
      if !dept.blank?
       whereclouse = whereclouse+" and dept.id = "+dept.to_s
     end
    LibHealthCondition.select('lib_health_conditions.id,lib_health_conditions.name, lib_health_conditions.icon, dept.name departmentName, department_id,lib_health_conditions.icd10')
    .joins("LEFT JOIN departments dept ON dept.id = lib_health_conditions.department_id ") 
    .where(whereclouse)   
    .order(order_by + " " + sort_direction).limit(no_of_records).offset(page)
  end
  
  def self.fetch_health_condition_library_new(order_by,sort_direction,condition,dept,org_id)
       
       whereclouse = ""
       if !org_id.blank?
         whereclouse = (whereclouse.blank?)?("lib_health_conditions.organization_id = "+org_id.to_s):(whereclouse+" and lib_health_conditions.organization_id = "+org_id.to_s)
       end
       if !condition.blank?
         whereclouse = (whereclouse.blank?)?("lib_health_conditions.id = "+condition.to_s):( whereclouse+" and lib_health_conditions.id = "+condition.to_s)
       end
        if !dept.blank?
         whereclouse = (whereclouse.blank?)?("dept.id = "+dept.to_s):(whereclouse+" and dept.id = "+dept.to_s)
       end
      LibHealthCondition.select('lib_health_conditions.id,lib_health_conditions.name, lib_health_conditions.icon, dept.name departmentName, department_id,lib_health_conditions.icd10')
      .joins("LEFT JOIN departments dept ON dept.id = lib_health_conditions.department_id ") 
      .where(whereclouse)   
      .order(order_by + " " + sort_direction)
    end
  #reuse for Health Condition API 
  def self.fetch_health_condition_of_user_by_organization_id(organization_id,user_id)
    joins('JOIN health_conditions_users hcu ON hcu.health_condition_id = health_conditions.id ')
    .where('health_conditions.organization_id=? and hcu.user_id=?',organization_id,user_id).first
  end
  #created for API
  def self.fetch_health_condition_by_user_and_organization_id(organization_id,user_id)
    joins('JOIN health_conditions_users hcu ON hcu.health_condition_id = health_conditions.id ')
    .where('health_conditions.organization_id=? and hcu.user_id=?',organization_id,user_id)
  end
  
  #created for API
  def self.fetch_health_condition_by_user(user_id)
    joins("left join health_conditions on health_conditions_users.health_condition_id = health_conditions.id ")
    joins("left join users on health_conditions_users.user_id = users.id ")
    .select("health_conditions.*")
    .where("where health_conditions_users.user_id = "+user_id).first
  end
  
  
  def self.fetch_all_conditions_counts(organization_id,sort_column,sort_direction)
    sort_clause=sort_column+' '+sort_direction

    self.select('health_conditions.organization_id as hospital_id,health_conditions.name,org.name as hospital_name,health_conditions.id as health_condition_id,libhc.created_at ,libhc.library_template_id,library_created,
    tab_count ,playlist_count,asset_count')
     .joins('JOIN organizations org ON org.id = health_conditions.organization_id')  
     .joins('JOIN library_health_conditions libhc ON libhc.health_condition_id = health_conditions.id')  
     .joins(' LEFT JOIN (SELECT IFNULL( tab.tabc,0)AS tab_count ,IFNULL(playlist.playlist,0) AS playlist_count,IFNULL(asset.asset,0) AS asset_count,library_created,tab.health_condition_id,library_template_id FROM
      (SELECT 
      COUNT( c.playlist_id) AS tabc,health_condition_id,lhc.library_template_id,lhc.created_at as library_created
      
      
      FROM library_health_conditions lhc 
      LEFT JOIN library_playlists lp ON lhc.library_template_id = lp.library_template_id 
      LEFT JOIN playlists ON lp.playlist_id = playlists.id  
      LEFT JOIN collections c ON c.playlist_id= lp.playlist_id AND  c.parent_playlist IS NULL AND c.content_id IS NULL
      WHERE playlists.deactivated = 0
      GROUP BY lhc.health_condition_id
      
      )tab LEFT JOIN 
      
      (SELECT 
      COUNT(DISTINCT c.playlist_id) AS playlist,health_condition_id
      FROM library_health_conditions lhc 
      LEFT JOIN library_playlists lp ON lhc.library_template_id = lp.library_template_id 
      LEFT JOIN playlists ON lp.playlist_id = playlists.id
      LEFT JOIN collections c ON c.playlist_id= lp.playlist_id
      LEFT JOIN playlists ppl ON ppl.id = c.parent_playlist
      WHERE  c.parent_playlist IS NOT NULL AND c.content_id IS NULL AND playlists.deactivated=0 AND ppl.deactivated=0 
      GROUP BY lhc.health_condition_id) playlist ON tab.health_condition_id=playlist.health_condition_id
       LEFT JOIN 
      (SELECT 
      COUNT(c.content_id) AS asset,health_condition_id
      FROM 
      library_health_conditions lhc 
      LEFT JOIN library_playlists lp ON lhc.library_template_id = lp.library_template_id 
      LEFT JOIN playlists ON lp.playlist_id = playlists.id
      LEFT JOIN collections c ON c.playlist_id= lp.playlist_id
      LEFT JOIN playlists ppl ON ppl.id = c.parent_playlist
      WHERE  c.parent_playlist IS NOT NULL AND c.playlist_id IS NOT NULL AND c.content_id IS NOT NULL AND playlists.deactivated=0 AND ppl.deactivated=0  
      GROUP BY lhc.health_condition_id
      )asset ON asset.health_condition_id=playlist.health_condition_id ) s

    ON s.health_condition_id=health_conditions.id')
    .where('health_conditions.organization_id=? ',organization_id)
    .order(sort_clause)
  end
  
  def self.fetch_all_conditions_counts_for_all_count(sort_column,sort_direction,conditions_filter,hospitals_filter,status, user_id,dept,lang,allfacility)
  
          
    wherequery = "hc.id"
   
    if !conditions_filter.blank? 
      wherequery =wherequery+" and hc.id IN ("+conditions_filter.to_s+")"
    end
    if !hospitals_filter.blank?
      if allfacility == false
        wherequery =wherequery+" and  llo.organization_id IN ("+hospitals_filter.to_s+")"
      else
        wherequery =wherequery+" and  llo.organization_id IN (select id from organizations where parent_id in ("+hospitals_filter.to_s+") or id in ("+hospitals_filter.to_s+"))"
      end
    end
    
    if !dept.blank?
      wherequery =wherequery+" and  d.id = "+dept
    end
    
    if !lang.blank? && lang.to_i == Constants::ENGLISH_LOCALE_ID
     wherequery =wherequery+" and ( (lltl.locale_id IN ("+lang.to_s+")) or lltl.locale_id is null)"
   elsif !lang.blank?
     wherequery =wherequery+" and ( lltl.locale_id IN ("+lang.to_s+"))"
   end
    
     
    if  !status.blank?
      wherequery = wherequery + ' AND lp.library_status_master_id = ' + status.to_s + ' AND lib_library_templates.id in (select library_id from library_approvers where is_approved=0 AND user_id = ' + user_id.to_s + ') '
    end
     
      LibLibraryTemplate.select("COUNT(lib_library_templates.id) as total")
     .joins("LEFT JOIN lib_library_template_locales lltl ON lltl.lib_library_template_id=lib_library_templates.id")
     .joins("JOIN lib_library_health_conditions llhc ON llhc.library_template_id=lib_library_templates.id")
     .joins("LEFT JOIN lib_health_conditions hc ON hc.id=llhc.health_condition_id")
     .joins("JOIN lib_library_organizations llo ON llo.library_template_id=lib_library_templates.id")
     .joins("LEFT JOIN library_publishes lp on lp.library_id = lib_library_templates.id")
     .joins("left JOIN departments d on d.id = hc.department_id")     
     .where(wherequery)
     .first
   
   
  
   
 end

 def self.fetch_all_conditions_counts_for_all(sort_column,sort_direction,conditions_filter,hospitals_filter,status, user_id,dept,lang,allfacility)
 
   sort_clause=sort_column+' '+sort_direction
   
   
   
 
         
    wherequery = "hc.id"
   
    if !conditions_filter.blank? 
      wherequery =wherequery+" and hc.id IN ("+conditions_filter.to_s+")"
    end
    if !hospitals_filter.blank?
      if allfacility == false
        wherequery =wherequery+" and  org.id IN ("+hospitals_filter.to_s+")"
      else
        wherequery =wherequery+" and  org.id IN (select id from organizations where parent_id in ("+hospitals_filter.to_s+") or id in ("+hospitals_filter.to_s+"))"
      end
    end
    
    if !dept.blank?
      wherequery =wherequery+" and  d.id = "+dept
    end
    
    if !lang.blank? && lang.to_i == Constants::ENGLISH_LOCALE_ID
     wherequery =wherequery+" and ( (lltl.locale_id IN ("+lang.to_s+")) or lltl.locale_id is null)"
   elsif !lang.blank?
     wherequery =wherequery+" and ( lltl.locale_id IN ("+lang.to_s+"))"
   end
    
     
    if  !status.blank?
      wherequery = wherequery + ' AND lp.library_status_master_id = ' + status.to_s + ' AND lib_library_templates.id in (select library_id from library_approvers where is_approved=0 AND user_id = ' + user_id.to_s + ') '
    end
     
     LibLibraryTemplate.select("case when dodn.alias is null then d.name else dodn.alias end as department,lsm.status,org.id as org_id,case when orgf.id is null then org.name else orgf.name end as hospital_name,lib_library_templates.id as library_template_id, lib_library_templates.name,hc.id as health_condition_id,llhc.health_condition_id as org_health_condition_id,llhc.id as Ref_Lib_Health,CASE WHEN lho.alias IS NULL THEN hc.name ELSE lho.alias END AS healthcondition,
      ct.asset_count,ct.playlist_count,ct.tab_count,org.name as facility_name")
     .joins("left join
      (SELECT count( ct.content_id) as asset_count ,count(distinct ct.playlist_id) as playlist_count,count(distinct ct.parent_playlist) as tab_count,llp.library_template_id
      FROM lib_library_playlists llp
      JOIN lib_playlists p ON p.id=llp.playlist_id
      JOIN lib_collections ct ON ct.playlist_id=p.id AND parent_playlist IS NOT NULL AND content_id IS NOT NULL
      WHERE p.deactivated =0
      group by llp.library_template_id) ct on ct.library_template_id = lib_library_templates.id")
     .joins("LEFT JOIN lib_library_template_locales lltl ON lltl.lib_library_template_id=lib_library_templates.id")
     .joins("JOIN lib_library_health_conditions llhc ON llhc.library_template_id=lib_library_templates.id")
     .joins("LEFT JOIN lib_health_conditions hc ON hc.id=llhc.health_condition_id")
     .joins("JOIN lib_library_organizations llo ON llo.library_template_id=lib_library_templates.id")
     .joins("LEFT JOIN organizations org on org.id = llo.organization_id") 
     .joins("LEFT JOIN organizations orgf on orgf.id = org.parent_id") 
     .joins("LEFT JOIN library_publishes lp on lp.library_id = lib_library_templates.id")
     .joins("LEFT JOIN lib_health_condition_organizations  lho on lho.health_condition_id =hc.id and org.id=lho.organization_id")
     .joins("JOIN library_status_masters lsm on lp.library_status_master_id = lsm.id")
     .joins("left JOIN departments d on d.id = hc.department_id")     
     .joins("left join department_organization_display_names dodn on dodn.department_id = d.id and dodn.organization_id = hc.organization_id")     
     .where(wherequery)
     .order(sort_clause)
    
   
   
  
   
 end
  
  
  def self.search_all_conditions_counts(search_term,organization_id,sort_column,sort_direction)
    
    sort_clause=sort_column+' '+sort_direction
   
    
    
    self.select('health_conditions.organization_id as hospital_id,health_conditions.name,org.name as hospital_name,health_conditions.id as health_condition_id,libhc.created_at,libhc.library_template_id,library_created,
    tab_count ,playlist_count,asset_count')
     .joins('JOIN organizations org ON org.id = health_conditions.organization_id')  
      .joins('JOIN library_health_conditions libhc ON libhc.health_condition_id = health_conditions.id')   
    .joins(' LEFT JOIN (SELECT IFNULL( tab.tabc,0)AS tab_count ,IFNULL(playlist.playlist,0) AS playlist_count,IFNULL(asset.asset,0) AS asset_count,library_created,tab.health_condition_id,library_template_id FROM
      (SELECT 
      COUNT( c.playlist_id) AS tabc,health_condition_id,lhc.library_template_id,lhc.created_at as library_created
      
      
      FROM library_health_conditions lhc 
      LEFT JOIN library_playlists lp ON lhc.library_template_id = lp.library_template_id 
      LEFT JOIN playlists ON lp.playlist_id = playlists.id  
      LEFT JOIN collections c ON c.playlist_id= lp.playlist_id AND  c.parent_playlist IS NULL AND c.content_id IS NULL
      WHERE playlists.deactivated = 0
      GROUP BY lhc.health_condition_id
      
      )tab LEFT JOIN 
      
      (SELECT 
      COUNT(DISTINCT c.playlist_id) AS playlist,health_condition_id
      FROM library_health_conditions lhc 
      LEFT JOIN library_playlists lp ON lhc.library_template_id = lp.library_template_id 
      LEFT JOIN playlists ON lp.playlist_id = playlists.id
      LEFT JOIN collections c ON c.playlist_id= lp.playlist_id
      LEFT JOIN playlists ppl ON ppl.id = c.parent_playlist
      WHERE  c.parent_playlist IS NOT NULL AND c.content_id IS NULL AND playlists.deactivated=0 AND ppl.deactivated=0 
      GROUP BY lhc.health_condition_id) playlist ON tab.health_condition_id=playlist.health_condition_id
       LEFT JOIN 
      (SELECT 
      COUNT(c.content_id) AS asset,health_condition_id
      FROM 
      library_health_conditions lhc 
      LEFT JOIN library_playlists lp ON lhc.library_template_id = lp.library_template_id 
      LEFT JOIN playlists ON lp.playlist_id = playlists.id
      LEFT JOIN collections c ON c.playlist_id= lp.playlist_id
      LEFT JOIN playlists ppl ON ppl.id = c.parent_playlist
      WHERE  c.parent_playlist IS NOT NULL AND c.playlist_id IS NOT NULL AND c.content_id IS NOT NULL AND playlists.deactivated=0 AND ppl.deactivated=0  
      GROUP BY lhc.health_condition_id
      )asset ON asset.health_condition_id=playlist.health_condition_id ) s

    ON s.health_condition_id=health_conditions.id')
    .where('(org.name LIKE ? OR health_conditions.name LIKE ?) and health_conditions.organization_id=? ','%'+search_term+'%','%'+search_term+'%',organization_id)
    .order(sort_clause)
    
    
  end
  
  def self.fetch_counts_by_health_condition_id(health_condition_id)
    self.select('health_conditions.id as health_condition_id,
    tab_count ,playlist_count,asset_count')
    .joins(' LEFT JOIN (SELECT IFNULL( tab.tabc,0)AS tab_count ,IFNULL(playlist.playlist,0) AS playlist_count,IFNULL(asset.asset,0) AS asset_count,library_created,tab.health_condition_id,library_template_id FROM
      (SELECT 
      COUNT( c.playlist_id) AS tabc,health_condition_id,lhc.library_template_id,lhc.created_at as library_created
      
      
      FROM library_health_conditions lhc 
      LEFT JOIN library_playlists lp ON lhc.library_template_id = lp.library_template_id 
      LEFT JOIN playlists ON lp.playlist_id = playlists.id  
      LEFT JOIN collections c ON c.playlist_id= lp.playlist_id AND  c.parent_playlist IS NULL AND c.content_id IS NULL
      WHERE playlists.deactivated = 0
      GROUP BY lhc.health_condition_id
      
      )tab LEFT JOIN 
      
      (SELECT 
      COUNT(DISTINCT c.playlist_id) AS playlist,health_condition_id
      FROM library_health_conditions lhc 
      LEFT JOIN library_playlists lp ON lhc.library_template_id = lp.library_template_id 
      LEFT JOIN playlists ON lp.playlist_id = playlists.id
      LEFT JOIN collections c ON c.playlist_id= lp.playlist_id
      LEFT JOIN playlists ppl ON ppl.id = c.parent_playlist
      WHERE  c.parent_playlist IS NOT NULL AND c.content_id IS NULL AND playlists.deactivated=0 AND ppl.deactivated=0 
      GROUP BY lhc.health_condition_id) playlist ON tab.health_condition_id=playlist.health_condition_id
       LEFT JOIN 
      (SELECT 
      COUNT(c.content_id) AS asset,health_condition_id
      FROM 
      library_health_conditions lhc 
      LEFT JOIN library_playlists lp ON lhc.library_template_id = lp.library_template_id 
      LEFT JOIN playlists ON lp.playlist_id = playlists.id
      LEFT JOIN collections c ON c.playlist_id= lp.playlist_id
      LEFT JOIN playlists ppl ON ppl.id = c.parent_playlist
      WHERE  c.parent_playlist IS NOT NULL AND c.playlist_id IS NOT NULL AND c.content_id IS NOT NULL AND playlists.deactivated=0 AND ppl.deactivated=0  
      GROUP BY lhc.health_condition_id
      )asset ON asset.health_condition_id=playlist.health_condition_id ) s

    ON s.health_condition_id=health_conditions.id')
    .where('health_conditions.id=? ',health_condition_id)
  end
  
  def self.fetch_content_count_of_health_condition(health_condition_ids,role_id)
    select("GROUP_CONCAT(content_id SEPARATOR ',') AS `contentIds`,COUNT(c.content_id) AS totalcontent, health_conditions.id")
    .joins("LEFT JOIN library_health_conditions lhc ON lhc.health_condition_id = health_conditions.id")
    .joins("LEFT JOIN library_playlists lp ON lp.library_template_id =lhc.library_template_id")
    .joins("join library_role_types lrt on lrt.library_template_id=lhc.library_template_id")
    .joins("LEFT JOIN collections c ON c.playlist_id=lp.playlist_id")
    .where("( FIND_IN_SET(health_conditions.id,(?)))  and lrt.role_type_id=? ",health_condition_ids,Role.find_by_id(role_id).try(:role_type_id))
    .group("health_conditions.id")
  end
  
  def self.fetch_videos_count_of_health_condition(health_condition_ids)
    puts 'total videos for health'
    select("COUNT(distinct c.content_id) AS TOTAL_VIDEOS, health_conditions.id")
    .joins("LEFT JOIN library_health_conditions lhc ON lhc.health_condition_id = health_conditions.id")
    .joins("LEFT JOIN library_playlists lp ON lp.library_template_id =lhc.library_template_id")
    .joins("LEFT JOIN collections c ON c.playlist_id=lp.playlist_id")
    .joins("LEFT JOIN contents ON c.content_id=contents.id")
    .where("contents.content_type='ooyala' AND health_conditions.id in (?)",health_condition_ids)
    .group("health_conditions.id")
    .limit(1)
    
    
  end
 
    
  def self.fetch_all()
    self.select("id, name, medical_field, icon")    

  end
  
  def update=(val)
     update = val
  end
  
  def update
  end
  
  
  def self.fetch_welcome_video(id,org_id,lang)
  
   joins("left join collections c on c.playlist_id = health_conditions.welcome_page_playlist ")
  .joins("left join contents ct on ct.id = c.content_id ")
  .joins("left join content_locales ctl on ctl.content_id = ct.id")
  .joins("left join locales l on l.id = ctl.locale_id ")
  .where("health_conditions.id = ? and health_conditions.organization_id = ? and l.name = ?",id,org_id,lang.to_s)
  .select("ct.id as id, ctl.content_ref as content_ref,ctl.bc_video_id,ctl.bc_thumbnail_ref") 
  .limit(1)
end

def self.fetch_healthcondition_by_userId(user_id)
   select("health_conditions.*")
  .joins("left join health_conditions_users on health_conditions_users.health_condition_id = health_conditions.id ")
  .joins("left join users on users.id = health_conditions_users.user_id ")
  .where("users.id = "+user_id.to_s+"")
end

#-----------heal-4347
def self.fetch_concated_healthconditions_by_org(org_id)
  select(" group_concat(id) as ids ")
  .where(" organization_id=? ",org_id)
end

end
