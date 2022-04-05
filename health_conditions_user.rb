class HealthConditionsUser < ActiveRecord::Base

  #Associations
  belongs_to :user
  belongs_to :health_condition

  #Attribute Accessors
  attr_accessor :proc_date
  attr_accessor :proc_time

  #Validations
 # validates :proc_date,
  #          :presence => true
  #validates :proc_date,
   #         :presence => true
  validates :health_condition_id,
            :presence => true

  #TODO find a way to fix it with new user
  # validates :user,
  #           :presence => true

  #Callbacks
  before_save :merge_date_time

  def merge_date_time
    require 'date'
    if self.proc_date.is_a?(Hash) && self.proc_time.is_a?(String)
      a = DateTime.parse(self.proc_time)
      self.procedure_date = a.change(year: self.proc_date[1], month: self.proc_date[2], day: self.proc_date[3])
    end
  end

  def self.fetch_patients_counts_by_health_condition_all
    select("health_conditions.id,health_conditions.name,COUNT(user_id) AS patients")
    .joins("JOIN health_conditions ON health_conditions.id = health_conditions_users.health_condition_id")
    .group("health_conditions_users.health_condition_id")
  end
  
  def self.fetch_patients_counts_by_health_condition_id(health_id)
    where("health_conditions.id NOT IN (?)",health_id)
    .select("health_conditions.id,health_conditions.name,COUNT(user_id) AS patients")
    .joins("JOIN health_conditions ON health_conditions.id = health_conditions_users.health_condition_id")
    .group("health_conditions_users.health_condition_id")
  end
  
  # Query for per video reminder email
  def self.fetch_user_not_watched_content(health_condition_ids,hours)
    role = Role.find_by_name(Constants::ROLE_PATIENT).id
    content = HealthCondition.fetch_content_count_of_health_condition(health_condition_ids).first
    
    User.joins("LEFT JOIN health_conditions_users ON users.id = health_conditions_users.user_id")
    .joins("LEFT JOIN organization_users ON users.id = organization_users.user_id")
    .joins("INNER JOIN `roles_users` ON `users`.`id` = `roles_users`.`user_id` ")
    .joins("LEFT JOIN (select min(login_time) as login_time,user_id from login_histories group by user_id) as lh on lh.user_id = health_conditions_users.user_id")
    .select("health_conditions_users.user_id as user_id,COUNT(ucc.content_id) content_watched_count,(COUNT(ucc.content_id)/"+content.totalcontent.to_s+")*100 as content_watched_perc,
     users.*,lh.login_time as login_time,AES_DECRYPT(UNHEX(users.first_name), 'Mytonomy') AS 'first_name',AES_DECRYPT(UNHEX(users.last_name), 'Mytonomy') AS 'last_name'")
    .joins("LEFT JOIN user_content_consumes ucc ON ucc.user_id = health_conditions_users.user_id")
    .where("( FIND_IN_SET(health_conditions_users.health_condition_id,(?)))  AND users.Has_Email = true 
    AND ( FIND_IN_SET(ucc.content_id ,(?)) OR ucc.content_id IS NULL) AND lh.login_time BETWEEN ADDDATE(NOW(),-7) AND NOW()
     and TIMESTAMPDIFF(hour,lh.login_time, NOW()) > ? and organization_users.deactivated = 0 and roles_users.role_id = ? and users.demo = 0  and users.created_at > '2017-09-26 00:00:00'",health_condition_ids,content.contentIds,hours,role)
    .group("user_id")

    
  end
  
 
  
  # Query for per video reminder email
  def self.fetch_user_done_pre_survey_content(days,survey_id)
    
    
    Question.select("COUNT(sua.question_id) AS ques_answered,(SELECT COUNT(DISTINCT ucc.id) 
    FROM user_content_consumes ucc 
    WHERE ucc.user_id=users.id AND  ucc.content_id IS NOT NULL 
    AND ucc.percent_consumed=100) AS Watched_Count,sua.user_id,max(sua.updated_at) as 
    last_answred,users.*, DATEDIFF(NOW(),sua.updated_at) as survey_time, AES_DECRYPT(UNHEX(users.first_name), 'Mytonomy') AS 'first_name',AES_DECRYPT(UNHEX(users.last_name), 'Mytonomy') AS 'last_name'")
    .joins("JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id")
    .joins("LEFT  JOIN `survey_user_answers` sua  ON questions.id=sua.`question_id`")
    .joins("LEFT JOIN users ON sua.user_id=users.id OR sua.user_id IS NULL ")
    .joins("LEFT JOIN organization_users ON users.id = organization_users.user_id")
    .joins("JOIN user_preferences on user_preferences.user_id = users.id")
    .select("user_preferences.language AS 'language'")
    .where("users.Has_Email = true and organization_users.deactivated = 0 and sst.survey_id =? and users.created_at > '2017-04-24 00:00:00' and sua.updated_at > '2017-09-23 03:32:56' and users.demo = 0",survey_id)
    .group("users.id")
    .having("Watched_Count > 3 and COUNT(sua.question_id)= 
    (SELECT COUNT(*) FROM 
    questions JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id WHERE sst.`survey_id`=?) 
    AND DATEDIFF(NOW(),last_answred)> ?",survey_id,days)
    
  end
  
  def self.fetch_survey_status(user_id,survey_name)
    
    Question.select("sua.*")
    .joins("JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id")
    .joins("JOIN `survey_masters` sm ON sm.id=sst.survey_id")
    .joins("LEFT  JOIN `survey_user_answers` sua  ON questions.id=sua.`question_id`")
    .where("sm.name =? and user_id=?",survey_name,user_id)
    .having(" COUNT(sua.question_id)= 
    (SELECT COUNT(*) FROM 
    questions JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id JOIN `survey_masters` sm ON sm.id=sst.survey_id WHERE sm.`name`=?) 
    ",survey_name)
    
  end
  
def self.fetch_survey_status_by_survey_id(user_id,survey_id)
    
    Question.select("sua.*")
    .joins("JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id")
    .joins("JOIN `survey_masters` sm ON sm.id=sst.survey_id")
    .joins("LEFT  JOIN `survey_user_answers` sua  ON questions.id=sua.`question_id`")
    .where("sm.id =? and user_id=?",survey_id,user_id)
    .having(" COUNT(sua.question_id)= 
    (SELECT COUNT(*) FROM 
    questions JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id JOIN `survey_masters` sm ON sm.id=sst.survey_id WHERE sm.id=?) 
    ",survey_id)
    
  end    
    
  def self.fetch_humc_outpatient_users(patient_type, role_id,notification)
    #result = ActiveRecord::Base.connection.execute("SELECT hcu.user_id as user_id,hcu.procedure_date as procd_date,u.phone_no as phone_no,    u.created_at as created_at from health_conditions_users hcu
     #join users u on u.id =hcu.user_id        join organization_users ou on ou.user_id = u.id join roles_users ru on ru.user_id = u.id  where hcu.procedure_date is not null and u.patient_type = " + patient_type.to_s + " and ru.role_id= " + role_id.to_s + " and u.demo=0 
     #  and ou.deactivated = 0 and ou.organization_id = " + org_id.to_s)
     #  Rails.logger.info("fetch_humc_outpatient_users #{result}")
     #  return result

     health_cond=""
    if !notification.health_condition_ids.nil?
      health_cond="and FIND_IN_SET( health_conditions_users.health_condition_id, '"+notification.health_condition_ids+"')"
    end
    notification_type_cond=(notification.notification_type_id.to_i==1? " u.has_email=1 and (u.mail_status is null or u.mail_status <> 1) " : " u.phone_no is not null and u.phone_no<>'' ")

    group_join=""
    group_condition=""    
    if !notification.group_ids.nil? && !notification.group_ids.blank?
      group_join=" LEFT JOIN user_units uu on uu.user_id=ou.user_id "
      group_condition=" AND FIND_IN_SET( uu.unit_id, '"+notification.group_ids.to_s+"')"
    end

      select("health_conditions_users.user_id as user_id,health_conditions_users.procedure_date as procd_date,health_conditions_users.health_condition_id as hcc_id, u.phone_no as phone_no,u.email as email, u.created_at  as created_at")
    .joins("join users u on u.id = health_conditions_users.user_id ")
    .joins(" left join notification_logs nlog on nlog.user_id=u.id and nlog.notification_master_id=#{notification.id.to_i} and nlog.organization_id=#{notification.organization_id.to_i} ")
    .joins("join organization_users ou on ou.user_id = u.id ")
    .joins(group_join)
     .joins(" join roles_users ru on ru.user_id = u.id ")
     .where(" nlog.id is null and TIMESTAMPDIFF(DAY, NOW(),health_conditions_users.procedure_date) <= ?  and (u.patient_type = ?) 
     and (ru.role_id = ?) and u.created_at>='2019-03-07' and u.demo=0 and ou.deactivated = 0 
     and ou.organization_id = ? and #{notification_type_cond} #{group_condition} "+health_cond,notification.type_no.to_i-1,patient_type, role_id, notification.organization_id)   
         
   end

   def self.fetch_humc_noprocdate_users(inpatient, outpatient, pat_role_id,caregiver_role_id, notification)
    
    health_cond=""
    if !notification.health_condition_ids.nil?
      health_cond="and FIND_IN_SET( health_conditions_users.health_condition_id, '"+notification.health_condition_ids+"')"
    end

    group_join=""
    group_condition=""    
    if !notification.group_ids.nil? && !notification.group_ids.blank?
      group_join=" LEFT JOIN user_units uu on uu.user_id=ou.user_id "
      group_condition=" AND FIND_IN_SET( uu.unit_id, '"+notification.group_ids.to_s+"')"
    end

    notification_type_cond=(notification.notification_type_id.to_i==1? " u.has_email=1 and (u.mail_status is null or u.mail_status <> 1) " : " u.phone_no is not null and u.phone_no<>'' ")
    
    select("health_conditions_users.user_id as user_id,health_conditions_users.procedure_date as procd_date,health_conditions_users.health_condition_id as hcc_id, 
            u.phone_no as phone_no,u.email as email,u.created_at  as created_at")
    .joins("join users u on u.id =health_conditions_users.user_id ")
    .joins(" left join notification_logs nlog on nlog.user_id=u.id and nlog.notification_master_id=#{notification.id.to_i} and nlog.organization_id=#{notification.organization_id.to_i} ")
    .joins("join organization_users ou on ou.user_id = u.id ")
    .joins(group_join)
     .joins("join roles_users ru on ru.user_id = u.id ")
     .where(" nlog.id is null and (health_conditions_users.procedure_date is  null || health_conditions_users.do_not_know = 1) and (u.patient_type =? or u.patient_type =? ) 
             and (ru.role_id = ? or ru.role_id=?) and u.created_at>='2019-03-07' 
             and TIMESTAMPDIFF(DAY, u.created_at, NOW()) >= ? and u.demo=0 and ou.deactivated = 0 
             and ou.organization_id = ? and #{notification_type_cond} #{group_condition}"+health_cond.to_s,inpatient, outpatient, pat_role_id,caregiver_role_id, notification.type_no, notification.organization_id)
    end

    def self.fetch_humc_procdate_users(notification_master)            
      health_cond=""
     if !notification_master.health_condition_ids.nil?
       health_cond="and FIND_IN_SET( health_conditions_users.health_condition_id, '"+notification_master.health_condition_ids+"')"
     end
     
     notification_type_cond=(notification_master.notification_type_id.to_i==1? " u.has_email=1 and (u.mail_status is null or u.mail_status <> 1) " : " u.phone_no is not null and u.phone_no<>'' ")
     
     before_or_after_condition=" <= "
     if !notification_master.type_id.nil? && notification_master.type_id == 4
        before_or_after_condition=" >= "            
     end

     group_join=""
    group_condition=""    
    if !notification_master.group_ids.nil? && !notification_master.group_ids.blank?
      group_join=" LEFT JOIN user_units uu on uu.user_id=ou.user_id "
      group_condition=" AND FIND_IN_SET( uu.unit_id, '"+notification_master.group_ids.to_s+"')"
    end
 
       select("health_conditions_users.user_id as user_id,health_conditions_users.procedure_date as procd_date,
         health_conditions_users.health_condition_id as hcc_id, u.phone_no as phone_no,u.email as email")
     .joins("join users u on u.id = health_conditions_users.user_id ")
     .joins(" left join notification_logs nlog on nlog.user_id=u.id and nlog.notification_master_id=#{notification_master.id.to_i} and nlog.organization_id=#{notification_master.organization_id.to_i} ")
     .joins("join organization_users ou on ou.user_id = u.id ")
     .joins(group_join)
     .joins(" join roles_users ru on ru.user_id = u.id ")
     .where(" nlog.id is null and health_conditions_users.procedure_date is not null and #{notification_type_cond} and TIMESTAMPDIFF(DAY, CONVERT_TZ(health_conditions_users.procedure_date,'UTC','US/Eastern'), CONVERT_TZ(now(),'UTC','US/Eastern') ) #{before_or_after_condition} ? 
      and date(u.created_at) >= date('2020-03-27') and u.demo=0 and ou.deactivated = 0 
      and ou.organization_id = ? #{group_condition} "+health_cond,notification_master.type_no,notification_master.organization_id)
    end
  
    def self.fetch_procedure_date_listing(user_id,org_id)
      select("health_conditions_users.procedure_date,health_conditions_users.do_not_know,health_conditions_users.user_id,health_conditions_users.id,hc.name")
      .joins("join users u on u.id = health_conditions_users.user_id ")
      .joins("join organization_users ou on ou.user_id = u.id")
      .joins("join health_conditions hc on hc.id=health_conditions_users.health_condition_id")
      .where("ou.organization_id=? and health_conditions_users.user_id=? and health_conditions_users.do_not_know is not null", org_id,user_id)
      #.order("health_conditions_users.procedure_date ")
    end

    def self.fetch_viewership_data_for_activity_page(user_id)
      select("tab.name as tab_name,pl.name as playlist_name,count(distinct c.content_id) as total_videos,count(distinct ucc.id) unique_watch,
              count(distinct ucl.id) total_watch,group_concat(distinct ucc.content_id) as content_ids")
    .joins("join health_conditions_playlists hcp on hcp.health_condition_id = health_conditions_users.health_condition_id")
    .joins("left join collections c on c.playlist_id =hcp.playlist_id and  c.content_id is not null")
    .joins("left join playlists tab on tab.id = c.parent_playlist and tab.deactivated = 0")
    .joins("left join playlists pl on pl.id = c.playlist_id and pl.deactivated = 0")
    .joins("left join user_content_consumes ucc on ucc.content_id = c.content_id and ucc.user_id = health_conditions_users.user_id and ucc.playlist_id is not null")
    .joins("left join user_consumption_logs ucl on ucl.ucc_id = ucc.id and ucl.skip_flag = 0")
    .where("health_conditions_users.user_id =? and c.id is not null",user_id)
    .group("tab.id,pl.id")
    end

end
