class Dashboard
  FOUR_HRS="4"
  EIGHT_HRS="8"
  TWELVE_HOURS="12"
  TWENTY_FOUR_HRS ="24 Hours"
  ONE_WEEK = "1 Week"
  FOUR_WEEK = "4 Weeks"
  SIX_MONTHS = "6 Months"
  TWELVE_MONTHS = "12 Months"
  
  
  #-------------------corporate start----------------------------
def self.fetch_product_page(start_date,end_date,org_id,proc_id,department,demographic,gender)
    
      where_condition = "ru.role_id=1"  
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
   if !org_id.blank?
      where_clause=where_clause+" and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')"
    end
    
    
    if !department.blank?
      where_clause = where_clause+ " and hc.department_id = "+department.to_s
     
    end
     if !demographic.blank?
      where_clause = where_clause+ " and users.ethinicity = "+demographic.to_s
     
    end
    
     if !gender.blank?
      where_clause = where_clause+ " and CONVERT(AES_DECRYPT(UNHEX(users.gender), 'Mytonomy') USING latin1) = '"+gender.to_s+"'"
     
    end
    
    
    ActiveRecord::Base.connection.execute("SELECT AVG(Minutes) as avgtime,menu_clicked FROM
(SELECT TIMESTAMPDIFF(MINUTE,un.created_at, un.updated_time) as minutes,
un.created_at, un.updated_time,un.user_id,un.`menu_clicked`,u.organization_id
FROM user_navigations un
JOIN users u on u.id=un.user_id 
JOIN organization_users ou ON ou.user_id = u.id
WHERE un.menu_clicked IN ('instructions','patients','sign_in','dashboard','nurses','survey') and (DATE(un.created_at) BETWEEN  '"+start_date.to_s+"' AND '"+end_date.to_s+"') AND u.demo = 0   and un.updated_time is not null)t
GROUP BY t.menu_clicked")

end


def self.fetch_patient_registration_time(start_date,end_date,org_id,proc_id,group_str,department,demographic,gender)
    
      where_condition = "ru.role_id=1"  
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
   if !org_id.blank?
      where_clause=where_clause+" and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')"
    end
    
    
    if !department.blank?
      where_clause = where_clause+ " and hc.department_id = "+department.to_s
     
    end
     if !demographic.blank?
      where_clause = where_clause+ " and users.ethinicity = "+demographic.to_s
     
    end
    
     if !gender.blank?
      where_clause = where_clause+ " and CONVERT(AES_DECRYPT(UNHEX(users.gender), 'Mytonomy') USING latin1) = '"+gender.to_s+"'"
     
    end
    
   
    
    
    ActiveRecord::Base.connection.execute("SELECT "+group_str.to_s+"(created_at),
AVG(CASE WHEN created='admin' THEN  timetaken END) AS admin,AVG(CASE WHEN created='self' THEN timetaken  END) AS self ,

DATE_FORMAT(DATE_ADD(created_at, INTERVAL(1-DAYOFWEEK(created_at)) DAY),'%m/%d') AS week_start,
DATE_FORMAT(DATE_ADD(created_at, INTERVAL(7-DAYOFWEEK(created_at)) DAY),'%m/%d') AS week_end FROM

 (SELECT 
CASE
 WHEN (users.created_by = users.id) THEN 'self'
 ELSE 'admin'
 END created,
 DATE(users.created_at) AS created_at,
  
 TIMESTAMPDIFF(SECOND,users.`start_time`,users.created_at) AS timetaken
 FROM users 
 JOIN organization_users ou ON ou.user_id = users.id
JOIN health_conditions_users hcu ON hcu.user_id = users.id
JOIN health_conditions hc ON hc.id = hcu.health_condition_id
JOIN roles_users ru ON ru.user_id=users.id
WHERE "+where_clause +" AND users.start_time IS NOT NULL AND DATE(users.created_at) BETWEEN '"+start_date.to_s+"' AND '"+end_date.to_s+"' AND users.demo = 0

 )AS USER
 GROUP BY "+group_str.to_s+"(created_at)
 ORDER BY created_at ASC")
    
    
  end



  def self.fetch_video_load_time(start_date,end_date,org_id,proc_id,department,demographic,gender)
    
      where_condition = "ru.role_id=1"  
  if !proc_id.blank?
    where_clause = where_condition+ " and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
   if !org_id.blank?
      where_clause=where_clause+" and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')"
    end
    
     if !department.blank?
      where_clause = where_clause+ " and hc.department_id = "+department.to_s
     
    end
     if !demographic.blank?
      where_clause = where_clause+ " and u.ethinicity = "+demographic.to_s
     
    end
    
     if !gender.blank?
      where_clause = where_clause+ " and CONVERT(AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') USING latin1) = '"+gender.to_s+"'"
     
    end
    
    
    
  
  UserConsumptionLog.select("ucc.content_id,ct.name as vid_name, avg(user_consumption_logs.load_time) as load_time")
  .joins("join user_content_consumes ucc on user_consumption_logs.ucc_id = ucc.id")
  .joins("JOIN users u ON u.id= ucc.user_id")
  .joins("JOIN contents ct ON ct.id=ucc.content_id")
  .joins("JOIN roles_users ru ON ru.user_id=u.id")
  .joins("JOIN organization_users ou on ou.user_id = u.id") 
  .joins("join health_conditions_users hcu on hcu.user_id = u.id")
  .joins("join health_conditions hc on hc.id = hcu.health_condition_id")
  .where(where_clause+" and user_consumption_logs.load_time is not null and DATE(ucc.updated_at) BETWEEN ? and ? and u.demo = 0",start_date,end_date)
  .group("ct.id").order("avg(user_consumption_logs.load_time) DESC").limit(10)
  
  end
  
  
  
  
  
  def self.fetch_device_type(start_date,end_date,org_id,proc_id,department,demographic,gender)
    
      where_condition = "ru.role_id=1"  
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
   if !org_id.blank?
      where_clause=where_clause+" and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')"
    end
    
  if !department.blank?
      where_clause = where_clause+ " and hc.department_id = "+department.to_s
     
    end
     if !demographic.blank?
      where_clause = where_clause+ " and u.ethinicity = "+demographic.to_s
     
    end
    
     if !gender.blank?
      where_clause = where_clause+ " and CONVERT(AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') USING latin1) = '"+gender.to_s+"'"
     
    end
  
  
   ActiveRecord::Base.connection.execute("select OS, COUNT(OS) AS freq FROM 
(SELECT 
 CASE
    WHEN ((user_agent LIKE '%iphone%' OR user_agent LIKE '%iPhone%' OR user_agent LIKE '%Windows mobile%' OR user_agent LIKE '%Windows phone%' OR user_agent LIKE '%Windows Phone%' OR user_agent LIKE '%Nexus 5%' OR user_agent LIKE '%GTI-9300%' OR user_agent LIKE '%Nokia%' OR user_agent LIKE '%SGH-M919V%' OR user_agent LIKE '%SCH-%' OR user_agent LIKE '%Mobile%' OR user_agent LIKE '%Opera mini%') AND (user_agent NOT LIKE '%iPad%')) THEN 'Mobile'
    WHEN ((user_agent LIKE '%Windows%' OR user_agent LIKE '%WOW64%' OR user_agent LIKE '%Intel Mac OS%' OR user_agent LIKE '%Windows NT 6.1; Trident/7.0%' OR user_agent LIKE '%Media Center PC%') AND (user_agent NOT LIKE '%iPad%')) THEN 'Laptop'
    WHEN (user_agent LIKE '%Tablet PC%' OR user_agent LIKE '%Touch%' OR user_agent LIKE '%MyPhone%' OR user_agent LIKE '%iPad%' OR user_agent LIKE '%ipad%' OR user_agent LIKE '%Tablet%') THEN 'Tablet'
    ELSE
    'Other'
END OS
  FROM login_histories lh
  join users u on u.id = lh.user_id
  join organization_users ou on ou.user_id = u.id
  join health_conditions_users hcu on hcu.user_id = u.id
  JOIN roles_users ru ON ru.user_id=u.id
  join health_conditions hc on hc.id = hcu.health_condition_id
  where "+where_clause +" and DATE(lh.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0 ) AS osses 
GROUP BY OS 
ORDER BY freq DESC")
  
  end
  
  
  def self.fetch_broswer_type(start_date,end_date,org_id,proc_id,department,demographic,gender)
    
      where_condition = "ru.role_id=1"  
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
   if !org_id.blank?
      where_clause=where_clause+" and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')"
    end
    
    
    if !department.blank?
      where_clause = where_clause+ " and hc.department_id = "+department.to_s
     
    end
     if !demographic.blank?
      where_clause = where_clause+ " and u.ethinicity = "+demographic.to_s
     
    end
    
     if !gender.blank?
      where_clause = where_clause+ " and CONVERT(AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') USING latin1) = '"+gender.to_s+"'"
     
    end
  
   ActiveRecord::Base.connection.execute("SELECT browser, COUNT(browser) AS freq FROM 
(SELECT 
CASE
   WHEN user_agent LIKE '%Chrome%' THEN 'Chrome'
   WHEN user_agent LIKE '%Safari%' THEN 'Safari'
   WHEN user_agent LIKE '%Firefox%' THEN 'Firefox'
   WHEN user_agent LIKE '%MSIE%' THEN 'IE'
   WHEN user_agent LIKE '%rv:11%' THEN 'IE'
   ELSE
   'Other'
   END browser
  FROM login_histories lh
  join users u on u.id = lh.user_id
  join organization_users ou on ou.user_id = u.id
  join health_conditions_users hcu on hcu.user_id = u.id
  join health_conditions hc on hc.id = hcu.health_condition_id  
  JOIN roles_users ru ON ru.user_id=u.id
  where "+where_clause +" and DATE(lh.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0 ) AS browsers 
GROUP BY browser 
ORDER BY freq DESC")
  
  end
  
  
  def self.fetch_video_with_lowest_usage(start_date,end_date,org_id,proc_id,department,demographic,gender)
    
      where_condition = "ru.role_id=1"  
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
   if !org_id.blank?
      where_clause=where_clause+" and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')"
    end
    
    if !department.blank?
      where_clause = where_clause+ " and hc.department_id = "+department.to_s
     
    end
     if !demographic.blank?
      where_clause = where_clause+ " and u.ethinicity = "+demographic.to_s
     
    end
    
     if !gender.blank?
      where_clause = where_clause+ " and CONVERT(AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') USING latin1) = '"+gender.to_s+"'"
     
    end
  
  UserContentConsume.select("COUNT(DISTINCT ucl.id) AS totalViews ,COUNT(DISTINCT user_content_consumes.user_id) AS uniquewatch, ct.name as vid_name")
  .joins("JOIN users u ON u.id= user_content_consumes.user_id")
  .joins("JOIN contents ct ON ct.id=user_content_consumes.content_id")
  .joins("JOIN user_consumption_logs ucl on ucl.ucc_id = user_content_consumes.id")  
  .joins("JOIN roles_users ru ON ru.user_id=u.id")
  .joins("JOIN organization_users ou on ou.user_id = u.id") 
  .joins("join health_conditions_users hcu on hcu.user_id = u.id")
  .joins("join health_conditions hc on hc.id = hcu.health_condition_id")
  .where(where_clause+"  AND user_content_consumes.percent_consumed = 100 
  and DATE(user_content_consumes.updated_at) BETWEEN ? and ? and u.demo = 0",start_date,end_date)
  .group("ct.id").order("COUNT(ucl.id) ASC").limit(7)
  
  end
  
  
  
  def self.fetch_video_with_highest_usage(start_date,end_date,org_id,proc_id,department,demographic,gender)
    
      where_condition = "ru.role_id=1"  
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
   if !org_id.blank?
      where_clause=where_clause+" and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')"
    end
    
    if !department.blank?
      where_clause = where_clause+ " and hc.department_id = "+department.to_s
     
    end
     if !demographic.blank? 
      where_clause = where_clause+ " and u.ethinicity = "+demographic.to_s
     
    end
    
     if !gender.blank?
      where_clause = where_clause+ " and CONVERT(AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') USING latin1) = '"+gender.to_s+"'"
     
    end
  
  UserContentConsume.select("COUNT(DISTINCT ucl.id) AS totalViews ,COUNT(DISTINCT user_content_consumes.user_id) AS uniquewatch, ct.name as vid_name")
  .joins("JOIN users u ON u.id= user_content_consumes.user_id")
  .joins("JOIN contents ct ON ct.id=user_content_consumes.content_id")
  .joins("JOIN user_consumption_logs ucl on ucl.ucc_id = user_content_consumes.id")  
  .joins("JOIN roles_users ru ON ru.user_id=u.id")
  .joins("JOIN organization_users ou on ou.user_id = u.id") 
  .joins("join health_conditions_users hcu on hcu.user_id = u.id")
  .joins("join health_conditions hc on hc.id = hcu.health_condition_id")
  .where(where_clause+"  AND user_content_consumes.percent_consumed = 100 
  and DATE(user_content_consumes.updated_at) BETWEEN ? and ? and u.demo = 0",start_date,end_date)
  .group("ct.id").order("COUNT(ucl.id) DESC").limit(7)
  
  end
  
  def self.fetch_session_time(start_date,end_date,org_id,proc_id,department,demographic,gender)
    
     where_condition = "ru.role_id=1" 
     if !proc_id.blank?
      where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
    else
      where_clause = where_condition 
    end
    
    if !org_id.blank?
      where_clause=where_clause+" and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')"
    end
    
    if !department.blank?
      where_clause = where_clause+ " and hc.department_id = "+department.to_s
     
    end
     if !demographic.blank?
      where_clause = where_clause+ " and u.ethinicity = "+demographic.to_s
     
    end
    
     if !gender.blank?
      where_clause = where_clause+ " and CONVERT(AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') USING latin1) = '"+gender.to_s+"'"
     
    end
    
    LoginHistory.select("avg((CASE WHEN login_histories.logout_time IS NULL THEN 180
                  ELSE TIMESTAMPDIFF(SECOND,login_histories.login_time,login_histories.logout_time) 
             END)) as session_time ,o.name org_name")
             .joins("join users u on u.id = login_histories.user_id")
             .joins("JOIN `roles_users` ru ON ru.user_id=u.id")
             .joins("join organization_users ou on ou.user_id = u.id")
             .joins("join organizations o on o.id = ou.organization_id")
             .joins("join health_conditions_users hcu on hcu.user_id = u.id")
            .joins("join health_conditions hc on hc.id = hcu.health_condition_id")
             .where(where_clause+ " AND (DATE(login_histories.created_at) BETWEEN DATE('"+start_date+"') AND DATE('"+end_date+"'))
              AND ou.deactivated=0 AND u.demo=0 ")
             
             
    
    
  end
  
 def self.fetch_session_time_by_hospital(start_date,end_date,org_id,proc_id,department,demographic,gender)
    where_condition = "ru.role_id=1" 
    if !proc_id.blank?
      where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
    else
      where_clause = where_condition 
    end
    
    if !org_id.blank?
      where_clause=where_clause+" and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')"
    end
    
    if !department.blank?
      where_clause = where_clause+ " and hc.department_id = "+department.to_s
     
    end
     if !demographic.blank?
      where_clause = where_clause+ " and u.ethinicity = "+demographic.to_s
     
    end
    
     if !gender.blank?
      where_clause = where_clause+ " and CONVERT(AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') USING latin1) = '"+gender.to_s+"'"
     
    end
    
    
    
    
    LoginHistory.select("avg((CASE WHEN login_histories.logout_time IS NULL THEN 180
                  ELSE TIMESTAMPDIFF(SECOND,login_histories.login_time,login_histories.logout_time) 
             END)) as session_time ,o.name org_name")
             .joins("join users u on u.id = login_histories.user_id")
             .joins("JOIN `roles_users` ru ON ru.user_id=u.id")
             .joins("join organization_users ou on ou.user_id = u.id")
             .joins("join organizations o on o.id = ou.organization_id")
             .joins("join health_conditions_users hcu on hcu.user_id = u.id")
            .joins("join health_conditions hc on hc.id = hcu.health_condition_id")
             .where(where_clause+ " AND (DATE(login_histories.created_at) BETWEEN DATE('"+start_date+"') AND DATE('"+end_date+"'))
              AND ou.deactivated=0 AND u.demo=0 ")
             .group("o.id")
             
    
    
  end
   

  
  
  def self.fetch_login_count_corporate(start_date,end_date,org_id,proc_id,department,demographic,gender)
    where_condition = "ru.role_id=1" 
     if !proc_id.blank?
      where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
    else
      where_clause = where_condition 
    end
    
    if !org_id.blank?
      where_clause=where_clause+" and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')"
    end
    
    if !department.blank?
      where_clause = where_clause+ " and hc.department_id = "+department.to_s
     
    end
     if !demographic.blank?
      where_clause = where_clause+ " and u.ethinicity = "+demographic.to_s
     
    end
    
     if !gender.blank?
      where_clause = where_clause+ " and CONVERT(AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') USING latin1) = '"+gender.to_s+"'"
     
    end
    
    LoginHistory.select("count( distinct login_histories.user_id) unique_login,count(login_histories.user_id) total_login,ou.organization_id,o.name as org_name")
    .joins("join users u on u.id = login_histories.user_id")
    .joins("JOIN `roles_users` ru ON ru.user_id=u.id")
    .joins("join organization_users ou on ou.user_id = u.id")
    .joins("join organizations o on o.id = ou.organization_id")
    .joins("join health_conditions_users hcu on hcu.user_id = u.id")
    .joins("join health_conditions hc on hc.id = hcu.health_condition_id")
    .where(where_clause+ " AND (DATE(login_histories.created_at) BETWEEN DATE('"+start_date+"') AND DATE('"+end_date+"'))
              AND ou.deactivated=0 AND u.demo=0 and o.deactivated=0") 
    .group("o.id")
    .order("unique_login desc")
    
  end
  
  
  def self.fetch_consent_count_corporate(start_date,end_date,org_id,proc_id,department,demographic,gender)
    
    where_condition = "ru.role_id=1" 
     
    if !proc_id.blank?
      where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
    else
      where_clause = where_condition 
    end
    
    if !department.blank?
      where_clause = where_clause+ " and hc.department_id = "+department.to_s
     
    end
     if !demographic.blank?
      where_clause = where_clause+ " and users.ethinicity = "+demographic.to_s
     
    end
    
     if !gender.blank?
      where_clause = where_clause+ " and CONVERT(AES_DECRYPT(UNHEX(users.gender), 'Mytonomy') USING latin1) = '"+gender.to_s+"'"
     
    end
    
    
    
    if !org_id.blank?
      where_clause=where_clause+" and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')"
    end
    
      User.select("count(DISTINCT users.id) as invited, COUNT(DISTINCT ucc.user_id) as watched ")
      .joins(" LEFT JOIN  user_content_consumes ucc ON users.id = ucc.user_id AND ucc.percent_consumed =100
     AND  ucc.content_id  IN (
      SELECT c.content_id FROM `library_health_conditions` lhc     
      JOIN `library_playlists` lp ON lp.`library_template_id` = lhc.`library_template_id`
      JOIN `playlists` p ON p.id= lp.`playlist_id`  
      JOIN collections c ON c.playlist_id = p.id and c.parent_playlist is not null
      WHERE p.deactivated=0 AND content_id IS NOT NULL) ")
      .joins("JOIN `roles_users` ru ON ru.user_id=users.id")
      .joins("JOIN `organization_users` ou ON ou.user_id=users.id")
      .joins("JOIN `organizations` o ON o.id=ou.organization_id")
      .joins("join health_conditions_users hcu on hcu.user_id = users.id")
    .joins("join health_conditions hc on hc.id = hcu.health_condition_id")
     .where(where_clause+ " AND (DATE(users.created_at) BETWEEN DATE('"+start_date+"') AND DATE('"+end_date+"'))
              AND ou.deactivated=0 AND users.demo=0") 
     
    
  end
  
  def self.fetch_avg_videos_watched_per_session_hospital(start_date,end_date,org_id,proc_id,department,demographic,gender)
  where_condition = "ru.role_id=1"  
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
   if !org_id.blank?
      where_clause=where_clause+" and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')"
    end
    
    if !department.blank?
      where_clause = where_clause+ " and hc.department_id = "+department.to_s
     
    end
     if !demographic.blank? 
      where_clause = where_clause+ " and u.ethinicity = "+demographic.to_s
     
    end
    
     if !gender.blank?
      where_clause = where_clause+ " and CONVERT(AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') USING latin1) = '"+gender.to_s+"'"
     
    end
  
ActiveRecord::Base.connection.execute("SELECT t.HospitalName ,ROUND(AVG(t.uniqueCt) ) AS AverageUniqueView, ROUND(AVG(t.Totct)) AS AverageTotalView
FROM
(SELECT COUNT( ucl.id) Totct, COUNT(DISTINCT ucl.ucc_id) uniqueCt,o.name HospitalName
FROM `login_histories` lh 
JOIN users u on u.id = lh.user_id
JOIN `roles_users` ru ON ru.user_id=u.id
JOIN  `organization_users` ou ON ou.user_id = lh.user_id
JOIN `user_consumption_logs` ucl ON ucl.`login_id` = lh.id
JOIN organizations o ON o.id = ou.organization_id
JOIN health_conditions_users hcu on hcu.user_id = u.id
JOIN health_conditions hc on hc.id = hcu.health_condition_id
WHERE ou.deactivated=0 and "+where_clause+ " AND DATE(lh.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' AND u.demo=0  
GROUP BY lh.id, o.name)t
GROUP BY t.HospitalName ")
             
end

def self.fetch_content_module_completion_rate(start_date,end_date,org_id,proc_id,department,demographic,gender)
  where_condition = "ru.role_id=1"  
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
   if !org_id.blank?
      where_clause=where_clause+" and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')"
    end
    
    if !department.blank?
      where_clause = where_clause+ " and hc.department_id = "+department.to_s
     
    end
     if !demographic.blank? 
      where_clause = where_clause+ " and u.ethinicity = "+demographic.to_s
     
    end
    
     if !gender.blank?
      where_clause = where_clause+ " and CONVERT(AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') USING latin1) = '"+gender.to_s+"'"
     
    end
    
ActiveRecord::Base.connection.execute("SELECT t.name AS ContentModuleName,  (COUNT(IF(t.watched = t.totalContent,1,NULL)) *100)/COUNT(t.user_id) AS CompletionRate,COUNT(t.user_id) AS Patients FROM 

(SELECT ucc.user_id,hc.name,
COUNT(ucc.content_id) AS watched,(SELECT 
COUNT(c.content_id)
FROM `health_conditions` hc
JOIN `health_conditions_playlists` lcp ON lcp.`health_condition_id` = hc.id
JOIN `playlists` p ON p.id = lcp.playlist_id AND p.`deactivated` = 0
JOIN `collections` c ON c.`playlist_id` = lcp.playlist_id 
WHERE hc.id= hcu.health_condition_id) AS totalContent
FROM `user_content_consumes` ucc 
JOIN `users` u ON u.id=ucc.user_id
JOIN `roles_users` ru ON ru.user_id=u.id
JOIN `health_conditions_users` hcu ON hcu.user_id= ucc.user_id
JOIN `health_conditions` hc ON hc.id=hcu.health_condition_id
JOIN  `organization_users` ou ON ou.user_id = ucc.user_id
WHERE ou.deactivated=0 AND "+where_clause+ " AND DATE(u.created_at) BETWEEN '"+start_date.to_s+"' AND '"+end_date.to_s+"' AND u.demo=0
GROUP BY ucc.user_id,hc.name)t

GROUP BY t.name ")


end


def self.fetch_avg_completion_of_content_module(start_date,end_date,org_id,proc_id,department,demographic,gender)
  where_condition = "ru.role_id=1"  
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
   if !org_id.blank?
      where_clause=where_clause+" and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')"
    end
    
    if !department.blank?
      where_clause = where_clause+ " and hc.department_id = "+department.to_s
     
    end
     if !demographic.blank? 
      where_clause = where_clause+ " and u.ethinicity = "+demographic.to_s
     
    end
    
     if !gender.blank?
      where_clause = where_clause+ " and CONVERT(AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') USING latin1) = '"+gender.to_s+"'"
     
    end
    
ActiveRecord::Base.connection.execute("SELECT AVG(overallAverage.CompletionRate) FROM 
 (SELECT o.name AS Hospital,  (COUNT(IF(t.watched = t.totalContent,1,NULL)) *100)/COUNT(t.user_id) AS CompletionRate,COUNT(t.user_id) AS Patients FROM 

(SELECT ucc.user_id,ou.organization_id,
COUNT(ucc.content_id) AS watched,(SELECT 
COUNT(c.content_id)
FROM `health_conditions` hc
JOIN `health_conditions_playlists` lcp ON lcp.`health_condition_id` = hc.id
JOIN `playlists` p ON p.id = lcp.playlist_id AND p.`deactivated` = 0
JOIN `collections` c ON c.`playlist_id` = lcp.playlist_id WHERE hc.id= hcu.health_condition_id) AS totalContent

FROM `user_content_consumes` ucc 
JOIN `users` u ON u.id=ucc.user_id
JOIN `roles_users` ru ON ru.user_id=u.id
JOIN `health_conditions_users` hcu ON hcu.user_id= ucc.user_id
JOIN `health_conditions` hc ON hc.id=hcu.health_condition_id
JOIN  `organization_users` ou ON ou.user_id = ucc.user_id
WHERE ou.deactivated=0 and "+where_clause+ " AND DATE(u.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' AND u.demo=0
GROUP BY ucc.user_id,ou.organization_id)t
JOIN `organizations` o ON o.id= t.organization_id
GROUP BY t.organization_id) overallAverage")

end
  #-----------------corporate end----------------------------
  
  
  
  #----------------cardiac query start-------------------
  
  #-------------Heal-4181----------------
   def self.fetch_avg_video_by_procedure_cardiac(role,org_id,start_date,end_date,proc_id,patientgrp_id,facility_id)
     
  where_condition = "ru.role_id in ("+role.to_s+") and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')" 
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
  if !patientgrp_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end

  join_clause = ''  
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = ou.user_id"
    where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
  end
  
  ActiveRecord::Base.connection.execute(" SELECT count( t.userContentCount),t.Procedures FROM 
(SELECT  CASE WHEN DATE(ucl.created_at) < DATE(hcu.`procedure_date`) THEN 'Before'
             WHEN DATE(ucl.created_at) = DATE(hcu.`procedure_date`) THEN 'On'
             WHEN DATE(ucl.created_at) > DATE(hcu.`procedure_date`) THEN 'After'
             END AS Procedures,
  COUNT(ucc.content_id) AS userContentCount

FROM users u
left join user_units uu on uu.user_id = u.id
JOIN `user_content_consumes` ucc ON ucc.user_id=u.id and ucc.percent_consumed = 100 
JOIN `user_consumption_logs` ucl ON ucl.ucc_id=ucc.id
JOIN `health_conditions_users` hcu ON hcu.user_id=u.id
LEFT JOIN `organization_users` ou ON ou.user_id=u.id
"+join_clause+"
JOIN roles_users ru ON ru.user_id=u.id
 join health_conditions hc on hc.id = hcu.health_condition_id
WHERE  ou.deactivated = 0 and  hcu.`procedure_date` IS NOT NULL and "+where_clause+"
AND DATE(CONVERT_TZ(ucc.updated_at,'UTC','US/Eastern')) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY Procedures,u.id)t
GROUP BY t.Procedures")
  end
  
  
  
  
  #-------------Heal-4181----------------
  def self.fetch_top_videos_watched_all_cardiac(role,org_id,start_date,end_date,proc_id,patientgrp_id,facility_id)
    
  where_condition = "ru.role_id in ("+role.to_s+") and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')" 
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
  if !patientgrp_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end

  join_clause = ''  
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = u.id"
    where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
  end
  
  ActiveRecord::Base.connection.execute("SELECT 
  COUNT(t.user_id) AS patientCount,
  CASE
    WHEN content >=1 AND content <=10 THEN '1-10'
    WHEN content >=11 AND content <= 20 THEN '11-20'
    WHEN content >=21 AND content <=30 THEN '21-30'
    WHEN content >=31 AND content <= 40 THEN '31-40'
    WHEN content >=41 AND content <=50 THEN '41-50'
    WHEN content >=51  THEN '51+'
    END AS NumberofVideoRange
FROM    
(SELECT COUNT(ucc.content_id)AS content,ucc.user_id
FROM user_content_consumes ucc ,roles_users ru ,organization_users ou,health_conditions_users hcu,health_conditions hc,users u
"+join_clause+"
left join user_units uu on uu.user_id = u.id
WHERE hcu.user_id = u.id and hc.id = hcu.health_condition_id and ucc.percent_consumed = 100 and  ou.user_id = ucc.user_id and u.id=ucc.user_id 
AND ou.deactivated = 0 and ru.user_id=u.id AND "+where_clause+" 
AND DATE(CONVERT_TZ(ucc.updated_at,'UTC','US/Eastern')) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0 GROUP BY ucc.user_id)t
GROUP BY NumberofVideoRange;")
end

  #-------------Heal-4181----------------
  def self.fetch_procedure_date_data_cardiac(role,org_id,start_date,end_date,proc_id,patientgrp_id,facility_id)
    
  where_condition = "ru.role_id in ("+role.to_s+") and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')" 
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
  if !patientgrp_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end

  join_clause = ''  
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = ou.user_id"
    where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
  end
    
    User.select("COUNT(DISTINCT users.id) as patient_count ,CASE WHEN hcu.`procedure_date` IS NULL THEN 'no_procedure_date_listed' ELSE 'procedure_date_listed' END 
                  AS ListedProcedureOrNot")
                  .joins("left join user_units uu on uu.user_id = users.id")
                  .joins(" JOIN (select max(procedure_date) as procedure_date,user_id from `health_conditions_users` where FIND_IN_SET(`health_conditions_users`.health_condition_id,'"+proc_id.to_s+"') group by user_id ) hcu ON hcu.`user_id` = users.id")
                  .joins("JOIN health_conditions hc on hc.organization_id = users.organization_id")
                  .joins(" JOIN `organization_users` ou ON ou.user_id=users.id")
                  .joins("#{join_clause}")
                  .joins("JOIN roles_users ru ON ru.user_id=users.id")
                  .where(where_clause + " AND  DATE(CONVERT_TZ(users.created_at,'UTC','US/Eastern')) BETWEEN ? and ?  and users.demo = 0 and ou.deactivated = 0",start_date,end_date)
                  .group("ListedProcedureOrNot")

   
  end
  
#-------------Heal-4303----------------
def self.fetch_patient_viewership_device_cardiac(role,org_id,start_date,end_date,proc_id,patientgrp_id,facility_id)
  where_condition = "ru.role_id in (select id from roles where role_type_id in (2)) and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')" 
   if !proc_id.blank?
     where_clause = where_condition+ "and FIND_IN_SET(hcu.health_condition_id,'"+proc_id.to_s+"')"
   else
     where_clause = where_condition 
   end
  
   if !patientgrp_id.blank?
     where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
   end

   join_clause = ''  
   if !facility_id.nil? && !facility_id.blank?
     join_clause = "left join facility_users fu on fu.user_id = ou.user_id"
     where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
   end
  
   ActiveRecord::Base.connection.execute(" select count(t.user_id) as pat_device,t.device as device_type
   from
    (select  lh.user_id, case when (lh.user_agent like '%mobile%' and lh.user_agent NOT LIKE '%tablet%' and lh.user_agent NOT LIKE '%ipad%') then 'Mobile' when (lh.user_agent like '%tablet%' or lh.user_agent LIKE '%ipad%') then 'Tablet' when (lh.user_agent NOT LIKE '%tablet%' and lh.user_agent NOT LIKE '%ipad%' and lh.user_agent NOT LIKE '%mobile%') then 'Desktop' end 'device'
    from `user_consumption_logs`
    JOIN login_histories lh ON lh.id=user_consumption_logs.login_id
    JOIN users u ON u.id= lh.user_id
    LEFT JOIN user_units uu on uu.user_id = lh.user_id
    join health_conditions_users hcu on hcu.user_id = lh.user_id
    JOIN organization_users ou on ou.user_id = u.id
    "+join_clause+"
    JOIN roles_users ru ON ru.user_id=u.id
    WHERE ("+where_clause+" and u.demo=0 and ou.deactivated = 0 and (Date(CONVERT_TZ(user_consumption_logs.created_at,'UTC','US/Eastern')) between '"+start_date.to_s+"' and '"+end_date.to_s+"'))
    group by lh.user_id,device) t
    group by t.device")
 end

 #-------------Heal-4303----------------

  #-------------Heal-4181----------------
  def self.fetch_patient_viewership_browser_cardiac(role,org_id,start_date,end_date,proc_id,patientgrp_id,facility_id)
    where_condition = "ru.role_id in ("+role.to_s+") and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')" 
    if !proc_id.blank?
      where_clause = where_condition+ "and FIND_IN_SET(lhc.health_condition_id,'"+proc_id.to_s+"')"
    else
      where_clause = where_condition 
    end
    
    if !patientgrp_id.blank?
      where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
    end

    join_clause = ''  
    if !facility_id.nil? && !facility_id.blank?
      join_clause = "left join facility_users fu on fu.user_id = ou.user_id"
      where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
    end
    
    UserConsumptionLog.select("DISTINCT lh.user_id as user,lh.user_agent as broswer")
    .joins("JOIN login_histories lh ON lh.id=user_consumption_logs.login_id")
    .joins("JOIN users u ON u.id= lh.user_id")
    .joins("JOIN organization_users ou on ou.user_id = u.id") 
    .joins("#{join_clause}")
    .joins("JOIN roles_users ru ON ru.user_id=ou.id")
    .joins("JOIN user_content_consumes ucc on ucc.id = user_consumption_logs.ucc_id")
    .joins("JOIN library_playlists lp on lp.playlist_id = ucc.playlist_id")  
    .joins("join library_health_conditions lhc on lhc.library_template_id = lp.library_template_id")
    .joins("LEFT JOIN user_units uu on uu.user_id = u.id")
  .where(where_clause+ " and user_consumption_logs.skip_flag=false and ucc.percent_consumed = 100 and ucc.content_id is not null and ucc.playlist_id is not null and ucc.content_id not in (15,30) AND  DATE(CONVERT_TZ(user_consumption_logs.created_at,'UTC','US/Eastern'))
         BETWEEN ? and ? and u.demo = 0 and ou.deactivated = 0",start_date,end_date)
    
  end

  #-------------Heal-4301----------------
  def self.patient_by_location_cardiac(role,org_id,start_date,end_date,proc_id,patientgrp_id,facility_id)
    
  where_condition = "ru.role_id in ("+role.to_s+") and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')" 
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
  if !patientgrp_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end

  join_clause = ''  
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = ou.user_id"
    where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
  end
  
  UserConsumptionLog.select("DISTINCT `latitude`,`longitude`,lh.city as city,lh.state as state,count(distinct lh.user_id) as pat_cnt")
  .joins("JOIN login_histories lh ON lh.id=user_consumption_logs.login_id")
  .joins("JOIN users u ON u.id= lh.user_id")
  .joins("left join user_units uu on uu.user_id = u.id")
  .joins("JOIN roles_users ru ON ru.user_id=u.id")
  .joins("JOIN organization_users ou on ou.user_id = u.id") 
  .joins("#{join_clause}")
  .joins("join health_conditions_users hcu on hcu.user_id = u.id")
  .joins("join health_conditions hc on hc.id = hcu.health_condition_id") 
  .where(where_clause+" AND  DATE(CONVERT_TZ(user_consumption_logs.created_at,'UTC','US/Eastern')) BETWEEN ? and ? and u.demo = 0 and ou.deactivated = 0 and lh.city is not null and lh.city != '' and lh.state is not null and lh.state != ''",start_date,end_date)
  .group("lh.city,lh.state")
end
#------------heal-4301----------------  
   #-------------Heal-4181----------------
   def self.fetch_average_age_cardiac_by_gender(role,org_id,procedure,start_date,end_date,proc_id,patientgrp_id,facility_id)
     
  where_condition = "roles_users.role_id in ("+role.to_s+") and FIND_IN_SET(organization_users.organization_id,'"+org_id.to_s+"')" 
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
  if !patientgrp_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end

  join_clause = ''  
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = organization_users.user_id"
    where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
  end
  
     User.where(where_clause+" and users.gender is not null and organization_users.deactivated=0  AND TIMESTAMP(CONVERT_TZ(users.created_at,'UTC','US/Eastern')) BETWEEN ? and ? and users.demo = 0",start_date,end_date)
   .joins("left join user_units uu on uu.user_id = users.id")
   .joins("join organization_users on users.id = organization_users.user_id")
   .joins("#{join_clause}")
   .joins("join roles_users on users.id = roles_users.user_id")
   .joins("join health_conditions_users hcu on hcu.user_id = users.id")
  .joins("join health_conditions hc on hc.id = hcu.health_condition_id")
   .select("AES_DECRYPT(UNHEX(gender),'Mytonomy') AS Gender,AVG(TIMESTAMPDIFF(YEAR, AES_DECRYPT(UNHEX(dob),'Mytonomy'), CURDATE())) AS 'AGE'")
   .group("AES_DECRYPT(UNHEX(gender),'Mytonomy')")      
  end
  
  #-------------Heal-4181----------------
  def self.fetch_average_age_cardiac(role,org_id,procedure,start_date,end_date,proc_id,patientgrp_id,facility_id)
       
    where_condition = "FIND_IN_SET(organization_users.organization_id,'"+org_id.to_s+"')" 
    if !proc_id.blank?
      where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
    else
      where_clause = where_condition 
    end
    
    if !patientgrp_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
    end
    
    join_clause = ''  
    if !facility_id.nil? && !facility_id.blank?
      join_clause = "left join facility_users fu on fu.user_id = organization_users.user_id"
      where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
    end

       User.where(where_clause+" and users.gender is not null and organization_users.deactivated=0 and  roles_users.role_id = ? AND TIMESTAMP(CONVERT_TZ(users.created_at,'UTC','US/Eastern')) BETWEEN ? and ? and users.demo = 0",role,start_date,end_date)
     .joins("left join user_units uu on uu.user_id = users.id")
     .joins("join organization_users on users.id = organization_users.user_id")
     .joins("#{join_clause}")
     .joins("join roles_users on users.id = roles_users.user_id")
     .joins("join health_conditions_users hcu on hcu.user_id = users.id")
    .joins("join health_conditions hc on hc.id = hcu.health_condition_id")
     .select("'All Patients' AS Gender,AVG(TIMESTAMPDIFF(YEAR, AES_DECRYPT(UNHEX(dob),'Mytonomy'), CURDATE())) AS 'AGE'")
    end

  
  #-------------Heal-4181----------------
  def self.fetch_consent_count_cardiac(role,start_date,end_date,org_id,proc_id,patientgrp_id,facility_id)
    
    where_condition = "ru.role_id IN ("+role.to_s+")  and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')" 
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
    
    if !patientgrp_id.blank?
     where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
    end

    join_clause = ''  
    if !facility_id.nil? && !facility_id.blank?
      join_clause = "left join facility_users fu on fu.user_id = ou.user_id"
      where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
    end
    
    User.select("o.name as hospital,o.id as org_id, count(DISTINCT users.id) as invited, COUNT(DISTINCT ucc.user_id) as watched ")
    .joins(" LEFT JOIN  user_content_consumes ucc ON users.id = ucc.user_id AND ucc.percent_consumed =100
   AND  ucc.content_id  IN (
SELECT c.content_id FROM `library_health_conditions` lhc     
    JOIN `library_playlists` lp ON lp.`library_template_id` = lhc.`library_template_id`
    JOIN `playlists` p ON p.id= lp.`playlist_id`  
    JOIN collections c ON c.playlist_id = p.id and c.parent_playlist is not null
    WHERE lhc.health_condition_id IN ("+proc_id.to_s+") AND p.deactivated=0 AND content_id IS NOT NULL) ")
    .joins("left join user_units uu on uu.user_id = users.id")
    .joins("JOIN `roles_users` ru ON ru.user_id=users.id")
    .joins("JOIN `organization_users` ou ON ou.user_id=users.id")
    .joins("#{join_clause}")
    .joins("JOIN `organizations` o ON o.id=ou.organization_id")
    .joins("join health_conditions_users hcu on hcu.user_id = users.id")
  .joins("join health_conditions hc on hc.id = hcu.health_condition_id")
    .where(where_clause+ " AND (DATE(CONVERT_TZ(users.created_at,'UTC','US/Eastern')) BETWEEN DATE('"+start_date+"') AND DATE('"+end_date+"'))
            AND ou.deactivated=0 AND users.demo=0") 
     .group("ou.organization_id")
    
  end
  
  #--------------Heal-4181---------------
  def self.fetch_video_rating_cardiac(role,org_id,proc_id,start_date,end_date,patientgrp_id,facility_id)
  
  where_condition = "FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')"
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hcu.health_condition_id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
  if !patientgrp_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end
  
  if !proc_id.blank? && !patientgrp_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hcu.health_condition_id,'"+proc_id.to_s+"') and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end

  join_clause = ''  
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = ou.user_id"
    where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
  end
  #UserContentConsume.select("ct.name as video_name,Count( case when user_rating is null then 'No' end ) as skipUser,  
  #Count( case when user_rating is not null then 'Yes' end ) as userProvidedRating,avg(user_content_consumes.user_rating) as user_rating")
  #.joins("JOIN users u ON u.id= user_content_consumes.user_id")
  #.joins("LEFT JOIN user_units uu on uu.user_id = u.id")
  #.joins("JOIN organization_users ou on ou.user_id = u.id")
  #.joins("JOIN health_conditions_users hcu on hcu.user_id =  u.id")
  #.joins("JOIN contents ct ON ct.id=user_content_consumes.content_id")
  #.where(where_clause+" and ou.deactivated = 0 and user_content_consumes.content_id is not null and user_content_consumes.playlist_id is not null and (DATE(user_content_consumes.updated_at) BETWEEN DATE('"+start_date+"') 
  #AND DATE('"+end_date+"')) and u.demo = 0")
  #.group("user_content_consumes.content_id")
  #.order("user_rating desc")
  
  ActiveRecord::Base.connection.execute("  select
   t.video_name,Count( case when t.user_rating is null then 'No' end ) as skipUser,  
 Count( case when t.user_rating is not null then 'Yes' end ) as userProvidedRating,avg(t.user_rating) as user_rating 
 
 from
   (SELECT user_content_consumes.content_id,  ct.name as video_name,user_content_consumes.user_id, user_content_consumes.playlist_id,user_content_consumes.user_rating -- Count( case when user_rating is null then 'No' end ) as skipUser,  
  -- Count( case when user_rating is not null then 'Yes' end ) as userProvidedRating,avg(user_content_consumes.user_rating) as user_rating 
  FROM `user_content_consumes` 
JOIN users u ON u.id= user_content_consumes.user_id
LEFT JOIN user_units uu on uu.user_id = u.id  
JOIN organization_users ou on ou.user_id = u.id
"+join_clause+"
JOIN roles_users ru on ru.user_id=u.id 
JOIN health_conditions_users hcu on hcu.user_id =  u.id
join health_conditions_playlists hcp on hcp.health_condition_id = hcu.health_condition_id
join collections cl on cl.content_id = user_content_consumes.content_id  and cl.playlist_id = hcp.playlist_id
JOIN contents ct ON ct.id=cl.content_id  
WHERE ("+where_clause+" and ru.role_id IN ("+role.to_s+") and ou.deactivated = 0 and user_content_consumes.content_id is not null and user_content_consumes.playlist_id is not null and 
(DATE(CONVERT_TZ(user_content_consumes.updated_at,'UTC','US/Eastern')) BETWEEN DATE('"+start_date+"') AND DATE('"+end_date+"')) and u.demo = 0) 
  group by user_content_consumes.content_id, ct.name ,user_content_consumes.user_id, user_content_consumes.playlist_id)t
 GROUP BY t.video_name  
 ORDER BY user_rating desc")
  
end
  
#-------------Heal-4181----------------  
def self.patient_usage_invitation_date_cardiac(role,org_id,start_date,end_date,proc_id,patientgrp_id,facility_id)
    
  where_condition = "ru.role_id in ("+role.to_s+") and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')" 
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
  if !patientgrp_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end

  join_clause = ''  
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = ou.user_id"
    where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
  end
  
    ActiveRecord::Base.connection.execute("SELECT count(distinct t.avgWatchcounttest) AS avgWatchcount,
    CASE
    WHEN (DATEDIFF(date1,date2) +1) >=0 AND (DATEDIFF(date1,date2) +1) <=1 THEN '1'
    WHEN (DATEDIFF(date1,date2) +1) >1 AND (DATEDIFF(date1,date2) +1) <= 2 THEN '2'
    WHEN (DATEDIFF(date1,date2) +1) >2 AND (DATEDIFF(date1,date2) +1) <=3 THEN '3'
    WHEN (DATEDIFF(date1,date2) +1) >3 AND (DATEDIFF(date1,date2) +1) <= 4 THEN '4'
    WHEN (DATEDIFF(date1,date2) +1) >4 AND (DATEDIFF(date1,date2) +1) <=5 THEN '5'
    WHEN (DATEDIFF(date1,date2) +1) >5 AND (DATEDIFF(date1,date2) +1) <=6 THEN '6'
    WHEN (DATEDIFF(date1,date2) +1) >6 AND (DATEDIFF(date1,date2) +1) <=7 THEN '7'
    WHEN (DATEDIFF(date1,date2) +1) >=8 and (DATEDIFF(date1,date2) +1) <=10 THEN '8-10'
    WHEN (DATEDIFF(date1,date2) +1) >=11 AND (DATEDIFF(date1,date2) +1) <=13 THEN '11-13'
    WHEN (DATEDIFF(date1,date2) +1) >=14 AND (DATEDIFF(date1,date2) +1) <=21 THEN '14-21'
    WHEN (DATEDIFF(date1,date2) +1) >21  THEN '81+'
    END AS days
    FROM 
    (SELECT ( ucl.id)  AS avgWatchcounttest,
      ucl.created_at as date1,users.created_at as date2
      FROM `users`
      LEFT JOIN user_units uu on uu.user_id = users.id 
      LEFT JOIN  user_content_consumes ucc ON users.id = ucc.user_id AND ucc.percent_consumed =100
      AND  ucc.content_id IN (SELECT c.content_id FROM `library_health_conditions` lhc     
      JOIN `library_playlists` lp ON lp.`library_template_id` = lhc.`library_template_id`
      JOIN `playlists` p ON p.id= lp.`playlist_id` 
      JOIN collections c ON c.playlist_id = p.id and c.parent_playlist is not null
      WHERE lhc.health_condition_id IN ("+proc_id.to_s+") 
      AND p.deactivated=0 AND content_id IS NOT NULL) 
      JOIN organization_users ou on ou.user_id = users.id 
      "+join_clause+"
      JOIN roles_users ru on ru.user_id = users.id 
      JOIN user_consumption_logs ucl on ucl.ucc_id = ucc.id 
      join health_conditions_users hcu on hcu.user_id = users.id 
      join health_conditions hc on hc.id = hcu.health_condition_id 
      WHERE ("+where_clause+" and ucl.skip_flag=false and ou.deactivated=0 and users.demo = 0 AND  DATE(CONVERT_TZ(users.created_at,'UTC','US/Eastern'))BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' ) )t
    GROUP BY days") 
       
end
  
#-------------Heal-4181----------------  
def self.patient_usage_cardiac(role,org_id,group_str,start_date,end_date,proc_id,patientgrp_id,facility_id)
  
  where_condition = "ru.role_id in ("+role.to_s+") and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')" 
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
  if !patientgrp_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end

  join_clause = ''  
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = ou.user_id"
    where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
  end
  
  if group_str != 'YEAR'
    
    group_by_clause1=" GROUP BY #{group_str.to_s} (dte)) tab1 "
    group_by_clause2=" GROUP BY #{group_str.to_s} (users.created_at) "
    
    if group_str.eql? "WEEK"
      group_by_clause1=" GROUP BY week_start,week_end ) tab1"
      group_by_clause2=" GROUP BY week_start,week_end "
    end
    
     results = ActiveRecord::Base.connection.execute("SELECT IFNULL(tab2.Invited,0) as Invited,IFNULL(tab2.viewed,0) as viewed,tab1.DAY ,tab2.create_date,tab1.week_start,tab1.week_end FROM
(SELECT "+group_str.to_s+"(dte) AS DAY,DAY(dte) as daynumber,WEEK(dte) as dayWeek, 
DATE_FORMAT(DATE_ADD(dte, INTERVAL(1-DAYOFWEEK(dte)) DAY),'%m/%d') AS week_start,
  DATE_FORMAT(DATE_ADD(dte, INTERVAL(7-DAYOFWEEK(dte)) DAY),'%m/%d') AS week_end FROM
(SELECT '"+ start_date.to_s+"' + INTERVAL a + b DAY dte
FROM
 (SELECT 0 a UNION SELECT 1 a UNION SELECT 2 UNION SELECT 3
    UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7
    UNION SELECT 8 UNION SELECT 9 ) d,
 (SELECT 0 b UNION SELECT 10 UNION SELECT 20 
    UNION SELECT 30 UNION SELECT 40) m
WHERE '"+ start_date.to_s+"' + INTERVAL a + b DAY  <  '"+ end_date.to_s+"'
ORDER BY a + b)t 
#{group_by_clause1} 
LEFT JOIN

(SELECT COUNT(DISTINCT users.id) AS Invited,COUNT(DISTINCT ucc.user_id) AS viewed,
" +group_str.to_s+"(users.created_at) AS DAY,DATE_FORMAT(users.created_at,'%m/%d') AS create_date,
  DATE_FORMAT(DATE_ADD(users.created_at, INTERVAL(1-DAYOFWEEK(users.created_at)) DAY),'%m/%d') AS week_start,
  DATE_FORMAT(DATE_ADD(users.created_at, INTERVAL(7-DAYOFWEEK(users.created_at)) DAY),'%m/%d') AS week_end
 FROM `users` LEFT JOIN user_units uu on uu.user_id = users.id
 JOIN organization_users ou ON ou.user_id = users.id #{join_clause} JOIN roles_users ru ON ru.user_id = users.id
 join health_conditions_users hcu on hcu.user_id = users.id join health_conditions hc on hc.id = hcu.health_condition_id 
  LEFT JOIN  user_content_consumes ucc ON users.id = ucc.user_id AND ucc.percent_consumed =100
   AND  ucc.content_id  IN (
SELECT c.content_id FROM `library_health_conditions` lhc     
    JOIN `library_playlists` lp ON lp.`library_template_id` = lhc.`library_template_id`
    JOIN `playlists` p ON p.id= lp.`playlist_id` 
    JOIN collections c ON c.playlist_id = p.id and c.parent_playlist is not null
    WHERE lhc.health_condition_id IN ("+proc_id.to_s+") AND p.deactivated=0 AND content_id IS NOT NULL)
  WHERE ("+where_clause+" AND  DATE(CONVERT_TZ(users.created_at,'UTC','US/Eastern'))
       BETWEEN '"+ start_date.to_s+"' AND '"+ end_date.to_s+"' AND users.demo = 0 and ou.deactivated=0 ) 
      #{group_by_clause2}  ORDER BY users.created_at ASC ) tab2 ON tab1.DAY=tab2.DAY order by tab1.dayWeek,tab1.daynumber ")
    
       
  else
    
    results = ActiveRecord::Base.connection.execute("SELECT COUNT(DISTINCT users.id) AS Invited,COUNT(DISTINCT ucc.user_id) AS viewed,YEAR(users.created_at) as day,
    DATE_FORMAT(users.created_at,'%m/%d') as create_date,
 DATE_FORMAT(DATE_ADD(users.created_at, INTERVAL(1-DAYOFWEEK(users.created_at)) DAY),'%m/%d') as week_start,
 DATE_FORMAT(DATE_ADD(users.created_at, INTERVAL(7-DAYOFWEEK(users.created_at)) DAY),'%m/%d') as week_end 
 FROM `users` LEFT JOIN user_units uu on uu.user_id = users.id
 JOIN organization_users ou on ou.user_id = users.id #{join_clause} JOIN roles_users ru on ru.user_id = users.id 
 LEFT JOIN  user_content_consumes ucc ON users.id = ucc.user_id AND ucc.percent_consumed =100
   AND  ucc.content_id  IN (
SELECT c.content_id FROM `library_health_conditions` lhc     
    JOIN `library_playlists` lp ON lp.`library_template_id` = lhc.`library_template_id`
    JOIN `playlists` p ON p.id= lp.`playlist_id` 
    JOIN collections c ON c.playlist_id = p.id and c.parent_playlist is not null
    WHERE lhc.health_condition_id IN ("+proc_id.to_s+") AND p.deactivated=0 AND content_id IS NOT NULL)
  join health_conditions_users hcu on hcu.user_id = users.id join health_conditions hc on hc.id = hcu.health_condition_id 
 WHERE ("+where_clause+" AND  DATE(CONVERT_TZ(users.created_at,'UTC','US/Eastern'))
      BETWEEN '"+ start_date.to_s+"' and '"+ end_date.to_s+"' and users.demo = 0 and ou.deactivated=0 )  ORDER BY users.created_at asc ")
    
    
      end
   
end

#-------------Heal-4181 and Heal 4187----------------
def self.fetch_top_videos_watched_cardiac(role,org_id,procedure,start_date,end_date,proc_id,patientgrp_id,facility_id)
  
  where_condition = "ru.role_id in ("+role.to_s+") and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')" 
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
  if !patientgrp_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end

  join_clause = ''  
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = ou.user_id"
    where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
  end
  
  UserContentConsume.select("COUNT(DISTINCT ucl.id) AS totalViews ,COUNT(DISTINCT user_content_consumes.user_id) AS uniquewatch, ct.name as vid_name")
  .joins("JOIN users u ON u.id= user_content_consumes.user_id")
  .joins("left join user_units uu on uu.user_id = u.id")
  .joins("JOIN contents ct ON ct.id=user_content_consumes.content_id")
  .joins("JOIN user_consumption_logs ucl on ucl.ucc_id = user_content_consumes.id")  
  .joins("JOIN roles_users ru ON ru.user_id=u.id")
  .joins("JOIN organization_users ou on ou.user_id = u.id") 
  .joins("#{join_clause}") 
  .joins("join health_conditions_users hcu on hcu.user_id = u.id")
  .joins("join health_conditions hc on hc.id = hcu.health_condition_id")
  .where(" user_content_consumes.content_id  IN (
SELECT c.content_id FROM `library_health_conditions` lhc     
    JOIN `library_playlists` lp ON lp.`library_template_id` = lhc.`library_template_id`
    JOIN `playlists` p ON p.id= lp.`playlist_id` 
    JOIN collections c ON c.playlist_id = p.id and c.parent_playlist is not null
    WHERE lhc.health_condition_id IN ("+proc_id.to_s+") AND p.deactivated=0 AND content_id IS NOT NULL) and ucl.skip_flag=false and "+where_clause+"  AND user_content_consumes.percent_consumed = 100 
  and DATE(CONVERT_TZ(user_content_consumes.updated_at,'UTC','US/Eastern')) BETWEEN ? and ? and u.demo = 0",start_date,end_date)
  .group("ct.name").order("COUNT(ucl.id) DESC").limit(5)
  
end

  #-------------Heal-4181----------------
  def self.top_videos_watched_gender_cardiac(role,org_id,gender,start_date,end_date,proc_id,patientgrp_id,facility_id)
    where_condition = "ru.role_id in ("+role.to_s+") and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')" 
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end
  
  if !patientgrp_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end  

  join_clause = ''  
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = ou.user_id"
    where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
  end
  
  UserContentConsume.select("ct.name as vid_name,AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') AS gender,COUNT(ucl.id) AS ct")
  .joins("JOIN users u ON u.id= user_content_consumes.user_id")
  .joins("left join user_units uu on uu.user_id = u.id")
  .joins("JOIN contents ct ON ct.id=user_content_consumes.content_id")
  .joins("JOIN user_consumption_logs ucl on ucl.ucc_id = user_content_consumes.id")  
  .joins("JOIN roles_users ru ON ru.user_id=u.id")
  .joins("JOIN organization_users ou on ou.user_id = u.id")
  .joins("#{join_clause}")
  .joins("join health_conditions_users hcu on hcu.user_id = u.id")
  .joins("join health_conditions hc on hc.id = hcu.health_condition_id")
.where("user_content_consumes.content_id  IN (
SELECT c.content_id FROM `library_health_conditions` lhc     
    JOIN `library_playlists` lp ON lp.`library_template_id` = lhc.`library_template_id`
    JOIN `playlists` p ON p.id= lp.`playlist_id` 
    JOIN collections c ON c.playlist_id = p.id and c.parent_playlist is not null
    WHERE lhc.health_condition_id IN ("+proc_id.to_s+") AND p.deactivated=0 AND 
    content_id IS NOT NULL) and "+where_clause+"   and user_content_consumes.percent_consumed = 100 AND ucl.skip_flag=false AND
AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') = '"+gender.to_s+"' AND user_content_consumes.percent_consumed = 100 and DATE(CONVERT_TZ(user_content_consumes.updated_at,'UTC','US/Eastern'))
       BETWEEN ? and ? and u.demo = 0",start_date,end_date).group("ct.name,u.gender").order("COUNT(ucl.id) DESC").limit(3)
  
end
  
  #-------------Heal-4181 and Heal 4187----------------
  def self.fetch_top_videos_watched_hospital_cardiac(role,org_id,start_date,end_date,proc_id,patientgrp_id,facility_id)
 
 where_condition = "ru.role_id in ("+role.to_s+") and FIND_IN_SET(u.organization_id,"+org_id.to_s+")" 
  if !proc_id.blank?
    health_condition = " and ( "
    health_cond = "hc.id"
    health_condition = health_condition + health_conditions_where_clause(proc_id,health_cond)
    #where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
    health_condition = health_condition + " )" 
    where_clause = where_condition+health_condition
    health_condition1 = "( "
    health_cond1 = "lhc.health_condition_id"
    health_condition1 = health_condition1 + health_conditions_where_clause(proc_id,health_cond1)
    #where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
    health_condition1 = health_condition1 + " )" 
    where_clause1 = health_condition1
  else
    where_clause = where_condition 
  end
  
  if !patientgrp_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end

  join_clause = ''  
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = u.id"
    where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
  end
  
  ActiveRecord::Base.connection.execute("(SELECT  c.name,o.name,COUNT(ucl.id) AS ct    
FROM user_content_consumes ucc,roles_users ru,contents c,user_consumption_logs ucl,health_conditions_users hcu,
health_conditions hc,`organization_users` ou,`organizations` o,users u 
LEFT JOIN user_units uu on uu.user_id = u.id  
"+join_clause+"
WHERE ucc.content_id not in (15,30) and ucc.content_id  IN (
SELECT c.content_id FROM `library_health_conditions` lhc     
    JOIN `library_playlists` lp ON lp.`library_template_id` = lhc.`library_template_id`
    JOIN `playlists` p ON p.id= lp.`playlist_id` 
    JOIN collections c ON c.playlist_id = p.id and c.parent_playlist is not null
       WHERE "+where_clause1+" AND p.deactivated=0 AND content_id IS NOT NULL) 
     AND hcu.user_id = u.id and hc.id = hcu.health_condition_id and u.id=ucc.user_id and  ru.user_id = u.id AND c.id=ucc.content_id  
AND ucl.ucc_id = ucc.id AND "+where_clause+" AND ou.user_id=u.id and ucl.skip_flag=false AND DATE(CONVERT_TZ(ucc.updated_at,'UTC','US/Eastern')) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'  AND o.id=ou.organization_id AND u.demo=0 AND ou.deactivated=0
GROUP BY c.name,o.name ORDER BY COUNT(ucl.id) DESC LIMIT 2)

#UNION ALL

#(SELECT  c.name,o.name,COUNT(ucl.id) AS ct    
#FROM user_content_consumes ucc,users u ,roles_users ru,contents c,user_consumption_logs ucl,
#`organization_users` ou,`organizations` o    
#WHERE u.id=ucc.user_id AND ru.user_id = u.id AND ru.role_id =1 AND c.id=ucc.content_id AND c.content_type='ooyala' 
#AND ucl.ucc_id = ucc.id AND u.organization_id = "+org_id.to_s+" AND ou.user_id=u.id AND DATE(CONVERT_TZ(ucc.updated_at,'UTC','US/Eastern')) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'  AND o.id=ou.organization_id AND u.demo=0 AND ou.deactivated=0
#GROUP BY c.id,o.name ORDER BY COUNT(ucl.id) DESC LIMIT 2)
#UNION ALL

#(SELECT  c.name,o.name,COUNT(ucl.id) AS ct    
#FROM user_content_consumes ucc,users u ,roles_users ru,contents c,user_consumption_logs ucl,
#`organization_users` ou,`organizations` o    
#WHERE u.id=ucc.user_id  AND ru.user_id = u.id AND ru.role_id =1 AND c.id=ucc.content_id AND c.content_type='ooyala' 
#AND ucl.ucc_id = ucc.id AND u.organization_id = "+org_id.to_s+" AND ou.user_id=u.id  AND DATE(CONVERT_TZ(ucc.updated_at,'UTC','US/Eastern')) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'  AND o.id=ou.organization_id AND u.demo=0 AND ou.deactivated=0
#GROUP BY c.id,o.name ORDER BY COUNT(ucl.id) DESC LIMIT 2);
")
  
end
  
  #-------------Heal-4181----------------
  def self.fetch_top_videos_watched_age_cardiac(role,org_id,start_date,end_date,proc_id,patientgrp_id,facility_id)
    
  where_condition = "ru.role_id in ("+role.to_s+") and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')" 
  if !proc_id.blank?
    health_condition = " and ( "
    health_cond = "hc.id"
    health_condition = health_condition + health_conditions_where_clause(proc_id,health_cond)
    #where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
    health_condition = health_condition + " )" 
    where_clause = where_condition+health_condition
    health_condition1 = "( "
    health_cond1 = "lhc.health_condition_id"
    health_condition1 = health_condition1 + health_conditions_where_clause(proc_id,health_cond1)
    #where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
    health_condition1 = health_condition1 + " )" 
    where_clause1 = health_condition1
  else
    where_clause = where_condition 
  end
  
  o_id = org_id
  
  if !patientgrp_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end

  join_clause = ''  
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = u.id"
    where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
  end
  
  ActiveRecord::Base.connection.execute("SELECT  tbl.age,tbl.name,
  CASE
    WHEN age >=10 AND age <=19 THEN '10 - 19'
    WHEN age >=20 AND age <=29 THEN '20 - 29'
    WHEN age >=30 AND age <=39 THEN '30 - 39'
    WHEN age >=40 AND age <=49 THEN '40 - 49'
    WHEN age >=50 AND age <=59 THEN '50 - 59'
    WHEN age >=60 AND age <=69 THEN '60 - 69'
    WHEN age >=70  THEN '70+' 
  END AS ageband
FROM    
    (SELECT COUNT(ucc.id) AS ct ,c.name,DATE_FORMAT(FROM_DAYS( DATEDIFF( CURRENT_DATE, DATE(AES_DECRYPT(UNHEX(u.dob), 'Mytonomy')))
        ),
        '%y'
    ) AS age
    
    FROM user_content_consumes ucc,roles_users ru,contents c ,organization_users ou,health_conditions_users hcu,health_conditions hc,
    users u
    LEFT JOIN user_units uu on uu.user_id = u.id 
    "+join_clause+" 
    WHERE ucc.content_id  IN (
SELECT c.content_id FROM `library_health_conditions` lhc     
    JOIN `library_playlists` lp ON lp.`library_template_id` = lhc.`library_template_id`
    JOIN `playlists` p ON p.id= lp.`playlist_id` 
    JOIN collections c ON c.playlist_id = p.id and c.parent_playlist is not null
      WHERE "+where_clause1+" AND p.deactivated=0 AND content_id IS NOT NULL)   AND  hcu.user_id = u.id and hc.id = hcu.health_condition_id 
    and ou.user_id = ucc.user_id and u.id=ucc.user_id AND ucc.percent_consumed = 100 and ru.user_id = u.id AND c.id=ucc.content_id
    AND "+where_clause+" AND DATE(CONVERT_TZ(ucc.updated_at,'UTC','US/Eastern')) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
    GROUP BY c.id,DATE_FORMAT(FROM_DAYS( DATEDIFF( CURRENT_DATE, DATE(AES_DECRYPT(UNHEX(u.dob), 'Mytonomy')))
        ),
        '%y'
    )  ORDER BY COUNT(ucc.id) DESC )  AS tbl
GROUP BY ageband ORDER BY ageband ASC LIMIT 5;")
end
 
 def self.fetch_sumary_unique_videos_watched_cardiac(role,org_id,start_date,end_date,proc_id,facility_id)
   
  where_condition = "ru.role_id in ("+role.to_s+") and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')" 
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end

  join_clause = ''  
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = ou.user_id"
    where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
  end
   
  o_id = org_id
   ActiveRecord::Base.connection.execute("select avg(t.vid_count) as avg_count from ( SELECT count(DISTINCT user_content_consumes.content_id) as vid_count,user_content_consumes.user_id
 FROM `user_content_consumes` JOIN users u ON u.id= user_content_consumes.user_id
  JOIN roles_users ru ON ru.user_id=u.id JOIN organization_users ou on ou.user_id = u.id "+join_clause+"
  join health_conditions_users hcu on hcu.user_id = u.id join health_conditions hc on hc.id = hcu.health_condition_id 
 WHERE (user_content_consumes.content_id  IN (
SELECT c.content_id FROM `library_health_conditions` lhc     
    JOIN `library_playlists` lp ON lp.`library_template_id` = lhc.`library_template_id`
    JOIN `playlists` p ON p.id= lp.`playlist_id` 
    JOIN collections c ON c.playlist_id = p.id and c.parent_playlist is not null
    WHERE lhc.health_condition_id IN ("+proc_id.to_s+") AND p.deactivated=0 AND content_id IS NOT NULL)  and "+where_clause+" and DATE(CONVERT_TZ(user_content_consumes.updated_at,'UTC','US/Eastern')) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'and user_content_consumes.percent_consumed = 100 and u.demo = 0)
 group by user_content_consumes.user_id)t")
 end 
 
 #-------------------------------------Heal 4187------------------
 
 
 def self.fetch_viewership_metrics_by_topic_cardiac(role,org_id,start_date,end_date,proc_id,patientgrp_id,facility_id)
  where_clause_group = ""
  if !proc_id.blank?
    where_clause = " FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
    health_condition1 = "( "
    health_cond1 = "hcu.health_condition_id"
    health_condition1 = health_condition1 + health_conditions_where_clause(proc_id,health_cond1)
    health_condition1 = health_condition1 + " )" 
    where_clause1 = health_condition1
  end
  
  if !patientgrp_id.blank?
    where_clause_group = "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end
  
  if !proc_id.blank? && !patientgrp_id.blank?
    where_clause_group = "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end 

  join_clause = '' 
  facility_where_clause = '' 
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = ou.user_id"
    facility_where_clause = " and fu.facility_id = "+facility_id.to_s+""
  end
   
  o_id = org_id


ActiveRecord::Base.connection.execute("select h.name as healthCondition,h.totalCount,older.ArcheiveCt,hcu.avgTotalView,hcu.avgUniqueView
from
(select
count(distinct c.content_id) as totalCount,hc.name,hc.id
from health_conditions hc
join health_conditions_playlists hcp on hcp.health_condition_id = hc.id
join collections c on c.playlist_id=hcp.playlist_id
join health_conditions_users hcu on hcu.health_condition_id = hc.id
left join user_units uu on uu.user_id = hcu.user_id
join roles_users ru on ru.user_id=hcu.user_id
where ru.role_id IN ("+role.to_s+") and hc.organization_id = '"+org_id.to_s+"' and "+where_clause+" "+where_clause_group+" and hcp.Ref_Survey_Id is null and c.content_id is not null
group by hc.id) h

left join
(select
hc.id,hc.name,count(distinct older.content_id) as ArcheiveCt from health_conditions hc
join health_conditions_users hcu on hcu.health_condition_id = hc.id
join
(select hc.id,c.content_id,c.playlist_id from health_conditions hc
join arch_health_conditions_playlists hcp on hcp.health_condition_id = hc.id
join arch_collections c on c.playlist_id=hcp.playlist_id
where hc.organization_id = '"+org_id.to_s+"' and "+where_clause+" and c.content_id is not null
group by hc.id,c.content_id,c.playlist_id
) older on older.id = hcu.health_condition_id

left join
(
select c.content_id,hc.id as currentModule,c.playlist_id from health_conditions hc
join health_conditions_playlists hcp on hcp.health_condition_id= hc.id
join collections c on c.playlist_id = hcp.playlist_id
where hc.organization_id = '"+org_id.to_s+"' and "+where_clause+"  and hcp.Ref_Survey_Id is null and c.content_id is not null
)cuurent on cuurent.currentModule = older.id and older.content_id  = cuurent.content_id
where hc.organization_id = '"+org_id.to_s+"' and "+where_clause+"  and older.content_id is not null and  cuurent.currentModule is null
group by hc.id
) older on older.id= h.id

left join

(select t1.healthConditionId,avg(t1.uniqueView) as avgUniqueView,avg(t1.totalView) as avgTotalView from
(select t.healthConditionId,t.user_id,sum(t.uniqueView) as uniqueView,sum(t.totalView) as totalView
from
(select hc.id as healthConditionId,u.id as user_id ,count(distinct ucc.content_id) as uniqueView,count(ucl.id) as totalView from users u
left join user_units uu on uu.user_id = u.id
join organization_users ou on ou.user_id = u.id
"+join_clause+"
join roles_users ru on ru.user_id=u.id
join health_conditions_users hcu on hcu.user_id = u.id
join health_conditions_playlists hcp on hcp.health_condition_id = hcu.health_condition_id
join health_conditions hc on hc.id = hcu.health_condition_id
join collections c on c.playlist_id=hcp.playlist_id
join contents cn on cn.id = c.content_id
left join user_content_consumes ucc on ucc.content_id=cn.id and ucc.user_id=u.id
left join user_consumption_logs ucl on ucl.ucc_id = ucc.id
where ru.role_id IN ("+role.to_s+") and ou.deactivated=0 and "+where_clause+" "+where_clause_group+facility_where_clause+" and u.demo = 0
AND DATE(CONVERT_TZ(ucc.updated_at,'UTC','US/Eastern')) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'
and ou.organization_id = '"+org_id.to_s+"' and ucl.skip_flag=false and ucc.content_id is not null and ucc.playlist_id is not null and ucc.percent_consumed=100
group by hc.id,u.id

union

select hcu.health_condition_id as healthConditionId,u.id as user_id ,count(distinct ucc.content_id) as uniqueView,count(ucl.id) as totalView from users u
left join user_units uu on uu.user_id = u.id
join organization_users ou on ou.user_id = u.id
"+join_clause+"
join roles_users ru on ru.user_id=u.id
join health_conditions_users hcu on hcu.user_id = u.id
join (select hc.id,c.content_id,c.playlist_id from health_conditions hc
join arch_health_conditions_playlists hcp on hcp.health_condition_id = hc.id
join arch_collections c on c.playlist_id=hcp.playlist_id
where hc.organization_id = '"+org_id.to_s+"' and "+where_clause+"
and c.content_id is not null and c.content_id not in (
select c.content_id from
health_conditions hc
join health_conditions_playlists hcp on hcp.health_condition_id= hc.id
join collections c on c.playlist_id = hcp.playlist_id
where hc.organization_id = '"+org_id.to_s+"' and "+where_clause+" and hcp.Ref_Survey_Id is null and c.content_id is not null
)
group by hc.id,c.content_id,c.playlist_id
) old on old.id = hcu.health_condition_id
left join user_content_consumes ucc on ucc.content_id=old.content_id and ucc.user_id=u.id and ucc.playlist_id = old.playlist_id
left join user_consumption_logs ucl on ucl.ucc_id = ucc.id
where ru.role_id IN ("+role.to_s+") and ou.deactivated=0 and "+where_clause1+" "+where_clause_group+facility_where_clause+" and u.demo = 0
AND DATE(CONVERT_TZ(ucc.updated_at,'UTC','US/Eastern')) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'
and ou.organization_id = '"+org_id.to_s+"' and ucl.skip_flag=false and ucc.content_id is not null and ucc.playlist_id is not null and ucc.percent_consumed=100
group by hcu.health_condition_id,u.id
)t
group by t.healthConditionId,t.user_id
)t1
group by t1.healthConditionId
) hcu on hcu.healthConditionId = h.id
where hcu.avgTotalView is not null or avgUniqueView is not null")  

end
 
 def self.fetch_sumary_total_videos_watched_cardiac(role,org_id,start_date,end_date,proc_id,facility_id)
   
   where_condition = "ru.role_id in ("+role.to_s+") and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')" 
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end

  join_clause = ''  
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = ou.user_id"
    where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
  end

   o_id = org_id
  
   ActiveRecord::Base.connection.execute("select avg(t.vid_count) as avg_count from ( SELECT COUNT(distinct ucl.id) as vid_count,user_content_consumes.user_id
 FROM `user_content_consumes` JOIN users u ON u.id= user_content_consumes.user_id 
 JOIN roles_users ru ON ru.user_id=u.id JOIN organization_users ou on ou.user_id = u.id "+join_clause+"
 JOIN user_consumption_logs ucl on ucl.ucc_id = user_content_consumes.id
 join health_conditions_users hcu on hcu.user_id = u.id join health_conditions hc on hc.id = hcu.health_condition_id
  WHERE ( user_content_consumes.content_id  IN (
SELECT c.content_id FROM `library_health_conditions` lhc     
    JOIN `library_playlists` lp ON lp.`library_template_id` = lhc.`library_template_id`
    JOIN `playlists` p ON p.id= lp.`playlist_id` 
    JOIN collections c ON c.playlist_id = p.id and c.parent_playlist is not null
    WHERE lhc.health_condition_id IN ("+proc_id.to_s+") AND p.deactivated=0 AND content_id IS NOT NULL)  and ucl.skip_flag=false and "+where_clause+"  and DATE(CONVERT_TZ(user_content_consumes.updated_at,'UTC','US/Eastern')) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'and user_content_consumes.percent_consumed = 100 and u.demo = 0 )
 group by user_content_consumes.user_id)t")
  
 
end

def self.fetch_total_no_of_videos_cardiac(role,org_id,proc_id)
  ActiveRecord::Base.connection.execute("SELECT COUNT(c.content_id) as total FROM `library_health_conditions` lhc     
  JOIN `library_playlists` lp ON lp.`library_template_id` = lhc.`library_template_id`
  JOIN `playlists` p ON p.id= lp.`playlist_id` 
  JOIN collections c ON c.playlist_id = p.id and c.parent_playlist is not null
  WHERE lhc.health_condition_id IN ("+proc_id.to_s+") AND p.deactivated=0 AND content_id IS NOT NULL")
end
  
  #-------------Heal-4181----------------
  def self.fetch_total_videos_watched_hospital_cardiac(role,org_id,start_date,end_date,proc_id,patientgrp_id,facility_id)
    
    where_condition = "ru.role_id in ("+role.to_s+") and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')" 
  if !proc_id.blank?
    health_condition = " and ( "
    health_cond = "hc.id"
    health_condition = health_condition + health_conditions_where_clause(proc_id,health_cond)
    #where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
    health_condition = health_condition + " )" 
    where_clause = where_condition+health_condition
    health_condition1 = "( "
    health_cond1 = "lhc.health_condition_id"
    health_condition1 = health_condition1 + health_conditions_where_clause(proc_id,health_cond1)
    #where_clause = where_condition+ "and FIND_IN_SET(hc.id,'"+proc_id.to_s+"')"
    health_condition1 = health_condition1 + " )" 
    where_clause1 = health_condition1
  else
    where_clause = where_condition 
  end
  
  if !patientgrp_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end

  join_clause = ''  
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = ou.user_id"
    where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
  end
  
  UserContentConsume.where(" user_content_consumes.percent_consumed = 100 and user_content_consumes.content_id IN (
SELECT c.content_id FROM `library_health_conditions` lhc     
    JOIN `library_playlists` lp ON lp.`library_template_id` = lhc.`library_template_id`
    JOIN `playlists` p ON p.id= lp.`playlist_id` 
    JOIN collections c ON c.playlist_id = p.id and c.parent_playlist is not null
    WHERE "+where_clause1+" AND p.deactivated=0 AND content_id IS NOT NULL) and ucl.skip_flag=false and "+where_clause+" AND DATE(CONVERT_TZ(user_content_consumes.updated_at,'UTC','US/Eastern')) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' AND u.demo=0 AND ou.deactivated=0
    AND ou.deactivated=0 ")
  .joins("JOIN `user_consumption_logs` ucl ON ucl.ucc_id=user_content_consumes.id")
  .joins("JOIN users u ON u.id=user_content_consumes.user_id")
  .joins("left join user_units uu on uu.user_id = u.id")
  .joins("JOIN `organization_users` ou ON ou.user_id=u.id")
  .joins("#{join_clause}")
  .joins("JOIN `organizations` o ON o.id=ou.organization_id")
  .joins("join health_conditions_users hcu on hcu.user_id = u.id")
  .joins("join health_conditions hc on hc.id = hcu.health_condition_id")  
  .joins("JOIN `roles_users` ru ON ru.user_id=u.id")
  .joins("JOIN contents c ON c.id=user_content_consumes.content_id")
  .joins("join health_conditions_users hcu on hcu.user_id = u.id")
  .joins("join health_conditions hc on hc.id = hcu.health_condition_id")
  .group("o.id")
  .select("o.name, COUNT(DISTINCT user_content_consumes.id) AS uniqueViews,COUNT(DISTINCT ucl.id) AS TotalViews")
  
    
end
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  #----------------cardiac query start-------------------
  
  def self.fetch_top_videos_watched_sanofi(role,org_id,procedure,start_date,end_date)
  
  UserContentConsume.select("COUNT(DISTINCT ucl.id) AS totalViews ,COUNT(DISTINCT user_content_consumes.user_id) AS uniquewatch, ct.name as vid_name")
  .joins("JOIN users u ON u.id= user_content_consumes.user_id")
  .joins("JOIN contents ct ON ct.id=user_content_consumes.content_id")
  .joins("JOIN user_consumption_logs ucl on ucl.ucc_id = user_content_consumes.id")  
  .joins("JOIN roles_users ru ON ru.user_id=u.id")
  .joins("JOIN organization_users ou on ou.user_id = u.id") 
  .joins("left join terms_users tu on tu.user_id = u.id") 
.where("ru.role_id=? and tu.term_id=2 and tu.accepted=1 AND FIND_IN_SET(ou.organization_id,?)  AND user_content_consumes.percent_consumed = 100 and DATE(user_content_consumes.updated_at)
       BETWEEN ? and ? and u.demo = 0",role,org_id,start_date,end_date).group("ct.id").order("COUNT(ucl.id) DESC").limit(5)
  
end
  
  def self.fetch_patients_count_watched_56_videos(role,org_id,start_date,end_date)
 ActiveRecord::Base.connection.execute("select count(t.user_id) as users,t.name,t.org_id from(select  ucc.user_id,o.name,o.id as org_id
from user_content_consumes ucc 
join organization_users ou on ou.user_id = ucc.user_id 
JOIN organizations o ON o.id=ou.organization_id 
join terms_users on terms_users.user_id = ucc.user_id 
join users u on u.id = ucc.user_id 
JOIN roles_users ru ON ru.user_id=u.id 
where  ou.deactivated = 0  AND term_id=2 AND accepted=1 and u.created_at BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'
and ucc.content_id IS NOT NULL  AND ucc.percent_consumed=100  AND ru.role_id = "+role.to_s+" 
AND FIND_IN_SET(ou.organization_id ,'"+org_id.to_s+"')
group by ucc.user_id
having COUNT(DISTINCT ucc.content_id) >=56)t
group by t.name;")
 end
 
   def self.fetch_patients_count_watched_50_videos_athome(role,org_id)
 ActiveRecord::Base.connection.execute("select count(t.user_id) as users,t.name from(select  ucc.user_id,o.name
from user_content_consumes ucc 
join organization_users ou on ou.user_id = ucc.user_id 
JOIN organizations o ON o.id=ou.organization_id 
join terms_users on terms_users.user_id = ucc.user_id 
join users u on u.id = ucc.user_id 
JOIN roles_users ru ON ru.user_id=u.id 
where  ou.deactivated = 0  AND term_id=2 AND accepted=1 and u.created_at BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'
and ucc.content_id IS NOT NULL  AND ucc.percent_consumed=100  AND ru.role_id = "+role.to_s+" 
AND FIND_IN_SET(ou.organization_id ,'"+org_id.to_s+"')
group by ucc.user_id
having COUNT(DISTINCT ucc.content_id) >=50)t
group by t.name;")
 end 
 
 
 def self.fetch_pam_questions_with_largest_chage(role,org_id,start_date,end_date)
     ActiveRecord::Base.connection.execute("SELECT pre.question,AVG(pre.ans_value) AS preAverage,AVG(post.ans_value) AS postAverage,(AVG(post.ans_value)-AVG(pre.ans_value)) AS diff
     FROM
(SELECT u.id,q.`question`,ans_value
FROM `survey_user_answers` sua
JOIN answers a ON a.id= sua.`answer_id`
JOIN  questions q ON q.id=sua.`question_id`
JOIN  `users` u ON u.id= sua.user_id
JOIN `organization_users` ou ON ou.user_id=u.id
JOIN terms_users tu ON tu.user_id=u.id
WHERE q.`survey_section_id` = 3 and u.created_at BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'
AND FIND_IN_SET(u.organization_id ,'"+org_id.to_s+"') AND (a.ans_value IS NOT NULL AND a.ans_value != 5) AND sua.answer_id IS NOT NULL
AND tu.term_id=2 AND tu.accepted=1 AND ou.deactivated=0 AND u.demo=0) pre

JOIN 
(SELECT u.id,q.`question`,ans_value
FROM `survey_user_answers` sua
JOIN answers a ON a.id= sua.`answer_id`
JOIN  questions q ON q.id=sua.`question_id`
JOIN  `users` u ON u.id= sua.user_id
JOIN `organization_users` ou ON ou.user_id=u.id
JOIN terms_users tu ON tu.user_id=u.id
WHERE q.`survey_section_id` = 9 and u.created_at BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'
AND FIND_IN_SET(u.organization_id ,'"+org_id.to_s+"') AND (a.ans_value IS NOT NULL AND a.ans_value != 5) AND sua.answer_id IS NOT NULL
AND tu.term_id=2 AND tu.accepted=1 AND ou.deactivated=0 AND u.demo=0
) post ON post.question= pre.question AND post.id=pre.id
  GROUP BY pre.question
  ORDER BY diff DESC LIMIT 3")
  
end


def self.fetch_pam_questions_with_largest_chage1(role,org_id,start_date,end_date)
     ActiveRecord::Base.connection.execute("SELECT pre.question,AVG(pre.ans_value) AS preAverage,AVG(post.ans_value) AS postAverage,(AVG(post.ans_value)-AVG(pre.ans_value)) AS diff
     FROM
(SELECT u.id,q.`question`,ans_value
FROM `survey_user_answers` sua
JOIN answers a ON a.id= sua.`answer_id`
JOIN  questions q ON q.id=sua.`question_id`
JOIN  `users` u ON u.id= sua.user_id
JOIN `organization_users` ou ON ou.user_id=u.id
JOIN terms_users tu ON tu.user_id=u.id
WHERE q.`survey_section_id` = 3 and u.created_at BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'
AND FIND_IN_SET(u.organization_id ,'"+org_id.to_s+"') AND (a.ans_value IS NOT NULL AND a.ans_value != 5) AND sua.answer_id IS NOT NULL
AND tu.term_id=2 AND tu.accepted=1 AND ou.deactivated=0 AND u.demo=0) pre

JOIN 
(SELECT u.id,q.`question`,ans_value
FROM `survey_user_answers` sua
JOIN answers a ON a.id= sua.`answer_id`
JOIN  questions q ON q.id=sua.`question_id`
JOIN  `users` u ON u.id= sua.user_id
JOIN `organization_users` ou ON ou.user_id=u.id
JOIN terms_users tu ON tu.user_id=u.id
WHERE q.`survey_section_id` = 9 and u.created_at BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'
AND FIND_IN_SET(u.organization_id ,'"+org_id.to_s+"') AND (a.ans_value IS NOT NULL AND a.ans_value != 5) AND sua.answer_id IS NOT NULL
AND tu.term_id=2 AND tu.accepted=1 AND ou.deactivated=0 AND u.demo=0
) post ON post.question= pre.question AND post.id=pre.id
  GROUP BY pre.question
  ORDER BY diff DESC")
  
end
 def self.fetch_ease_of_use_sanofi_by_hospital(org_id,start_date,end_date)
    ActiveRecord::Base.connection.execute("SELECT ROUND(AVG(t.individualPer),2) AS rating, t.name,t.o_id,t.org_name 
  FROM
  (SELECT COUNT(sua.id),que.question,ss.name,SUM(a.ans_value),ou.organization_id as o_id,o.name as org_name, (SUM(a.ans_value)/COUNT(sua.id)) AS individualPer 
  FROM survey_user_answers sua
  JOIN users u ON u.id = sua.user_id
  JOIN answers a ON a.id= sua.`answer_id`
  JOIN questions que ON que.id=sua.question_id
  JOIN `organization_users` ou ON ou.user_id=u.id
  join organizations o ON ou.organization_id=o.id 
  JOIN section_subtitles ss ON ss.id=que.`section_subtitle_id`
  WHERE sua.answer_id IS NOT NULL AND   que.survey_section_id IN (15,17)
  AND (DATE( sua.`updated_at`) BETWEEN  DATE('" + start_date.to_s + "') AND DATE('"+end_date.to_s+"'))
  AND FIND_IN_SET(ou.organization_id ,'"+org_id.to_s+"')  AND u.demo=0 AND  ou.deactivated=0 
  GROUP BY ss.name,que.id,ou.organization_id) t 
  GROUP BY t.o_id,t.name")
    
  end
 
def self.top_videos_watched_gender_sanofi(role,org_id,gender,start_date,end_date)
  
  UserContentConsume.select("ct.name as vid_name,AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') AS gender,COUNT(ucl.id) AS ct")
  .joins("JOIN users u ON u.id= user_content_consumes.user_id")
  .joins("JOIN contents ct ON ct.id=user_content_consumes.content_id")
  .joins("JOIN user_consumption_logs ucl on ucl.ucc_id = user_content_consumes.id")  
  .joins("JOIN roles_users ru ON ru.user_id=u.id")
  .joins("JOIN organization_users ou on ou.user_id = u.id")
  .joins("left join terms_users tu on tu.user_id = u.id")  
.where("ru.role_id=? AND FIND_IN_SET(ou.organization_id ,?) and user_content_consumes.percent_consumed = 100 AND AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') = '"+gender.to_s+"' AND user_content_consumes.percent_consumed = 100 and DATE(user_content_consumes.updated_at)
       BETWEEN ? and ? and u.demo = 0",role,org_id,start_date,end_date).group("ct.name,u.gender").order("COUNT(ucl.id) DESC").limit(3)
  
end

def self.fetch_top_videos_watched_age_sanofi(role,org_id,start_date,end_date)
  o_id = org_id
 
 ActiveRecord::Base.connection.execute("SELECT tbl1.age,tbl1.name,tbl1.ageband,MAX(totalCt)
FROM 
(SELECT  tbl.age,tbl.name,
 CASE
   WHEN age >=10 AND age <=19 THEN '10 - 19'
   WHEN age >=20 AND age <=29 THEN '20 - 29'
   WHEN age >=30 AND age <=39 THEN '30 - 39'
   WHEN age >=40 AND age <=49 THEN '40 - 49'
   WHEN age >=50 AND age <=59 THEN '50 - 59'
   WHEN age >=60 AND age <=69 THEN '60 - 69'
   WHEN age >=70  THEN '70+' 
 END AS ageband,SUM(tbl.ct) AS totalCt
FROM    
   (SELECT COUNT(ucl.id) AS ct ,c.name,DATE_FORMAT(FROM_DAYS( DATEDIFF( CURRENT_DATE, DATE(AES_DECRYPT(UNHEX(u.dob),
'Mytonomy')))
       ),
       '%y'
   ) AS age
   
   FROM user_content_consumes ucc,users u ,roles_users ru,contents c ,organization_users ou,user_consumption_logs ucl, terms_users tu  
   WHERE  ou.user_id = ucc.user_id AND tu.term_id=2 AND tu.accepted=1 AND tu.user_id = u.id AND u.id=ucc.user_id AND ucl.ucc_id = ucc.id AND
ucc.percent_consumed = 100 AND ru.user_id = u.id AND ru.role_id =1 AND c.id=ucc.content_id AND c.content_type='ooyala'
   AND FIND_IN_SET(ou.organization_id ,'"+o_id.to_s+"') AND 
   DATE(ucc.updated_at) BETWEEN '"+start_date.to_s+"' AND '"+end_date.to_s+"' AND u.demo = 0
   GROUP BY c.id,DATE_FORMAT(FROM_DAYS( DATEDIFF( CURRENT_DATE, DATE(AES_DECRYPT(UNHEX(u.dob), 'Mytonomy')))
       ),
       '%y'
   )  ORDER BY COUNT(ucl.id) DESC )  AS tbl
GROUP BY ageband,tbl.name ORDER BY ageband,SUM(tbl.ct) DESC)tbl1  -- ASC LIMIT 5; 
GROUP BY tbl1.ageband")
end
 

  
  
 def self.fetch_sumary_unique_videos_watched_sanofi(role,org_id,start_date,end_date)
  o_id = org_id
   ActiveRecord::Base.connection.execute("select avg(t.vid_count) as avg_count from ( SELECT count(DISTINCT user_content_consumes.content_id) as vid_count,user_content_consumes.user_id
 FROM `user_content_consumes` JOIN users u ON u.id= user_content_consumes.user_id
 left join terms_users tu on tu.user_id = u.id 
 JOIN roles_users ru ON ru.user_id=u.id JOIN organization_users ou on ou.user_id = u.id 
 WHERE (ru.role_id= "+role.to_s+" AND FIND_IN_SET(ou.organization_id ,'"+o_id.to_s+"') and tu.term_id=2 and tu.accepted=1 and DATE(user_content_consumes.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'and user_content_consumes.percent_consumed = 100 and u.demo = 0)
 group by user_content_consumes.user_id)t")
  
 
end
 
 
 def self.fetch_sumary_total_videos_watched_sanofi(role,org_id,start_date,end_date)
   o_id = org_id
  
   ActiveRecord::Base.connection.execute("select avg(t.vid_count) as avg_count from ( SELECT COUNT(ucl.id) as vid_count,user_content_consumes.user_id
 FROM `user_content_consumes` JOIN users u ON u.id= user_content_consumes.user_id 
 JOIN roles_users ru ON ru.user_id=u.id JOIN organization_users ou on ou.user_id = u.id 
 JOIN user_consumption_logs ucl on ucl.ucc_id = user_content_consumes.id
 left join terms_users tu on tu.user_id = u.id 
 WHERE (ru.role_id= "+role.to_s+" AND FIND_IN_SET(ou.organization_id ,'"+o_id.to_s+"') and tu.term_id=2 and tu.accepted=1 and DATE(user_content_consumes.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'and user_content_consumes.percent_consumed = 100 and u.demo = 0)
 group by user_content_consumes.user_id)t")
  
 
end


def self.fetch_top_videos_watched_race_sanofi(role,org_id,start_date,end_date)
  
 o_id = org_id
  
    
    ActiveRecord::Base.connection.execute("SELECT t1.vid_name,t1.etn_name,MAX(t1.ct) AS vid_count
FROM
 (SELECT  
t.name AS vid_name,t.ethinicity AS etn_name ,
-- MAX(ct) as vid_count 
ct
FROM `ethnicities` 
CROSS JOIN (SELECT  e.id,c.name,e.`ethinicity`,COUNT(ucl.id) AS ct    
    FROM 
user_content_consumes ucc,users u ,roles_users ru,contents c,user_consumption_logs ucl,`ethnicities` e ,
organization_users ou,
    terms_users tu 
    WHERE tu.user_id = u.id AND ucc.percent_consumed = 100 AND u.id=ucc.user_id AND ru.user_id = u.id 
AND ru.role_id = "+ role.to_s+" AND c.id=ucc.content_id AND c.content_type='ooyala' 
    AND ucl.ucc_id = ucc.id AND u.organization_id IN("+o_id.to_s+") AND e.id=u.`ethinicity` AND ou.user_id = u.id 
     AND DATE(ucc.updated_at) BETWEEN '"+start_date.to_s+"' AND '"+end_date.to_s+"' AND u.demo = 0 
AND tu.term_id=2 AND tu.accepted=1
    GROUP BY c.id,u.ethinicity ORDER BY COUNT(ucl.id) DESC ) t ON t.id = ethnicities.id 
ORDER BY ct DESC)t1
 GROUP BY t1.etn_name limit 5")
    
    
    
 
end


def self.fetch_top_videos_watched_ethnicity_sanofi(role,org_id,start_date,end_date)
  
  o_id = org_id
   
  
  
   ActiveRecord::Base.connection.execute("SELECT t1.vid_name,t1.etn_name,MAX(t1.ct) AS vid_count
FROM
 (SELECT  
t.name AS vid_name,t.ethinicity AS etn_name ,
-- MAX(ct) as vid_count 
ct
FROM `parent_ethnicities` 
CROSS JOIN (SELECT  e.id,c.name,e.`ethinicity`,COUNT(ucl.id) AS ct    
    FROM 
user_content_consumes ucc,users u ,roles_users ru,contents c,user_consumption_logs ucl,`parent_ethnicities` e ,
organization_users ou,
    terms_users tu 
    WHERE tu.user_id = u.id AND ucc.percent_consumed = 100 AND u.id=ucc.user_id AND ru.user_id = u.id 
AND ru.role_id = "+ role.to_s+" AND c.id=ucc.content_id AND c.content_type='ooyala' 
    AND ucl.ucc_id = ucc.id AND u.organization_id IN("+o_id.to_s+") AND e.id=u.`parent_ethnicities_id` AND ou.user_id = u.id 
     AND DATE(ucc.updated_at) BETWEEN '"+start_date.to_s+"' AND '"+end_date.to_s+"' AND u.demo = 0 
AND tu.term_id=2 AND tu.accepted=1
    GROUP BY c.id,u.parent_ethnicities_id ORDER BY COUNT(ucl.id) DESC ) t ON t.id = parent_ethnicities.id 
ORDER BY ct DESC)t1
 GROUP BY t1.etn_name limit 5")
    
 
end
  
  
  #Reuse for Dashborad API
  def self.fetch_consent_count_sanofi(start_date,end_date,org_id)
    
    ActiveRecord::Base.connection.execute("call Sp_InvitedConsentedPatientsCount('" + start_date.to_s + "','"+end_date.to_s+"','"+org_id.to_s+"')")
    
    
    
  end

  
  def self.fetch_video_watched_hospital_creation_sanofi(start_date,end_date,org_id)
    ActiveRecord::Base.connection.execute("call Sp_VideoWatchSinceAccountCreationHospitalWise('" + start_date.to_s + "','"+end_date.to_s+"')")
  end

def self.fetch_pam_pre_survey_data_sanofi(role,org_id,start_date,end_date)
  o_id = org_id
  
  ActiveRecord::Base.connection.execute("SELECT IFNULL(ROUND(AVG(avgIndividual),2),0) AS pampreSurveyData  
FROM 
(SELECT sua.user_id,SUM(a.ans_value),COUNT(sua.question_id), (SUM(a.ans_value)/COUNT(sua.question_id)) AS avgIndividual
FROM `survey_user_answers` sua 
JOIN answers a ON a.id= sua.`answer_id`
JOIN organization_users ou on ou.user_id = sua.user_id
JOIN users on users.id = sua.user_id 
JOIN roles_users ru ON ru.user_id=users.id 
left join terms_users tu on tu.user_id = users.id
WHERE ru.role_id = "+ role.to_s+" and (a.ans_value IS NOT NULL AND a.ans_value != 5) and tu.term_id=2 and tu.accepted=1 AND FIND_IN_SET(ou.organization_id ,'"+o_id.to_s+"')
AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and users.demo = 0
 AND  sua.question_id IN (SELECT q.id FROM questions q WHERE q.`survey_section_id` IN (SELECT id FROM `survey_section_titles` WHERE survey_id=3 AND is_black_and_white=1  ) )
  GROUP BY user_id) t;").first
end


def self.fetch_pam_post_survey_data_sanofi(role,org_id,start_date,end_date)
   o_id = org_id
 
  
  ActiveRecord::Base.connection.execute("SELECT IFNULL(ROUND(AVG(avgIndividual),2),0) AS pampreSurveyData  
FROM 
(SELECT sua.user_id,SUM(a.ans_value),COUNT(sua.question_id), (SUM(a.ans_value)/COUNT(sua.question_id)) AS avgIndividual
FROM `survey_user_answers` sua 
JOIN answers a ON a.id= sua.`answer_id`
JOIN organization_users ou on ou.user_id = sua.user_id
JOIN users on users.id = sua.user_id
JOIN roles_users ru ON ru.user_id=users.id 
WHERE ru.role_id= "+ role.to_s+" AND (a.ans_value IS NOT NULL AND a.ans_value != 5) AND FIND_IN_SET(ou.organization_id ,'"+o_id.to_s+"')
AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and users.demo = 0
 AND  sua.question_id IN (SELECT q.id FROM questions q WHERE q.`survey_section_id` IN (SELECT id FROM `survey_section_titles` WHERE survey_id=4 AND is_black_and_white=1  ) )
  GROUP BY user_id) t;").first
end

def self.fetch_ease_of_use_sanofi(role,org_id,start_date,end_date)
   ActiveRecord::Base.connection.execute("call Sp_OverallEaseOfUse('" + start_date.to_s + "','"+end_date.to_s+"','"+org_id.to_s+"')")
   
end

def self.fetch_average_pem_all_hospital_sanofi(role,org_id,start_date,end_date)
   ActiveRecord::Base.connection.execute("call Sp_AveragePamScoreAllHospitals('" + start_date.to_s + "','"+end_date.to_s+"','"+org_id.to_s+"')")
   
end


def self.fetch_taking_insulin_pre_survey_responses_sanofi(role,org_id,start_date,end_date)
   o_id = org_id
  Answer.select("IFNULL(t.answerCount,0) as ans_count,answers.answer as ans_label")
  .joins("LEFT JOIN (SELECT  COUNT(sua.id) AS answerCount,a.answer,sua.answer_id
FROM `survey_user_answers` sua 
JOIN answers a ON a.id = sua.answer_id
JOIN users u ON u.id=sua.user_id
JOIN roles_users ru ON ru.user_id=u.id
left join terms_users tu on tu.user_id = u.id
JOIN organization_users ou on ou.user_id = sua.user_id
WHERE  ru.role_id = "+role.to_s+" and sua.question_id = 27 and tu.term_id=2 and tu.accepted=1 AND FIND_IN_SET(u.organization_id ,'"+o_id.to_s+"') 
 AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY sua.answer_id)t ON t.answer_id = answers.id")
.where("answers.survey_section_id = 4")

end

def self.fetch_taking_insulin_post_survey_responses_sanofi(role,org_id,start_date,end_date)
   o_id = org_id
  Answer.select("IFNULL(t.answerCount,0) as ans_count,answers.answer as ans_label")
  .joins("LEFT JOIN (SELECT  COUNT(sua.id) AS answerCount,a.answer,sua.answer_id
FROM `survey_user_answers` sua 
JOIN answers a ON a.id = sua.answer_id
JOIN users u ON u.id=sua.user_id
JOIN roles_users ru ON ru.user_id=u.id
JOIN organization_users ou on ou.user_id = sua.user_id
WHERE sua.question_id = 30 AND FIND_IN_SET(u.organization_id ,'"+o_id.to_s+"') AND ru.role_id = "+role.to_s+"
 AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY sua.answer_id)t ON t.answer_id = answers.id")
.where("answers.survey_section_id = 16")

end

def self.diabetes_with_insulin_pre_survey_responses_sanofi(role,org_id,start_date,end_date)
   o_id = org_id
  Answer.select("IFNULL(t.answerCount,0) as ans_count,answers.answer as ans_label")
  .joins("LEFT JOIN (SELECT  COUNT(sua.id) AS answerCount,a.answer,sua.answer_id
FROM `survey_user_answers` sua 
JOIN answers a ON a.id = sua.answer_id
JOIN users u ON u.id=sua.user_id
JOIN roles_users ru ON ru.user_id=u.id
JOIN organization_users ou on ou.user_id = sua.user_id
WHERE sua.question_id = 28 AND FIND_IN_SET(u.organization_id ,'"+o_id.to_s+"') AND ru.role_id = "+role.to_s+"
 AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY sua.answer_id)t ON t.answer_id = answers.id")
.where("answers.survey_section_id = 4")

end



def self.diabetes_with_insulin_post_survey_responses_sanofi(role,org_id,start_date,end_date)
   o_id = org_id
  Answer.select("IFNULL(t.answerCount,0) as ans_count,answers.answer as ans_label")
  .joins("LEFT JOIN (SELECT  COUNT(sua.id) AS answerCount,a.answer,sua.answer_id
FROM `survey_user_answers` sua 
JOIN answers a ON a.id = sua.answer_id
JOIN users u ON u.id=sua.user_id
JOIN roles_users ru ON ru.user_id=u.id
JOIN organization_users ou on ou.user_id = sua.user_id
WHERE sua.question_id = 31 AND FIND_IN_SET(u.organization_id ,'"+o_id.to_s+"') AND ru.role_id = "+role.to_s+"
 AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY sua.answer_id)t ON t.answer_id = answers.id")
.where("answers.survey_section_id = 16")

end

def self.fetch_condition_worse_pre_survey_responses_sanofi(role,org_id,start_date,end_date)
   o_id = org_id
  Answer.select("IFNULL(t.answerCount,0) as ans_count,answers.answer as ans_label")
  .joins("LEFT JOIN (SELECT  COUNT(sua.id) AS answerCount,a.answer,sua.answer_id
FROM `survey_user_answers` sua 
JOIN answers a ON a.id = sua.answer_id
JOIN users u ON u.id=sua.user_id
JOIN roles_users ru ON ru.user_id=u.id
JOIN organization_users ou on ou.user_id = sua.user_id
WHERE sua.question_id = 29 AND FIND_IN_SET(u.organization_id ,'"+o_id.to_s+"') AND ru.role_id = "+ role.to_s+"
 AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY sua.answer_id)t ON t.answer_id = answers.id")
.where("answers.survey_section_id = 4")

end



def self.fetch_condition_worse_post_survey_responses_sanofi(role,org_id,start_date,end_date)
   o_id = org_id
  Answer.select("IFNULL(t.answerCount,0) as ans_count,answers.answer as ans_label")
  .joins("LEFT JOIN (SELECT  COUNT(sua.id) AS answerCount,a.answer,sua.answer_id
FROM `survey_user_answers` sua 
JOIN answers a ON a.id = sua.answer_id
JOIN users u ON u.id=sua.user_id
JOIN roles_users ru ON ru.user_id=u.id
JOIN organization_users ou on ou.user_id = sua.user_id
WHERE sua.question_id = 32 AND FIND_IN_SET(u.organization_id ,'"+o_id.to_s+"') AND ru.role_id = "+ role.to_s+"
 AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY sua.answer_id)t ON t.answer_id = answers.id")
.where("answers.survey_section_id = 16")

end

def self.fetch_top_videos_watched_hospital_sanofi(role,org_id,start_date,end_date)
 
  
  ActiveRecord::Base.connection.execute("(SELECT  c.name,o.name,COUNT(ucl.id) AS ct    
FROM user_content_consumes ucc,users u ,roles_users ru,contents c,user_consumption_logs ucl,
`organization_users` ou,`organizations` o, terms_users tu     
WHERE u.id=ucc.user_id and tu.term_id=2 and tu.accepted=1 and tu.user_id = u.id AND ru.user_id = u.id AND ru.role_id =1 AND c.id=ucc.content_id AND c.content_type='ooyala' 
AND ucl.ucc_id = ucc.id AND u.organization_id = 5 AND ou.user_id=u.id AND DATE(ucc.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'  AND o.id=ou.organization_id AND u.demo=0 AND ou.deactivated=0
GROUP BY c.id,o.name ORDER BY COUNT(ucl.id) DESC LIMIT 2)

UNION ALL

(SELECT  c.name,o.name,COUNT(ucl.id) AS ct    
FROM user_content_consumes ucc,users u ,roles_users ru,contents c,user_consumption_logs ucl,
`organization_users` ou,`organizations` o  , terms_users tu  
WHERE u.id=ucc.user_id and tu.term_id=2 and tu.accepted=1 and tu.user_id = u.id AND ru.user_id = u.id AND ru.role_id =1 AND c.id=ucc.content_id AND c.content_type='ooyala' 
AND ucl.ucc_id = ucc.id AND u.organization_id = 6 AND ou.user_id=u.id AND DATE(ucc.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'  AND o.id=ou.organization_id AND u.demo=0 AND ou.deactivated=0
GROUP BY c.id,o.name ORDER BY COUNT(ucl.id) DESC LIMIT 2)
UNION ALL

(SELECT  c.name,o.name,COUNT(ucl.id) AS ct    
FROM user_content_consumes ucc,users u ,roles_users ru,contents c,user_consumption_logs ucl,
`organization_users` ou,`organizations` o , terms_users tu   
WHERE u.id=ucc.user_id and tu.term_id=2 and tu.accepted=1 and tu.user_id = u.id AND ru.user_id = u.id AND ru.role_id =1 AND c.id=ucc.content_id AND c.content_type='ooyala' 
AND ucl.ucc_id = ucc.id AND u.organization_id = 7 AND ou.user_id=u.id  AND DATE(ucc.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'  AND o.id=ou.organization_id AND u.demo=0 AND ou.deactivated=0
GROUP BY c.id,o.name ORDER BY COUNT(ucl.id) DESC LIMIT 2);")
  
end

def self.fetch_total_videos_watched_hospital_sanofi(role,org_id,start_date,end_date)
  
  UserContentConsume.where("ru.role_id = "+ role.to_s+" and tu.term_id=2 and tu.accepted=1 AND DATE(user_content_consumes.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' AND u.demo=0 
  AND ou.deactivated=0 AND c.content_type='ooyala' AND FIND_IN_SET(ou.organization_id,?)",org_id)
  .joins("JOIN `user_consumption_logs` ucl ON ucl.ucc_id=user_content_consumes.id")
  .joins("JOIN users u ON u.id=user_content_consumes.user_id")
  .joins("JOIN `organization_users` ou ON ou.user_id=u.id")
  .joins("JOIN `organizations` o ON o.id=ou.organization_id")
  .joins("JOIN `roles_users` ru ON ru.user_id=u.id")
  .joins("JOIN contents c ON c.id=user_content_consumes.content_id")
  .joins("left join terms_users tu on tu.user_id = u.id")
  .group("o.id")
  .select("o.name,COUNT(DISTINCT user_content_consumes.id) AS uniqueViews,COUNT(DISTINCT ucl.id) AS TotalViews,(COUNT(ucl.id)/ count(DISTINCT user_content_consumes.user_id)) as avgWatched")
  
    
end

def self.patient_usage_year_sanofi(role,org_id,group_str,start_date,end_date)
  
  puts "asjkjkashdjk"
  
  User.select("COUNT(DISTINCT users.id) AS Invited,COUNT(DISTINCT ucc.user_id) AS viewed,"+group_str.to_s+"(users.created_at) as day,DATE_FORMAT(users.created_at,'%m/%d') as create_date,
  DATE_FORMAT(DATE_ADD(users.created_at, INTERVAL(1-DAYOFWEEK(users.created_at)) DAY),'%m/%d') as week_start,
  DATE_FORMAT(DATE_ADD(users.created_at, INTERVAL(7-DAYOFWEEK(users.created_at)) DAY),'%m/%d') as week_end")
  .joins("JOIN organization_users ou on ou.user_id = users.id")
  .joins("JOIN roles_users ru on ru.user_id = users.id")
  .joins("left join terms_users tu on tu.user_id = users.id")
  .joins("LEFT JOIN user_content_consumes ucc on ucc.user_id = users.id and ucc.percent_consumed =100")
  .where("ru.role_id=? and tu.term_id=2 and tu.accepted=1  AND FIND_IN_SET(ou.organization_id,?)  AND  DATE(users.created_at)
       BETWEEN ? and ? and users.demo = 0",role,org_id,start_date,end_date)
       .order("users.created_at asc ")
  
end

#Reuse for Dashborad API

def self.patient_usage_sanofi(role,org_id,group_str,start_date,end_date)
  
  puts "mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm"
  puts group_str
  puts 'YEAR'
  if group_str != 'YEAR'
    
    
     results = ActiveRecord::Base.connection.execute("SELECT IFNULL(tab2.Invited,0) as Invited,IFNULL(tab2.viewed,0) as viewed,tab1.DAY ,tab2.create_date,tab1.week_start,tab1.week_end FROM
(SELECT "+group_str.to_s+"(dte) AS DAY,
DATE_FORMAT(DATE_ADD(dte, INTERVAL(1-DAYOFWEEK(dte)) DAY),'%m/%d') AS week_start,
  DATE_FORMAT(DATE_ADD(dte, INTERVAL(7-DAYOFWEEK(dte)) DAY),'%m/%d') AS week_end FROM
(SELECT '"+ start_date.to_s+"' + INTERVAL a + b DAY dte
FROM
 (SELECT 0 a UNION SELECT 1 a UNION SELECT 2 UNION SELECT 3
    UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7
    UNION SELECT 8 UNION SELECT 9 ) d,
 (SELECT 0 b UNION SELECT 10 UNION SELECT 20 
    UNION SELECT 30 UNION SELECT 40) m
WHERE '"+ start_date.to_s+"' + INTERVAL a + b DAY  <  '"+ end_date.to_s+"'
ORDER BY a + b)t
GROUP BY " +group_str.to_s+"(dte)) tab1
LEFT JOIN

(SELECT COUNT(DISTINCT users.id) AS Invited,COUNT(DISTINCT ucc.user_id) AS viewed,
" +group_str.to_s+"(users.created_at) AS DAY,DATE_FORMAT(users.created_at,'%m/%d') AS create_date,
  DATE_FORMAT(DATE_ADD(users.created_at, INTERVAL(1-DAYOFWEEK(users.created_at)) DAY),'%m/%d') AS week_start,
  DATE_FORMAT(DATE_ADD(users.created_at, INTERVAL(7-DAYOFWEEK(users.created_at)) DAY),'%m/%d') AS week_end
 FROM `users` JOIN organization_users ou ON ou.user_id = users.id JOIN roles_users ru ON ru.user_id = users.id 
 LEFT JOIN terms_users tu ON tu.user_id = users.id LEFT JOIN user_content_consumes ucc ON ucc.user_id 
= users.id AND ucc.percent_consumed =100 WHERE (ru.role_id=1 AND tu.term_id=2 AND tu.accepted=1  
AND FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')  AND  DATE(users.created_at)
       BETWEEN '"+ start_date.to_s+"' AND '"+ end_date.to_s+"' AND users.demo = 0) 
       GROUP BY " +group_str.to_s+"(users.created_at)  ORDER BY users.created_at ASC ) tab2 ON tab1.DAY=tab2.DAY  ")
    
    
       
  else
    
    
    puts "nononooonononononononononnononnno"
    
    
    
    
    results = ActiveRecord::Base.connection.execute("SELECT COUNT(DISTINCT users.id) AS Invited,COUNT(DISTINCT ucc.user_id) AS viewed,YEAR(users.created_at) as day,
    DATE_FORMAT(users.created_at,'%m/%d') as create_date,
 DATE_FORMAT(DATE_ADD(users.created_at, INTERVAL(1-DAYOFWEEK(users.created_at)) DAY),'%m/%d') as week_start,
 DATE_FORMAT(DATE_ADD(users.created_at, INTERVAL(7-DAYOFWEEK(users.created_at)) DAY),'%m/%d') as week_end 
 FROM `users` JOIN organization_users ou on ou.user_id = users.id JOIN roles_users ru on ru.user_id = users.id 
 left join terms_users tu on tu.user_id = users.id LEFT JOIN user_content_consumes ucc on ucc.user_id = users.id and ucc.percent_consumed =100 
 WHERE (ru.role_id=1 and tu.term_id=2 and tu.accepted=1  AND FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')  AND  DATE(users.created_at)
      BETWEEN '"+ start_date.to_s+"' and '"+ end_date.to_s+"' and users.demo = 0)  ORDER BY users.created_at asc ")
    
    
    
      end
   
end


  #Reuse for Dashborad API
  def self.patient_usage_invitation_date_sanofi(role,org_id,start_date,end_date)
    
    
    
  
    User.select("COUNT(ucl.id)  AS avgWatchcount,
  DATEDIFF(ucl.created_at, users.created_at) + 1  AS days")
  .joins("JOIN user_content_consumes ucc on users.id = ucc.user_id")
  .joins("JOIN organization_users ou on ou.user_id = users.id")
  .joins("JOIN roles_users ru on ru.user_id = users.id")
  .joins("JOIN user_consumption_logs ucl on ucl.ucc_id = ucc.id")
  .joins("left join terms_users tu on tu.user_id = users.id")
  .joins("JOIN health_conditions hc on hc.organization_id = users.organization_id")
  .where("ru.role_id=? and tu.term_id=2 and tu.accepted=1 and ou.deactivated=0 and users.demo = 0 AND FIND_IN_SET(ou.organization_id,?) 
  AND (DATEDIFF(ucl.created_at, users.created_at) BETWEEN 0 AND 6) AND  DATE(users.created_at)
       BETWEEN ? and ? ",role,org_id,start_date,end_date)
       .group("DATEDIFF(ucl.created_at, users.created_at) +1")
       
end


  #Reuse for Dashborad API
  def self.patient_gender_percentage_sanofi(role,org_id,start_date,end_date)
  User.select("COUNT(DISTINCT users.id) AS NumberPatient, AES_DECRYPT(UNHEX(users.gender), 'Mytonomy') AS gender")
  .joins("JOIN organization_users ou on ou.user_id = users.id")
  .joins("JOIN roles_users ru on ru.user_id = users.id")
  .joins("left join terms_users tu on tu.user_id = users.id")
  .where("ru.role_id=? and tu.term_id=2 and tu.accepted=1  AND FIND_IN_SET(ou.organization_id,?)   AND  DATE(users.created_at)
       BETWEEN ? and ? and users.demo = 0 ",role,org_id,start_date,end_date)
       .group("gender")
end


  #-------------Heal-4181----------------
  def self.patient_gender_percentage_cardiac(role,org_id,start_date,end_date,proc_id,patientgrp_id,facility_id)
    
  where_condition = "ru.role_id in ("+role.to_s+") and FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')" 
  if !proc_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(hcu.health_condition_id,'"+proc_id.to_s+"')"
  else
    where_clause = where_condition 
  end

  if !patientgrp_id.blank?
    where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
  end
  
  join_clause = ''  
  if !facility_id.nil? && !facility_id.blank?
    join_clause = "left join facility_users fu on fu.user_id = ou.user_id"
    where_clause = where_condition+ " and fu.facility_id = "+facility_id.to_s+""
  end

    #.where(" AES_DECRYPT(UNHEX(users.last_name), 'Mytonomy') "+reg_str.to_s+" '%BulkUser%' and ru.role_id=? AND FIND_IN_SET(ou.organization_id,?)   AND  DATE(users.created_at)
  User.select("COUNT( DISTINCT users.id) AS NumberPatient, AES_DECRYPT(UNHEX(users.gender), 'Mytonomy') AS gender")
  .joins("LEFT JOIN user_units uu on uu.user_id = users.id")
  .joins("JOIN organization_users ou on ou.user_id = users.id")
  .joins("#{join_clause}")
  .joins("JOIN roles_users ru on ru.user_id = users.id")
  .joins("join health_conditions_users hcu on hcu.user_id = users.id")
    .where(where_clause+" AND DATE(CONVERT_TZ(users.created_at,'UTC','US/Eastern'))
       BETWEEN ? and ? and users.demo = 0 and gender is not null  AND ou.deactivated=0  ",start_date,end_date)
       .group("gender")
end
  
   def self.fetch_all_time_patient_usage_sanofi(role,org_id,start_date,end_date)
    User.where("ru.role_id=1 and tu.term_id=2 and tu.accepted=1 AND users.organization_id IN (?)",org_id)
    .joins("LEFT JOIN user_content_consumes ucc ON ucc.user_id = users.id")
    .joins("JOIN  roles_users ru ON ru.user_id=users.id")
    .joins("left join terms_users tu on tu.user_id = users.id")
    .select("COUNT(DISTINCT users.id) AS Invited,COUNT(DISTINCT ucc.user_id) AS viewed")
    
  end
  #Reuse for Dashborad API
  def self.insurance_Type_summary_sanofi(role,org_id,start_date,end_date)
    
    o_id = org_id
    
  User.select("ins.insurance as ins_label, count(users.id) as PatientCount,ROUND(
(COUNT(users.id) / t.totalPatientCount) * 100) AS Percantage")
  .joins("JOIN organization_users ou on ou.user_id = users.id")
  .joins("JOIN roles_users ru on ru.user_id = users.id")
  .joins("LEFT JOIN insurances ins on  users.insurance=ins.id")
  .joins("left join terms_users tu on tu.user_id = users.id")
  .joins(" CROSS JOIN (SELECT COUNT(*) AS totalPatientCount FROM users,roles_users
  WHERE roles_users.user_id=users.id AND roles_users.role_id=1 and users.demo = 0 
  AND DATE(users.created_at)
       BETWEEN '"+start_date.to_s+"' and '"+ end_date.to_s+"' ) t")
  .where("ru.role_id=? and tu.term_id=2 and tu.accepted=1  AND FIND_IN_SET(ou.organization_id ,?)  AND  DATE(users.created_at)
       BETWEEN ? and ? and users.demo = 0",role,org_id,start_date,end_date)
       .group("ins.id")
       
end
#Reuse for Dashborad API
def self.fetch_patient_ethnicity_sanofi(role,org_id,start_date,end_date)
  
   o_id = org_id
  
  
  User.select("IFNULL( pe.`ethinicity`,'Not Reported') as ethin,
COUNT(users.id) AS PatientCount,
ROUND(
(COUNT(users.id) / t.totalPatientCount) * 100) AS Percantage")
.joins("LEFT JOIN `parent_ethnicities` pe ON users.`parent_ethnicities_id`=pe.id")
.joins("LEFT JOIN roles_users ur ON ur.user_id=users.id")
.joins("LEFT JOIN organization_users ou on ou.user_id = users.id")
.joins("left join terms_users tu on tu.user_id = users.id")
.joins("CROSS JOIN (SELECT COUNT(*) AS totalPatientCount 
 FROM users,roles_users,organization_users,terms_users tus WHERE tus.user_id = users.id and tus.term_id=2 and tus.accepted=1 and FIND_IN_SET(organization_users.organization_id,'"+org_id.to_s+"') and users.id = organization_users.user_id AND users.id=roles_users.user_id 
 AND roles_users.role_id=1 and DATE(users.created_at)
       BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and users.demo = 0) t")
 .where("users.demo = 0 and ur.role_id=1 and tu.term_id=2 and tu.accepted=1 AND FIND_IN_SET(ou.organization_id,?) and DATE(users.created_at)
       BETWEEN ? and ? ",org_id,start_date,end_date)
 .group("users.parent_ethnicities_id")
  
end
  
  #Reuse for Dashborad API
 def self.fetch_patient_race_sanofi(role,org_id,start_date,end_date)
  
  User.select("IFNULL( e.`ethinicity`,'Not Reported') as race,
COUNT(users.id) AS PatientCount,
ROUND(
(COUNT(users.id) / t.totalPatientCount) * 100) AS Percantage")
.joins("LEFT JOIN `ethnicities` e ON users.`ethinicity`=e.id")
.joins("LEFT JOIN roles_users ur ON ur.user_id=users.id")
.joins("LEFT JOIN organization_users ou on ou.user_id = users.id")
.joins("left join terms_users tu on tu.user_id = users.id")
.joins("CROSS JOIN (SELECT COUNT(*) AS totalPatientCount 
 FROM users,roles_users,organization_users,terms_users tus WHERE tus.user_id = users.id and tus.term_id=2 and tus.accepted=1 and users.id = organization_users.user_id AND users.id=roles_users.user_id 
 AND roles_users.role_id=1 AND FIND_IN_SET(organization_users.organization_id,'"+org_id.to_s+"') and DATE(users.created_at)
       BETWEEN '"+start_date.to_s+"' and'"+end_date.to_s+"' and users.demo = 0) t")
 .where("ur.role_id=1 and tu.term_id=2 and tu.accepted=1 AND  users.demo = 0 and FIND_IN_SET(ou.organization_id,?) and DATE(users.created_at)
       BETWEEN ? and ? ",org_id,start_date,end_date)
 .group("users.ethinicity")
  
end
#Reuse for Dashboard API
def self.insulin_brand_use_count_sanofi(role,org_id,start_date,end_date)
  
  User.select("COUNT(ub.user_id) as PatientCount,b.name as brandName,ROUND((COUNT(ub.user_id)/t.tot) * 100) AS per")
  .joins("JOIN organization_users ou on ou.user_id = users.id")
  .joins("JOIN roles_users ru on ru.user_id = users.id")
  .joins("left join terms_users tu on tu.user_id = users.id")
  .joins("CROSS JOIN(
SELECT COUNT(ubi.user_id) AS tot FROM users ui,roles_users rui,user_brands ubi, organization_users
WHERE rui.role_id=1 AND rui.user_id = ui.id AND ubi.user_id=ui.id   AND FIND_IN_SET(organization_users.organization_id,'"+org_id.to_s+"')
 and ui.demo = 0 AND DATE(ui.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"') t")
 .joins(" JOIN `user_brands` ub ON ub.user_id= users.id")
.joins("LEFT JOIN brands b ON b.id=ub.brand_id")
.joins("LEFT JOIN brand_types bt on b.brand_type =bt.id ")
.joins("left join terms_users tu on tu.user_id = users.id")
.where("ru.role_id=? and tu.term_id=2 and tu.accepted=1 and tu.term_id=2 and tu.accepted=1 AND FIND_IN_SET(ou.organization_id,?) AND  DATE(users.created_at)
       BETWEEN ? and ? and users.demo = 0",role,org_id,start_date,end_date).group("b.name").order("COUNT(ub.user_id) DESC")
  
end

#Reuse for Dashboard 
def self.insulin_brand_use_sanofi(role,org_id,start_date,end_date,page,no_of_records)
  User.select("COUNT(ub.user_id) as PatientCount,b.name as brandName,ROUND((COUNT(ub.user_id)/t.tot) * 100) AS per,bt.name as type_name")
  .joins("JOIN organization_users ou on ou.user_id = users.id")
  .joins("JOIN roles_users ru on ru.user_id = users.id")
  .joins("left join terms_users tu on tu.user_id = users.id")
  .joins("CROSS JOIN(
SELECT COUNT(ubi.user_id) AS tot FROM users ui,roles_users rui,user_brands ubi,organization_users
WHERE rui.role_id=1 AND rui.user_id = ui.id AND ubi.user_id=ui.id   AND FIND_IN_SET(organization_users.organization_id,'"+org_id.to_s+"')
and ui.demo = 0 AND DATE(ui.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"') t")
 .joins(" JOIN `user_brands` ub ON ub.user_id= users.id")
.joins("LEFT JOIN brands b ON b.id=ub.brand_id")
.joins("LEFT JOIN brand_types bt on b.brand_type =bt.id ")
.where("ru.role_id=? and tu.term_id=2 and tu.accepted=1 AND FIND_IN_SET(ou.organization_id,?) AND  DATE(users.created_at)
       BETWEEN ? and ? and users.demo = 0",role,org_id,start_date,end_date).group("b.name").order("COUNT(ub.user_id) DESC").limit(no_of_records).offset(page)
  
end

#Reuse for Dashborad API
def self.fetch_patient_age_ethnicity_sanofi(role,org_id,start_date,end_date)
  User.select("DATE_FORMAT(FROM_DAYS( DATEDIFF( CURRENT_DATE, DATE(AES_DECRYPT(UNHEX(users.dob),
   'Mytonomy')))),'%y' )AS Age,e.`ethinicity` as etini")
   .joins("LEFT JOIN roles_users ur ON ur.user_id=users.id")
   .joins("LEFT JOIN `parent_ethnicities` e ON e.id=users.`parent_ethnicities_id`")
   .joins("LEFT JOIN organization_users ou on ou.user_id = users.id")
   .joins("left join terms_users tu on tu.user_id = users.id")
   .where("ur.role_id=1 and tu.term_id=2 and tu.accepted=1 AND FIND_IN_SET(ou.organization_id,?) and users.`parent_ethnicities_id` is not null and DATE(users.created_at)
       BETWEEN ? and ? and users.demo = 0 ",org_id,start_date,end_date)
  
end
#Reuse for Dashborad API
def self.fetch_patient_age_race_sanofi(role,org_id,start_date,end_date)
  User.select("DATE_FORMAT(FROM_DAYS( DATEDIFF( CURRENT_DATE, DATE(AES_DECRYPT(UNHEX(users.dob),
   'Mytonomy')))),'%y' )AS Age,e.`ethinicity` as race")
   .joins("LEFT JOIN roles_users ur ON ur.user_id=users.id")
   .joins("LEFT JOIN `ethnicities` e ON e.id=users.`ethinicity`")
   .joins("LEFT JOIN organization_users ou on ou.user_id = users.id")
   .joins("left join terms_users tu on tu.user_id = users.id")
   .where("ur.role_id=1 and tu.term_id=2 and tu.accepted=1 AND FIND_IN_SET(ou.organization_id,?) and users.`ethinicity` is not null and DATE(users.created_at)
       BETWEEN ? and ? and users.demo = 0 ",org_id,start_date,end_date)
  
end

 #Reuse for Dashborad API
def self.patient_by_location_sanofi(role,org_id,start_date,end_date)
  
  UserConsumptionLog.select("DISTINCT `latitude`,`longitude`,city")
  .joins("JOIN login_histories lh ON lh.id=user_consumption_logs.login_id")
  .joins("JOIN users u ON u.id= lh.user_id")
  .joins("JOIN roles_users ru ON ru.user_id=u.id")
  .joins("JOIN organization_users ou on ou.user_id = u.id")  
  .joins("left join terms_users tu on tu.user_id = u.id")
.where("ru.role_id=? and tu.term_id=2 and tu.accepted=1 AND FIND_IN_SET(ou.organization_id ,?)  AND  DATE(user_consumption_logs.created_at)
       BETWEEN ? and ? and u.demo = 0",role,org_id,start_date,end_date).limit(50)
  
end

  #Reuse for Dashborad API
    def self.fetch_average_age_by_demography_sanofi(role,org_id,procedure,start_date,end_date)
     User.where("demo =0  and tu.term_id=2 and tu.accepted=1 AND roles_users.role_id = ? AND FIND_IN_SET(organization_users.organization_id ,?)  AND TIMESTAMP(users.created_at) BETWEEN ? and ? and users.demo = 0",role,org_id,start_date,end_date)
   .joins("join organization_users on users.id = organization_users.user_id")
   .joins("join roles_users on users.id = roles_users.user_id")
   .joins("left join terms_users tu on tu.user_id = users.id")
   .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
   .select("AES_DECRYPT(UNHEX(gender),'Mytonomy') AS Gender,TIMESTAMPDIFF(YEAR, AES_DECRYPT(UNHEX(dob),'Mytonomy'), CURDATE()) AS 'AGE', users.ethinicity AS 'DEMOGRAPHY'");
           
  end


  
  def self.fetch_total_users(role)
    Role.find_by_name(role).users.count 
  end
  
  def self.fetch_users_by_gender
    Role.find_by_name(role).users.count  
  end
 
 def self.fetch_widget_data(widget_id,feq_id)
    TempDashboard.where("widget_id like ? and frequency like ? ",widget_id,feq_id)
  end
 
 def self.fetch_hospital_users_count(role,org_id)
   
   User.select("os.name as hospital_name, COUNT(users.id) as total_users")
   .joins("JOIN organization_users ou on ou.user_id = users.id")
   .joins("JOIN roles_users ru on ru.user_id = users.id")
   .joins("JOIN organizations os on os.id=ou.organization_id")
   .where("ru.role_id =? and ou.organization_id = ? and users.demo = 0",role,org_id).first
   
 end
 
 #All Time Patient Usage 
  def self.fetch_all_time_patient_usage(role,org_id,procedure,start_date,end_date)
   
   User.select("COUNT(DISTINCT users.id) AS Invited,COUNT(DISTINCT ucc.user_id) AS viewed")
   .joins("JOIN organization_users ou on ou.user_id = users.id")
   .joins("JOIN roles_users ru on ru.user_id = users.id")
   .joins("LEFT JOIN user_content_consumes ucc ON ucc.user_id = users.id")
   .where("ru.role_id =? and ou.organization_id = ? and users.demo = 0 ",role,org_id).first
   
   
 end
 
 
def self.patient_usage_invitation_date(role,org_id,procedure,start_date,end_date)
  
    User.select("ROUND( (COUNT(ucc.id)/COUNT(DISTINCT users.id)))  AS avgWatchcount,
  DATEDIFF(ucc.created_at, users.created_at)+1  AS days")
  .joins("JOIN user_content_consumes ucc on users.id = ucc.user_id")
  .joins("JOIN organization_users ou on ou.user_id = users.id")
  .joins("JOIN roles_users ru on ru.user_id = users.id")
  .where("ru.role_id=? and users.demo = 0 AND ou.organization_id = ? AND DATEDIFF(ucc.created_at, users.created_at) < 7 AND  DATE(ucc.updated_at)
       BETWEEN ? and ?",role,org_id,start_date,end_date)
       .group("DATEDIFF(ucc.created_at, users.created_at) +1")
end

def self.patient_gender_percentage(role,org_id,procedure,start_date,end_date)
  User.select("COUNT(users.id) AS NumberPatient, AES_DECRYPT(UNHEX(users.gender), 'Mytonomy') AS gender")
  .joins("JOIN organization_users ou on ou.user_id = users.id")
  .joins("JOIN roles_users ru on ru.user_id = users.id")
  .where("ru.role_id=?  AND ou.organization_id = ?  AND  DATE(users.created_at)
       BETWEEN ? and ? and users.demo = 0 ",role,org_id,start_date,end_date)
       .group("gender")
end

def self.patient_usage(role,org_id,group_str,start_date,end_date)
  User.select("COUNT(DISTINCT ucc.user_id) AS numberofpatient ,"+group_str.to_s+"(users.created_at) as day,DATE_FORMAT(users.created_at,'%m/%d') as create_date, 
  DATE_FORMAT(DATE_ADD(users.created_at, INTERVAL(1-DAYOFWEEK(users.created_at)) DAY),'%m/%d') as week_start,
  DATE_FORMAT(DATE_ADD(users.created_at, INTERVAL(7-DAYOFWEEK(users.created_at)) DAY),'%m/%d') as week_end")
  .joins("JOIN organization_users ou on ou.user_id = users.id")
  .joins("JOIN roles_users ru on ru.user_id = users.id")
  .joins("JOIN user_content_consumes ucc on users.id=ucc.user_id and ucc.percent_consumed =100")
  .where("ru.role_id=?  AND ou.organization_id = ?  AND  DATE(users.created_at)
       BETWEEN ? and ? and users.demo = 0",role,org_id,start_date,end_date)
       .group(group_str.to_s+"(users.created_at)")
       .order("users.created_at asc ")
end

def self.patient_usage_cumulative(role,org_id,group_str,start_date,end_date)
  User.select("COUNT(DISTINCT users.id) AS Invited,COUNT(DISTINCT ucc.user_id) AS viewed,"+group_str.to_s+"(users.created_at) as day,DATE_FORMAT(users.created_at,'%m/%d') as create_date,
  DATE_FORMAT(DATE_ADD(users.created_at, INTERVAL(1-DAYOFWEEK(users.created_at)) DAY),'%m/%d') as week_start,
  DATE_FORMAT(DATE_ADD(users.created_at, INTERVAL(7-DAYOFWEEK(users.created_at)) DAY),'%m/%d') as week_end")
  .joins("JOIN organization_users ou on ou.user_id = users.id")
  .joins("JOIN roles_users ru on ru.user_id = users.id")
  .joins("LEFT JOIN user_content_consumes ucc on ucc.user_id = users.id and ucc.percent_consumed =100")
  .where("ru.role_id=?  AND ou.organization_id = ?  AND  DATE(users.created_at)
       BETWEEN ? and ? and users.demo = 0",role,org_id,start_date,end_date)
       .group(group_str.to_s+"(users.created_at)")
       .order("users.created_at asc ")
end

def self.insurance_Type_summary(role,org_id,procedure,start_date,end_date)
  User.select("ins.insurance as ins_label, count(users.id) as PatientCount,ROUND(
(COUNT(users.id) / t.totalPatientCount) * 100) AS Percantage")
  .joins("JOIN organization_users ou on ou.user_id = users.id")
  .joins("JOIN roles_users ru on ru.user_id = users.id")
  .joins("LEFT JOIN insurances ins on  users.insurance=ins.id")
  .joins(" CROSS JOIN (SELECT COUNT(*) AS totalPatientCount FROM users,roles_users
  WHERE roles_users.user_id=users.id AND roles_users.role_id=1 and users.demo = 0 AND users.organization_id ="+org_id.to_s+"
  AND DATE(users.created_at)
       BETWEEN '"+start_date.to_s+"' and '"+ end_date.to_s+"' ) t")
  .where("ru.role_id=?  AND ou.organization_id = ?  AND  DATE(users.created_at)
       BETWEEN ? and ? and users.demo = 0",role,org_id,start_date,end_date)
       .group("ins.id")
       
end


def self.insulin_brand_use(role,org_id,procedure,start_date,end_date)
  User.select("COUNT(ub.user_id) as PatientCount,b.name as brandName,ROUND((COUNT(ub.user_id)/t.tot) * 100) AS per")
  .joins("JOIN organization_users ou on ou.user_id = users.id")
  .joins("JOIN roles_users ru on ru.user_id = users.id")
  .joins("CROSS JOIN(
SELECT COUNT(ubi.user_id) AS tot FROM users ui,roles_users rui,user_brands ubi
WHERE rui.role_id=1 AND rui.user_id = ui.id AND ubi.user_id=ui.id   AND ui.organization_id = "+org_id.to_s+"
and ui.demo = 0 AND DATE(ui.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"') t")
 .joins("LEFT JOIN `user_brands` ub ON ub.user_id= users.id")
.joins("LEFT JOIN brands b ON b.id=ub.brand_id")
.where("ru.role_id=? AND ou.organization_id = ?  AND  DATE(users.created_at)
       BETWEEN ? and ? and users.demo = 0",role,org_id,start_date,end_date).group("b.name").order("COUNT(ub.user_id) DESC").limit(10)
  
end
   
       
   def self.in_patient_demographic(role,org_id,procedure,start_date,end_date)
  User.select("et.ethinicity as etn_label,COUNT(users.id) AS PatientCount,
ROUND(
(COUNT(users.id) / t.totalPatientCount) * 100) AS Percantage")
  .joins("JOIN organization_users ou on ou.user_id = users.id")
  .joins("JOIN roles_users ru on ru.user_id = users.id")
  .joins("CROSS JOIN (SELECT COUNT(*) AS totalPatientCount 
 FROM users,roles_users  WHERE users.ethinicity is not null and users.organization_id ="+org_id.to_s+" AND users.id=roles_users.user_id 
 AND roles_users.role_id=1 and users.demo = 0
 AND DATE(users.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"') t"
  )
 .joins("LEFT JOIN ethnicities et ON users.`ethinicity`=et.id")
.where("users.ethinicity is not null and ru.role_id=? AND ou.organization_id = ?  AND  DATE(users.created_at)
       BETWEEN ? and ? and users.demo = 0",role,org_id,start_date,end_date).group("et.ethinicity")
  
end

def self.out_patient_demographic(role,org_id,procedure,start_date,end_date)
  User.select("et.ethinicity as etn_label,COUNT(users.id) AS PatientCount,
ROUND(
(COUNT(users.id) / t.totalPatientCount) * 100) AS Percantage")
  .joins("JOIN organization_users ou on ou.user_id = users.id")
  .joins("JOIN roles_users ru on ru.user_id = users.id")
  .joins("CROSS JOIN (SELECT COUNT(*) AS totalPatientCount 
 FROM users,roles_users  WHERE users.ethinicity is not null and users.organization_id ="+org_id.to_s+" AND users.id=roles_users.user_id 
 AND roles_users.role_id=1 and users.demo = 0
 AND DATE(users.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"') t"
  )
 .joins("LEFT JOIN ethnicities et ON users.`ethinicity`=et.id")
.where("users.ethinicity is not null and ru.role_id=? AND ou.organization_id = ? and ou.organization_id = 100 AND  DATE(users.created_at)
       BETWEEN ? and ? and users.demo = 0",role,org_id,start_date,end_date).group("et.ethinicity")
  
end

def self.patient_by_location(role,org_id,procedure,start_date,end_date)
  
  UserConsumptionLog.select("DISTINCT `latitude`,`longitude`")
  .joins("JOIN login_histories lh ON lh.id=user_consumption_logs.login_id")
  .joins("JOIN users u ON u.id= lh.user_id")
  .joins("JOIN roles_users ru ON ru.user_id=u.id")
  .joins("JOIN organization_users ou on ou.user_id = u.id")  
.where("ru.role_id=? AND ou.organization_id = ?  AND  DATE(user_consumption_logs.created_at)
       BETWEEN ? and ? and u.demo = 0",role,org_id,start_date,end_date).limit(50)
  
end

def self.top_patient_location(role,org_id,procedure,start_date,end_date)
  LoginHistory.select("city,state, count(login_histories.user_id) as user_count")
  .joins("JOIN organization_users ou on ou.user_id = login_histories.user_id") 
  .joins("JOIN roles_users ru ON ru.user_id=login_histories.user_id")
  .joins("JOIN users on users.id = login_histories.user_id ")
 .where("login_histories.city is not null and login_histories.state is not null and ru.role_id=? AND ou.organization_id = ?  AND  DATE(login_histories.updated_at)
       BETWEEN ? and ? and users.demo = 0",role,org_id,start_date,end_date)
       .group("city, state").order("count(login_histories.user_id) desc").limit(10)
end

def self.fetch_top_videos_watched(role,org_id,procedure,start_date,end_date)
  
  UserContentConsume.select("COUNT(distinct ucl.id) AS totalViews ,COUNT(DISTINCT user_content_consumes.user_id) AS uniquewatch, ct.name as vid_name")
  .joins("JOIN users u ON u.id= user_content_consumes.user_id")
  .joins("JOIN contents ct ON ct.id=user_content_consumes.content_id")
  .joins("JOIN user_consumption_logs ucl on ucl.ucc_id = user_content_consumes.id")  
  .joins("JOIN roles_users ru ON ru.user_id=u.id")
  .joins("JOIN organization_users ou on ou.user_id = u.id")  
.where("ru.role_id=? AND ou.organization_id = ?  AND user_content_consumes.percent_consumed = 100 and DATE(user_content_consumes.updated_at)
       BETWEEN ? and ? and u.demo = 0",role,org_id,start_date,end_date).group("ct.id").order("COUNT(ucl.id) DESC").limit(5)
  
end

def self.fetch_least_videos_watched(role,org_id,procedure,start_date,end_date)
  
  UserContentConsume.select("COUNT(distinct ucl.id) AS totalViews ,COUNT(DISTINCT user_content_consumes.user_id) AS uniquewatch, ct.name as vid_name")
  .joins("JOIN users u ON u.id= user_content_consumes.user_id")
  .joins("JOIN contents ct ON ct.id=user_content_consumes.content_id")
  .joins("JOIN user_consumption_logs ucl on ucl.ucc_id = user_content_consumes.id")  
  .joins("JOIN roles_users ru ON ru.user_id=u.id")
  .joins("JOIN organization_users ou on ou.user_id = u.id")  
.where("ru.role_id=? AND ou.organization_id = ?  AND user_content_consumes.percent_consumed = 100 and DATE(user_content_consumes.updated_at)
       BETWEEN ? and ? and u.demo = 0",role,org_id,start_date,end_date).group("ct.id").order("COUNT(ucl.id) ASC").limit(5)
  
end

def self.fetch_top_videos_watched_demograpic(role,org_id,procedure,start_date,end_date)
  
  Ethnicity.select("t.name as vid_name,t.ethinicity as etn_name ,MAX(ct) as vid_count")
  .joins("cross join (SELECT  e.id,c.name,e.`ethinicity`,COUNT(ucl.id) AS ct    
    FROM user_content_consumes ucc,users u ,roles_users ru,contents c,user_consumption_logs ucl,`ethnicities` e ,organization_users ou
    WHERE ucc.percent_consumed = 100 and u.id=ucc.user_id AND ru.user_id = u.id AND ru.role_id =1 AND c.id=ucc.content_id AND c.content_type='ooyala' 
    AND ucl.ucc_id = ucc.id AND u.organization_id = "+org_id.to_s+" AND e.id=u.`ethinicity` and ou.user_id = u.id 
     AND DATE(ucc.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
    GROUP BY c.id,u.ethinicity ORDER BY COUNT(ucl.id) DESC ) t on t.id = ethnicities.id")
    .group("t.ethinicity")
 
end


def self.top_videos_watched_gender(role,org_id,gender,start_date,end_date)
  
  UserContentConsume.select("ct.name as vid_name,AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') AS gender,COUNT(ucl.id) AS ct")
  .joins("JOIN users u ON u.id= user_content_consumes.user_id")
  .joins("JOIN contents ct ON ct.id=user_content_consumes.content_id")
  .joins("JOIN user_consumption_logs ucl on ucl.ucc_id = user_content_consumes.id")  
  .joins("JOIN roles_users ru ON ru.user_id=u.id")
  .joins("JOIN organization_users ou on ou.user_id = u.id")  
.where("ru.role_id=? AND ou.organization_id = ? and user_content_consumes.percent_consumed = 100 AND AES_DECRYPT(UNHEX(u.gender), 'Mytonomy') = '"+gender.to_s+"' AND user_content_consumes.percent_consumed = 100 and DATE(user_content_consumes.updated_at)
       BETWEEN ? and ? and u.demo = 0",role,org_id,start_date,end_date).group("ct.name,u.gender").order("COUNT(ucl.id) DESC").limit(3)
  
end


def self.fetch_top_videos_watched_age(role,org_id,gender,start_date,end_date)
  
  ActiveRecord::Base.connection.execute("SELECT  tbl.age,tbl.name,
  CASE
    WHEN age >=10 AND age <19 THEN '10-19'
    WHEN age >=20 AND age <29 THEN '20-29'
    WHEN age >=30 AND age <39 THEN '30-39'
    WHEN age >=40 AND age <49 THEN '40-49'
    WHEN age >=50 AND age <59 THEN '50-59'
    WHEN age >=60 AND age <69 THEN '60-69'
    WHEN age >=70 AND age <79 THEN '70-79' 
  END AS ageband
FROM    
    (SELECT COUNT(ucc.id) AS ct ,c.name,DATE_FORMAT(FROM_DAYS( DATEDIFF( CURRENT_DATE, DATE(AES_DECRYPT(UNHEX(u.dob), 'Mytonomy')))
        ),
        '%y'
    ) AS age
    
    FROM user_content_consumes ucc,users u ,roles_users ru,contents c ,organization_users ou
    WHERE ou.user_id = ucc.user_id and u.id=ucc.user_id AND ucc.percent_consumed = 100 and ru.user_id = u.id AND ru.role_id =1 AND c.id=ucc.content_id AND c.content_type='ooyala'
    AND ou.organization_id = "+org_id.to_s+" AND 
    DATE(ucc.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
    GROUP BY c.id,DATE_FORMAT(FROM_DAYS( DATEDIFF( CURRENT_DATE, DATE(AES_DECRYPT(UNHEX(u.dob), 'Mytonomy')))
        ),
        '%y'
    )  ORDER BY COUNT(ucc.id) DESC )  AS tbl
GROUP BY ageband ORDER BY ageband ASC LIMIT 5;")
end

def self.fetch_top_videos_watched_all(role,org_id,gender,start_date,end_date)
  
  ActiveRecord::Base.connection.execute("SELECT 
  COUNT(t.user_id) AS patientCount,
  CASE
    WHEN content >=1 AND content <=10 THEN '1-10'
    WHEN content >=11 AND content <= 20 THEN '11-20'
    WHEN content >=21 AND content <=30 THEN '21-30'
    WHEN content >=31 AND content <= 40 THEN '31-40'
    WHEN content >=41 AND content <=50 THEN '41-50'
    WHEN content >=51  THEN '51+'
    END AS NumberofVideoRange
FROM    
(SELECT COUNT(ucc.content_id)AS content,ucc.user_id
FROM user_content_consumes ucc ,users u,roles_users ru ,organization_users ou WHERE ucc.percent_consumed = 100 and  ou.user_id = ucc.user_id and u.id=ucc.user_id AND ru.user_id=u.id AND ru.role_id=1 AND  ou.organization_id = "+org_id.to_s+"
AND DATE(ucc.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY ucc.user_id)t
GROUP BY NumberofVideoRange;")
end

def self.fetch_prior_pre_videos_watched_all(role,org_id,gender,start_date,end_date)
  
  ActiveRecord::Base.connection.execute("SELECT 
  COUNT(t.user_id) AS patientCount,
  CASE
    WHEN content >=1 AND content <=10 THEN '1-10'
    WHEN content >=11 AND content <= 20 THEN '11-20'
    WHEN content >=21 AND content <=30 THEN '21-30'
    WHEN content >=31 AND content <= 40 THEN '31-40'
    WHEN content >=41 AND content <=50 THEN '41-50'
    WHEN content >=51  THEN '51+'
    END AS NumberofVideoRange
FROM    
(SELECT COUNT(ucc.content_id)AS content,ucc.user_id
FROM user_content_consumes ucc ,users u,roles_users ru ,organization_users ou, health_conditions_users hcu
WHERE ucc.percent_consumed = 100 and  ou.user_id = ucc.user_id and u.id=ucc.user_id AND ru.user_id=u.id and hcu.user_id = u.id and hcu.procedure_date is not null
AND DATE(ucc.updated_at)  < DATE(hcu.procedure_date) AND ru.role_id=1 AND  ou.organization_id = "+org_id.to_s+"
AND DATE(ucc.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY ucc.user_id)t
GROUP BY NumberofVideoRange;")
  
end

def self.fetch_prior_on_videos_watched_all(role,org_id,gender,start_date,end_date)
  
  ActiveRecord::Base.connection.execute("SELECT 
  COUNT(t.user_id) AS PatientCount,
  CASE
    WHEN content >=1 AND content <=10 THEN '1-10'
    WHEN content >=11 AND content <= 20 THEN '11-20'
    WHEN content >=21 AND content <=30 THEN '21-30'
    WHEN content >=31 AND content <= 40 THEN '31-40'
    WHEN content >=41 AND content <=50 THEN '41-50'
    WHEN content >=51  THEN '51+'
    END AS NumberofVideoRange
FROM    
(SELECT COUNT(ucc.content_id)AS content,ucc.user_id
FROM user_content_consumes ucc ,users u,roles_users ru ,organization_users ou, health_conditions_users hcu
WHERE ucc.percent_consumed = 100 and  ou.user_id = ucc.user_id and u.id=ucc.user_id AND ru.user_id=u.id and hcu.user_id = u.id and hcu.procedure_date is not null
AND DATE(ucc.updated_at)  = DATE(hcu.procedure_date) AND ru.role_id=1 AND  ou.organization_id = "+org_id.to_s+"
AND DATE(ucc.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY ucc.user_id)t
GROUP BY NumberofVideoRange;")
  
end

def self.fetch_prior_no_pro_videos_watched_all(role,org_id,gender,start_date,end_date)
  
  ActiveRecord::Base.connection.execute("SELECT 
  COUNT(t.user_id) AS PatientCount,
  CASE
    WHEN content >=1 AND content <=10 THEN '1-10'
    WHEN content >=11 AND content <= 20 THEN '11-20'
    WHEN content >=21 AND content <=30 THEN '21-30'
    WHEN content >=31 AND content <= 40 THEN '31-40'
    WHEN content >=41 AND content <=50 THEN '41-50'
    WHEN content >=51  THEN '51+'
    END AS NumberofVideoRange
FROM    
(SELECT COUNT(ucc.content_id)AS content,ucc.user_id
FROM user_content_consumes ucc ,users u,roles_users ru ,organization_users ou, health_conditions_users hcu
WHERE ucc.percent_consumed = 100 and  ou.user_id = ucc.user_id and u.id=ucc.user_id AND ru.user_id=u.id and hcu.user_id = u.id and hcu.procedure_date is NULL
 AND ru.role_id=1 AND  ou.organization_id = "+org_id.to_s+"
AND DATE(ucc.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY ucc.user_id)t
GROUP BY NumberofVideoRange;")
  
end


def self.fetch_prior_after_videos_watched_all(role,org_id,gender,start_date,end_date)
  
  ActiveRecord::Base.connection.execute("SELECT 
  COUNT(t.user_id) AS patientCount,
  CASE
    WHEN content >=1 AND content <=10 THEN '1-10'
    WHEN content >=11 AND content <= 20 THEN '11-20'
    WHEN content >=21 AND content <=30 THEN '21-30'
    WHEN content >=31 AND content <= 40 THEN '31-40'
    WHEN content >=41 AND content <=50 THEN '41-50'
    WHEN content >=51  THEN '51+'
    END AS NumberofVideoRange
FROM    
(SELECT COUNT(ucc.content_id)AS content,ucc.user_id
FROM user_content_consumes ucc ,users u,roles_users ru ,organization_users ou, health_conditions_users hcu
WHERE ucc.percent_consumed = 100 and  ou.user_id = ucc.user_id and u.id=ucc.user_id AND ru.user_id=u.id and hcu.user_id = u.id and hcu.procedure_date is not null
AND DATE(ucc.updated_at)  > DATE(hcu.procedure_date) AND ru.role_id=1 AND  ou.organization_id = "+org_id.to_s+"
AND DATE(ucc.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0 
GROUP BY ucc.user_id)t
GROUP BY NumberofVideoRange;")
  
end

def self.fetch_total_videos_watched(role,org_id,gender,start_date,end_date)
  
  ActiveRecord::Base.connection.execute("SELECT 
  COUNT(t.user_id) AS patientCount,
  CASE
    WHEN content >=1 AND content <=10 THEN '1-10'
    WHEN content >=11 AND content <= 20 THEN '11-20'
    WHEN content >=21 AND content <=30 THEN '21-30'
    WHEN content >=31 AND content <= 40 THEN '31-40'
    WHEN content >=41 AND content <=50 THEN '41-50'
    WHEN content >=51  THEN '51+'
    END AS NumberofVideoRange
FROM    
(SELECT COUNT(ucc.content_id)AS content,ucc.user_id
FROM user_content_consumes ucc ,users u,roles_users ru ,organization_users ou
WHERE ucc.percent_consumed = 100 and  ou.user_id = ucc.user_id and u.id=ucc.user_id AND ru.user_id=u.id 
AND ru.role_id=1 AND  ou.organization_id = "+org_id.to_s+"
AND DATE(ucc.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0 
GROUP BY ucc.user_id)t
GROUP BY NumberofVideoRange;")
  
end




def self.fetch_patient_view_by_gender(role,org_id,gender,start_date,end_date)
  
  ActiveRecord::Base.connection.execute("SELECT 
  AES_DECRYPT(UNHEX(t.gender), 'Mytonomy') ,
   CASE
    WHEN AVG(t.content) >=1 AND AVG(t.content) <=10 THEN '1-10'
    WHEN AVG(t.content) >=11 AND AVG(t.content) <= 20 THEN '11-20'
    WHEN AVG(t.content) >=21 AND AVG(t.content) <=30 THEN '21-30'
    WHEN AVG(t.content) >=31 AND AVG(t.content) <= 40 THEN '31-40'
    WHEN AVG(t.content) >=41 AND AVG(t.content) <=50 THEN '41-50'
    WHEN AVG(t.content) >=51  THEN '51+'
    END AS NumberofVideoRange
FROM    
(SELECT COUNT(ucc.content_id) AS content,u.id,u.gender
FROM user_content_consumes ucc ,users u,roles_users ru,organization_users ou WHERE ucc.percent_consumed = 100 and ou.user_id = ucc.user_id and u.id=ucc.user_id AND ru.user_id=u.id AND ru.role_id=1 AND ou.organization_id = "+org_id.to_s+"
 AND DATE(ucc.updated_at)BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY u.id )t
GROUP BY gender
UNION 
SELECT 
  'All Patients',
   CASE
    WHEN AVG(t.content) >=1 AND AVG(t.content) <=10 THEN '1-10'
    WHEN AVG(t.content) >=11 AND AVG(t.content) <= 20 THEN '11-20'
    WHEN AVG(t.content) >=21 AND AVG(t.content) <=30 THEN '21-30'
    WHEN AVG(t.content) >=31 AND AVG(t.content) <= 40 THEN '31-40'
    WHEN AVG(t.content) >=41 AND AVG(t.content) <=50 THEN '41-50'
    WHEN AVG(t.content) >=51  THEN '51+'
    END AS NumberofVideoRange
FROM    
(SELECT COUNT(ucc.content_id) AS content,u.id,u.gender
FROM user_content_consumes ucc ,users u,roles_users ru ,organization_users ou WHERE ou.user_id = ucc.user_id and u.id=ucc.user_id AND ru.user_id=u.id AND ru.role_id=1 AND ou.organization_id = "+org_id.to_s+"
 AND DATE(ucc.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY u.id )t;")
end

def self.fetch_patient_view_by_demo(role,org_id,gender,start_date,end_date)
  
  ActiveRecord::Base.connection.execute("SELECT 
  t.`ethinicity` ,
   CASE
    WHEN AVG(t.content) >0 AND AVG(t.content) <=10 THEN '1-10'
    WHEN AVG(t.content) >10 AND AVG(t.content) <= 20 THEN '11-20'
    WHEN AVG(t.content) >20 AND AVG(t.content) <=30 THEN '21-30'
    WHEN AVG(t.content) >30 AND AVG(t.content) <= 40 THEN '31-40'
    WHEN AVG(t.content) >40 AND AVG(t.content) <=50 THEN '41-50'
    WHEN AVG(t.content) >50  THEN '51+'
    END AS NumberofVideoRange
FROM    
(SELECT COUNT(ucc.content_id) AS content,u.id,e.`ethinicity`
FROM user_content_consumes ucc ,users u,roles_users ru,`ethnicities` e,organization_users ou WHERE ucc.percent_consumed = 100 and ou.user_id = ucc.user_id and u.id=ucc.user_id AND ru.user_id=u.id AND u.`ethinicity`=e.id AND ru.role_id=1 AND ou.organization_id = "+org_id.to_s+"
 AND DATE(ucc.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY u.id )t
GROUP BY ethinicity;
")
end






def self.fetch_sumary_videos_watched_present(role,org_id,gender,start_date,end_date)
  puts "summary watched"
  
  ActiveRecord::Base.connection.execute("select avg(t.vid_count) as avg_count from ( SELECT count(DISTINCT user_content_consumes.content_id) as vid_count,user_content_consumes.user_id
 FROM `user_content_consumes` JOIN users u ON u.id= user_content_consumes.user_id 
 JOIN roles_users ru ON ru.user_id=u.id JOIN organization_users ou on ou.user_id = u.id 
 WHERE (ru.role_id= "+role.to_s+" AND ou.organization_id = "+org_id.to_s+" and  user_content_consumes.percent_consumed = 100 and u.demo = 0)
 group by user_content_consumes.user_id)t")
  
end



def self.fetch_sumary_videos_watched(role,org_id,gender,start_date,end_date)
  
   ActiveRecord::Base.connection.execute("select avg(t.vid_count) as avg_count from ( SELECT count(DISTINCT user_content_consumes.content_id) as vid_count,user_content_consumes.user_id
 FROM `user_content_consumes` JOIN users u ON u.id= user_content_consumes.user_id 
 JOIN roles_users ru ON ru.user_id=u.id JOIN organization_users ou on ou.user_id = u.id 
 WHERE (ru.role_id= "+role.to_s+" AND ou.organization_id = "+org_id.to_s+" and DATE(user_content_consumes.updated_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'and user_content_consumes.percent_consumed = 100 and u.demo = 0)
 group by user_content_consumes.user_id)t")
  
 
end



def self.fetch_pam_pre_survey_data(role,org_id,gender,start_date,end_date)
  
  ActiveRecord::Base.connection.execute("SELECT IFNULL(ROUND(AVG(avgIndividual),2),0) AS pampreSurveyData  
FROM 
(SELECT sua.user_id,SUM(a.ans_value),COUNT(sua.question_id), (SUM(a.ans_value)/COUNT(sua.question_id)) AS avgIndividual
FROM `survey_user_answers` sua 
JOIN answers a ON a.id= sua.`answer_id`
JOIN organization_users ou on ou.user_id = sua.user_id
JOIN users on users.id = sua.user_id 
JOIN roles_users ru ON ru.user_id=users.id 
WHERE ru.role_id = "+ role.to_s+" and (a.ans_value IS NOT NULL AND a.ans_value != 5) AND ou.organization_id = "+org_id.to_s+"
AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and users.demo = 0
 AND  sua.question_id IN (SELECT q.id FROM questions q WHERE q.`survey_section_id` IN (SELECT id FROM `survey_section_titles` WHERE survey_id=3 AND is_black_and_white=1  ) )
  GROUP BY user_id) t;").first
end


def self.fetch_pam_post_survey_data(role,org_id,gender,start_date,end_date)
  
  ActiveRecord::Base.connection.execute("SELECT IFNULL(ROUND(AVG(avgIndividual),2),0) AS pampreSurveyData  
FROM 
(SELECT sua.user_id,SUM(a.ans_value),COUNT(sua.question_id), (SUM(a.ans_value)/COUNT(sua.question_id)) AS avgIndividual
FROM `survey_user_answers` sua 
JOIN answers a ON a.id= sua.`answer_id`
JOIN organization_users ou on ou.user_id = sua.user_id
JOIN users on users.id = sua.user_id
JOIN roles_users ru ON ru.user_id=users.id 
WHERE ru.role_id= "+ role.to_s+" AND (a.ans_value IS NOT NULL AND a.ans_value != 5) AND ou.organization_id = "+org_id.to_s+"
AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and users.demo = 0
 AND  sua.question_id IN (SELECT q.id FROM questions q WHERE q.`survey_section_id` IN (SELECT id FROM `survey_section_titles` WHERE survey_id=4 AND is_black_and_white=1  ) )
  GROUP BY user_id) t;").first
end

def self.fetch_usability_satisfaction_understanding(role,org_id,gender,start_date,end_date)
  
  ActiveRecord::Base.connection.execute("SELECT ROUND(AVG(t.individualPer),2) AS rating,t.name 
  FROM
  (SELECT COUNT(sua.id),ss.name,SUM(a.ans_value), (SUM(a.ans_value)/COUNT(sua.id)) AS individualPer 
  FROM survey_user_answers sua
  JOIN answers a ON a.id= sua.`answer_id`
  JOIN roles_users ru ON ru.user_id = sua.user_id
  JOIN organization_users ou on ou.user_id = sua.user_id
  JOIN questions que ON que.id=sua.question_id
  JOIN users on users.id = sua.user_id
JOIN section_subtitles ss ON ss.id=que.`section_subtitle_id`
  WHERE  ru.role_id = "+role.to_s+" and que.survey_section_id IN (15,17) and ou.organization_id = "+org_id.to_s+"
  AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and users.demo = 0
  GROUP BY ss.name,sua.user_id) t   
  GROUP BY t.name ;")
end

def self.fetch_taking_insulin_pre_survey_responses(role,org_id,gender,start_date,end_date)
  Answer.select("IFNULL(t.answerCount,0) as ans_count,answers.answer as ans_label")
  .joins("LEFT JOIN (SELECT  COUNT(sua.id) AS answerCount,a.answer,sua.answer_id
FROM `survey_user_answers` sua 
JOIN answers a ON a.id = sua.answer_id
JOIN users u ON u.id=sua.user_id
JOIN roles_users ru ON ru.user_id=u.id
JOIN organization_users ou on ou.user_id = sua.user_id
WHERE  ru.role_id = "+role.to_s+" and sua.question_id = 27 AND u.organization_id = "+org_id.to_s+" 
 AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY sua.answer_id)t ON t.answer_id = answers.id")
.where("answers.survey_section_id = 4")

end

def self.fetch_taking_insulin_post_survey_responses(role,org_id,gender,start_date,end_date)
  Answer.select("IFNULL(t.answerCount,0) as ans_count,answers.answer as ans_label")
  .joins("LEFT JOIN (SELECT  COUNT(sua.id) AS answerCount,a.answer,sua.answer_id
FROM `survey_user_answers` sua 
JOIN answers a ON a.id = sua.answer_id
JOIN users u ON u.id=sua.user_id
JOIN roles_users ru ON ru.user_id=u.id
JOIN organization_users ou on ou.user_id = sua.user_id
WHERE sua.question_id = 30 AND u.organization_id = "+org_id.to_s+" AND ru.role_id= "+role.to_s+"
 AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY sua.answer_id)t ON t.answer_id = answers.id")
.where("answers.survey_section_id = 16")

end

def self.diabetes_with_insulin_pre_survey_responses(role,org_id,gender,start_date,end_date)
  Answer.select("IFNULL(t.answerCount,0) as ans_count,answers.answer as ans_label")
  .joins("LEFT JOIN (SELECT  COUNT(sua.id) AS answerCount,a.answer,sua.answer_id
FROM `survey_user_answers` sua 
JOIN answers a ON a.id = sua.answer_id
JOIN users u ON u.id=sua.user_id
JOIN roles_users ru ON ru.user_id=u.id
JOIN organization_users ou on ou.user_id = sua.user_id
WHERE sua.question_id = 28 AND u.organization_id = "+org_id.to_s+" AND ru.role_id= "+role.to_s+"
 AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY sua.answer_id)t ON t.answer_id = answers.id")
.where("answers.survey_section_id = 4")

end



def self.diabetes_with_insulin_post_survey_responses(role,org_id,gender,start_date,end_date)
  Answer.select("IFNULL(t.answerCount,0) as ans_count,answers.answer as ans_label")
  .joins("LEFT JOIN (SELECT  COUNT(sua.id) AS answerCount,a.answer,sua.answer_id
FROM `survey_user_answers` sua 
JOIN answers a ON a.id = sua.answer_id
JOIN users u ON u.id=sua.user_id
JOIN roles_users ru ON ru.user_id=u.id
JOIN organization_users ou on ou.user_id = sua.user_id
WHERE sua.question_id = 31 AND u.organization_id = "+org_id.to_s+" AND ru.role_id= "+role.to_s+"
 AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY sua.answer_id)t ON t.answer_id = answers.id")
.where("answers.survey_section_id = 16")

end

def self.fetch_condition_worse_pre_survey_responses(role,org_id,gender,start_date,end_date)
  Answer.select("IFNULL(t.answerCount,0) as ans_count,answers.answer as ans_label")
  .joins("LEFT JOIN (SELECT  COUNT(sua.id) AS answerCount,a.answer,sua.answer_id
FROM `survey_user_answers` sua 
JOIN answers a ON a.id = sua.answer_id
JOIN users u ON u.id=sua.user_id
JOIN roles_users ru ON ru.user_id=u.id
JOIN organization_users ou on ou.user_id = sua.user_id
WHERE sua.question_id = 29 AND u.organization_id = "+org_id.to_s+" AND ru.role_id= "+ role.to_s+"
 AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY sua.answer_id)t ON t.answer_id = answers.id")
.where("answers.survey_section_id = 4")

end



def self.fetch_condition_worse_post_survey_responses(role,org_id,gender,start_date,end_date)
  Answer.select("IFNULL(t.answerCount,0) as ans_count,answers.answer as ans_label")
  .joins("LEFT JOIN (SELECT  COUNT(sua.id) AS answerCount,a.answer,sua.answer_id
FROM `survey_user_answers` sua 
JOIN answers a ON a.id = sua.answer_id
JOIN users u ON u.id=sua.user_id
JOIN roles_users ru ON ru.user_id=u.id
JOIN organization_users ou on ou.user_id = sua.user_id
WHERE sua.question_id = 32 AND u.organization_id = "+org_id.to_s+" AND ru.role_id= "+ role.to_s+"
 AND DATE(sua.created_at) BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' and u.demo = 0
GROUP BY sua.answer_id)t ON t.answer_id = answers.id")
.where("answers.survey_section_id = 16")

end


def self.all_survey_count(role,org_id,gender,start_date,end_date)
  SurveyUserAnswer.select("COUNT(DISTINCT survey_user_answers.user_id) as all_count")
  .joins("JOIN users u on u.id= survey_user_answers.user_id")
  .joins("JOIN roles_users ru ON ru.user_id=u.id")
  .joins("JOIN organization_users ou on ou.user_id = u.id")
  .where("ru.role_id=? AND u.organization_id = ? AND DATE(survey_user_answers.created_at) BETWEEN ? and ? and u.demo = 0",role,org_id,start_date,end_date)
  
end

  
 # new end
 

  #Fetching user count for analysis bar on dashboard
  def self.fetch_total_users_count(role_id,org_id,procedure,start_date,end_date)
   User.where("demo =0 AND roles_users.role_id = ? AND organization_users.organization_id = ? AND DATE(users.created_at) BETWEEN ? and ? ",role_id,org_id,start_date,end_date)
   .joins("join organization_users on users.id = organization_users.user_id")
   .joins("join roles_users on users.id = roles_users.user_id")
   .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
   .select("COUNT(*) as total_users,
           SUM(CASE WHEN CONVERT(AES_DECRYPT(UNHEX(gender), 'Mytonomy') USING latin1)='Male' THEN 1 ELSE 0 END)/COUNT(*)*100 AS male_users,
           SUM(CASE WHEN CONVERT(AES_DECRYPT(UNHEX(gender), 'Mytonomy') USING latin1)='Female' THEN 1 ELSE 0 END)/COUNT(*)*100 AS female_users,
            SUM(CASE WHEN CONVERT(AES_DECRYPT(UNHEX(gender), 'Mytonomy') USING latin1)='Male' THEN 1 ELSE 0 END) AS male_users_count,
            SUM(CASE WHEN CONVERT(AES_DECRYPT(UNHEX(gender), 'Mytonomy') USING latin1)='Female' THEN 1 ELSE 0 END) AS female_users_count,             
            SUM(CASE WHEN CONVERT(AES_DECRYPT(UNHEX(gender), 'Mytonomy') USING latin1)='Male' THEN TIMESTAMPDIFF(YEAR, AES_DECRYPT(UNHEX(dob),'Mytonomy'), CURDATE()) ELSE 0 END ) AS male_total_age,
           SUM(CASE WHEN CONVERT(AES_DECRYPT(UNHEX(gender), 'Mytonomy') USING latin1)='Female' THEN TIMESTAMPDIFF(YEAR, AES_DECRYPT(UNHEX(dob),'Mytonomy'), CURDATE()) ELSE 0 END ) AS female_total_age,
           AVG(TIMESTAMPDIFF(YEAR, AES_DECRYPT(UNHEX(dob),'Mytonomy'), CURDATE())) AS average_age,
           SUM(CASE WHEN DATE(users.created_at)=DATE(NOW())THEN 1 ELSE 0 END) AS new_users ").first
          
 end
  
  #Top Video Watched
  def self.fetch_top_video_watched_new_dashboard(org_id,procedure,start_date,end_date)
    puts 'Top Video Watched'
    UserContentConsume.where(" ru.role_id = "+ role.to_s+"users.demo = 0 "+procedure.to_s+" AND user_content_consumes.percent_consumed > 0 AND   
     organization_users.organization_id="+org_id.to_s + " AND contents.content_type='ooyala' AND TIMESTAMP(users.created_at) BETWEEN ? and ? ",start_date,end_date)
     .order('TIMES_VIEWED DESC')
     .limit(5)
     .joins("join users ON user_content_consumes.user_id=users.id")
     .joins("join roles_users on user_content_consumes.id = roles_users.user_id")
     .joins("join organization_users on users.id = organization_users.user_id")
     .joins("join contents ON user_content_consumes.content_id=contents.id")
     .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
     .joins("join login_histories on login_histories.id = user_consumption_logs.login_id")
     .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
     .select("count(*) TIMES_VIEWED, contents.name").group('user_content_consumes.content_id')           
  end
  
    #Top Video Watched by Demographic
  def self.fetch_top_video_watched_demographic_new_dashboard(org_id,procedure,ethnicity,start_date,end_date)
    puts 'Top Video Watched by Demographic'
    UserContentConsume.where("users.demo = 0 "+procedure.to_s+" AND user_content_consumes.percent_consumed > 0 AND   
     organization_users.organization_id="+org_id.to_s + " AND contents.content_type='ooyala' AND users.ethinicity = " + ethnicity.to_s + " AND TIMESTAMP(users.created_at) BETWEEN ? and ? ",start_date,end_date)
     .order('TIMES_VIEWED DESC')
     .limit(1)
     .joins("join users ON user_content_consumes.user_id = users.id")
     .joins("join organization_users on users.id = organization_users.user_id")
     .joins("join contents ON user_content_consumes.content_id=contents.id")
     .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
     .joins("join login_histories on login_histories.id = user_consumption_logs.login_id")
     .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
     .select("count(user_content_consumes.content_id) TIMES_VIEWED, contents.name,users.ethinicity")
     .group("user_content_consumes.content_id")           
  end
  
    #Top Video Watched by Gender
  def self.fetch_top_video_watched_gender_new_dashboard(gender,org_id,procedure,start_date,end_date)
    puts 'Top Video Watched by Gender'
    if(gender.blank?)
      gender = "Male"
    end
    UserContentConsume.where("users.demo = 0 "+procedure.to_s+
     " AND user_content_consumes.percent_consumed > 0 AND "+  
     "organization_users.organization_id="+org_id.to_s + " AND contents.content_type='ooyala' AND "+
     "CONVERT(AES_DECRYPT(UNHEX(users.gender), 'Mytonomy') USING latin1) ='" + gender + "' AND TIMESTAMP(users.created_at) BETWEEN ? and ? ",start_date,end_date)
     .order('TIMES_VIEWED DESC').limit(3)
     .joins("join users ON user_content_consumes.user_id=users.id")
     .joins("join organization_users on users.id = organization_users.user_id")
     .joins("join contents ON user_content_consumes.content_id=contents.id")
     .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
     .joins("join login_histories on login_histories.id = user_consumption_logs.login_id")
     .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
     .select("count(*) TIMES_VIEWED, contents.name, CONVERT(AES_DECRYPT(UNHEX(users.gender), 'Mytonomy') USING latin1) GENDER")
     .group('user_content_consumes.content_id,users.gender')           
  end
  
  #summary Videos Watched
  def self.fetch_summary_video_watched_new_dashboard(org_id,procedure,start_date,end_date)
    puts 'summary Videos Watched'
    UserContentConsume.where("users.demo = 0 "+procedure.to_s+" AND user_content_consumes.percent_consumed > 0 AND   
     organization_users.organization_id="+org_id.to_s + " AND contents.content_type='ooyala' AND TIMESTAMP(users.created_at) BETWEEN ? and ? ",start_date,end_date)
     .joins("join users ON user_content_consumes.user_id=users.id")
     .joins("join organization_users on users.id = organization_users.user_id")
     .joins("join contents ON user_content_consumes.content_id=contents.id")
     .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
     .joins("join login_histories on login_histories.id = user_consumption_logs.login_id")
     .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
     .select("count(distinct contents.id) TOTAL_VIDEOS_WATCHED, users.id, 
             sum(user_content_consumes.percent_consumed)/count(contents.id) AVERAGE_VIDEO_USER")
     .group("users.id")           
  end
  
  #Total # Videos Watched by All Patients
  def self.fetch_total_video_watched_all_patient_new_dashboard(org_id,procedure,start_count,end_count,start_date,end_date)
    puts 'Total # Videos Watched by All Patients'
     if end_count==0   
      UserContentConsume.where("users.demo = 0 "+procedure.to_s+" AND user_content_consumes.percent_consumed > 0 AND   
       organization_users.organization_id="+org_id.to_s + " AND contents.content_type='ooyala' AND TIMESTAMP(users.created_at) BETWEEN ? and ? ",start_date,end_date)
       .joins("join users ON user_content_consumes.user_id=users.id")
       .joins("join organization_users on users.id = organization_users.user_id")
       .joins("join contents ON user_content_consumes.content_id=contents.id")
       .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
       .joins("join login_histories on login_histories.id = user_consumption_logs.login_id")
       .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
       .select("count(distinct contents.id) TOTAL_VIDEOS_WATCHED, users.id")
       .group("users.id").having("TOTAL_VIDEOS_WATCHED > ?",start_count)
     else
       UserContentConsume.where("users.demo = 0 "+procedure.to_s+" AND user_content_consumes.percent_consumed > 0 AND   
       organization_users.organization_id="+org_id.to_s + " AND contents.content_type='ooyala' AND TIMESTAMP(users.created_at) BETWEEN ? and ? ",start_date,end_date)
       .joins("join users ON user_content_consumes.user_id=users.id")
       .joins("join organization_users on users.id = organization_users.user_id")
       .joins("join contents ON user_content_consumes.content_id=contents.id")
       .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
       .joins("join login_histories on login_histories.id = user_consumption_logs.login_id")
       .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
       .select("count(distinct contents.id) TOTAL_VIDEOS_WATCHED, users.id")
       .group("users.id").having("TOTAL_VIDEOS_WATCHED between ? AND ?",start_count,end_count)
     end            
  end
  
  
  #Total # Videos Watched by African Americans demography
  def self.fetch_total_video_watched_ethinicity_new_dashboard(org_id,procedure,start_count,end_count,ethinicity,start_date,end_date)
    puts 'Total # Videos Watched by All Patients'
     if end_count==0   
      UserContentConsume.where("users.demo = 0 "+procedure.to_s+" AND user_content_consumes.percent_consumed > 0 AND   
       organization_users.organization_id="+org_id.to_s + " AND contents.content_type='ooyala' AND users.ethinicity=" + ethinicity.to_s + " AND TIMESTAMP(users.created_at) BETWEEN ? and ? ",start_date,end_date)
       .joins("join users ON user_content_consumes.user_id=users.id")
       .joins("join organization_users on users.id = organization_users.user_id")
       .joins("join contents ON user_content_consumes.content_id=contents.id")
       .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
       .joins("join login_histories on login_histories.id = user_consumption_logs.login_id")
       .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
       .select("count(distinct contents.id) TOTAL_VIDEOS_WATCHED, users.id")
       .group("users.id").having("TOTAL_VIDEOS_WATCHED > ?",start_count)
     else
       UserContentConsume.where("users.demo = 0 "+procedure.to_s+" AND user_content_consumes.percent_consumed > 0 AND   
       organization_users.organization_id="+org_id.to_s + " AND contents.content_type='ooyala' AND users.ethinicity=" + ethinicity.to_s+ " AND TIMESTAMP(users.created_at) BETWEEN ? and ? ",start_date,end_date)
       .joins("join users ON user_content_consumes.user_id=users.id")
       .joins("join organization_users on users.id = organization_users.user_id")
       .joins("join contents ON user_content_consumes.content_id=contents.id")
       .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
       .joins("join login_histories on login_histories.id = user_consumption_logs.login_id")
       .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
       .select("count(distinct contents.id) TOTAL_VIDEOS_WATCHED, users.id")
       .group("users.id").having("TOTAL_VIDEOS_WATCHED between ? AND ?",start_count,end_count)
     end            
  end
  
  #Patient Gender by Percentage
  def self.fetch_total_users_count_percentage_new_dashboard(role,org_id,procedure)
    puts 'Patient Gender by Percentage'
    Role.find_by_name(role).users.where("demo =0 "+procedure.to_s+" AND organization_users.organization_id = ? ",org_id)
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .select("COUNT(*) as total_users,
            SUM(CASE WHEN CONVERT(AES_DECRYPT(UNHEX(gender), 'Mytonomy') USING latin1)='Male' THEN 1 ELSE 0 END)/COUNT(*)*100 AS male_users,
            SUM(CASE WHEN CONVERT(AES_DECRYPT(UNHEX(gender), 'Mytonomy') USING latin1)='Female' THEN 1 ELSE 0 END)/COUNT(*)*100 AS female_users,
            AVG(TIMESTAMPDIFF(YEAR, AES_DECRYPT(UNHEX(dob),'Mytonomy'), CURDATE())) AS average_age,
            SUM(CASE WHEN DATE(users.created_at)=DATE(NOW())THEN 1 ELSE 0 END) AS new_users ").first
           
  end
    
  #Average Patient Age
  def self.fetch_total_users_count_new_dashboard(role,org_id,procedure)
    puts 'Average Patient Age'
    Role.find_by_name(role).users.where("demo =0 "+procedure.to_s+" AND organization_users.organization_id = ? ",org_id)
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .select("COUNT(*) as total_users,
            SUM(CASE WHEN CONVERT(AES_DECRYPT(UNHEX(gender), 'Mytonomy') USING latin1)='Male' THEN 1 ELSE 0 END) AS male_users,
            SUM(CASE WHEN CONVERT(AES_DECRYPT(UNHEX(gender), 'Mytonomy') USING latin1)='Female' THEN 1 ELSE 0 END) AS female_users,
            AVG(TIMESTAMPDIFF(YEAR, AES_DECRYPT(UNHEX(dob),'Mytonomy'), CURDATE())) AS average_age,
            SUM(CASE WHEN DATE(users.created_at)=DATE(NOW())THEN 1 ELSE 0 END) AS new_users ").first
           
  end
  
  #Average Patient Age by Demography
  def self.fetch_average_age_by_demography(role_id,org_id,procedure,start_date,end_date)
    User.where("demo =0 AND roles_users.role_id = ? AND organization_users.organization_id = ? AND TIMESTAMP(users.created_at) BETWEEN ? and ? and users.demo = 0",role_id,org_id,start_date,end_date)
   .joins("join organization_users on users.id = organization_users.user_id")
   .joins("join roles_users on users.id = roles_users.user_id")
   .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
   .select("AES_DECRYPT(UNHEX(gender),'Mytonomy') AS Gender,TIMESTAMPDIFF(YEAR, AES_DECRYPT(UNHEX(dob),'Mytonomy'), CURDATE()) AS 'AGE', users.ethinicity AS 'DEMOGRAPHY'");
          
 end
  
  #Patient Demographic by Percentage
  def self.fetch_users_demographic_percentage_new_dashboard(role,org_id,procedure,start_date,end_date)
    puts 'Patient Demographic by Percentage'
    Role.find_by_name(role).users.where("demo =0 "+procedure.to_s+" AND organization_users.organization_id = ? AND TIMESTAMP(users.created_at) BETWEEN ? and ? ",org_id,start_date,end_date)
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .select("COUNT(*) as total_users, 
          SUM(CASE WHEN users.ethinicity ='1' THEN 1 ELSE 0 END)/COUNT(*)*100 AS 'White',
          SUM(CASE WHEN users.ethinicity ='2' THEN 1 ELSE 0 END)/COUNT(*)*100 AS 'AfricanAmericanBlack',
          SUM(CASE WHEN users.ethinicity ='3' THEN 1 ELSE 0 END)/COUNT(*)*100 AS 'HispanicLatino',
          SUM(CASE WHEN users.ethinicity ='4' THEN 1 ELSE 0 END)/COUNT(*)*100 AS 'SouthAsian',
          SUM(CASE WHEN (users.ethinicity ='5' OR users.ethinicity is null) THEN 1 ELSE 0 END)/COUNT(*)*100 AS 'Other'").first
           
  end
  #Patient Demographic
  def self.fetch_users_demographic_new_dashboard(role,org_id,procedure,start_date,end_date)
    puts 'Patient Demographic'
    Role.find_by_name(role).users.where("demo =0 "+procedure.to_s+" AND organization_users.organization_id = ? AND TIMESTAMP(users.created_at) BETWEEN ? and ? ",org_id,start_date,end_date)
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .select("COUNT(*) as total_users, 
          SUM(CASE WHEN users.ethinicity ='1' THEN 1 ELSE 0 END) AS 'White',
          SUM(CASE WHEN users.ethinicity ='2' THEN 1 ELSE 0 END) AS 'AfricanAmericanBlack',
          SUM(CASE WHEN users.ethinicity ='3' THEN 1 ELSE 0 END) AS 'HispanicLatino',
          SUM(CASE WHEN users.ethinicity ='4' THEN 1 ELSE 0 END) AS 'SouthAsian',
          SUM(CASE WHEN (users.ethinicity ='5' OR users.ethinicity is null) THEN 1 ELSE 0 END) AS 'Other'").first
           
  end
  #Patient Demographic by ethnicity_id
  def self.fetch_users_demographic_new_dashboard_by_ethni(role,org_id,procedure,ethnicity_query,start_date,end_date)
    puts 'Patient Demographic by ethnicity_id'
    Role.find_by_name(role).users.where("demo =0 "+procedure.to_s+" AND organization_users.organization_id = ? AND TIMESTAMP(users.created_at) BETWEEN ? and ? ",org_id,start_date,end_date)
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .select("COUNT(*) as total_users"+ethnicity_query).first
           
  end  
  #Patient Demographic Age
  def self.fetch_users_demographic_age_new_dashboard(role_id,org_id,procedure,ethnicity,start_date,end_date)
   puts 'Patient Demographic Age'
   User.where("demo =0 "+procedure.to_s + " AND roles_users.role_id = ?  AND users.ethinicity " + ethnicity.to_s +
   " AND organization_users.organization_id = ? AND TIMESTAMP(users.created_at) BETWEEN ? and ? and users.demo = 0",role_id,org_id,start_date,end_date)
   .joins("join organization_users on users.id = organization_users.user_id")
   .joins("join roles_users on users.id = roles_users.user_id")
   .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
   .select("TIMESTAMPDIFF(YEAR, AES_DECRYPT(UNHEX(dob),'Mytonomy'), CURDATE()) AS Age,users.id")
   .order("Age ASC")
            
 end 
  #Insulin Brand by Demographic
  def self.fetch_users_demographic_insulin_new_dashboard(role,org_id,procedure,insulin_brand,start_date,end_date)
    puts 'Insulin Brand by Demographic'
    Role.find_by_name(role).users.where("demo =0 " + procedure.to_s + " AND users.ethinicity is not null AND user_brands.brand_id=" + insulin_brand.to_s +
    " AND organization_users.organization_id = ? AND TIMESTAMP(users.created_at) BETWEEN ? and ? ",org_id,start_date,end_date)
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .joins("join user_brands on (users.id = user_brands.user_id and user_brands.deactivated = 0) ")
    .select("count(distinct users.id) PATIENT_COUNT,users.ethinicity")
    .group("users.ethinicity")
    .order("users.ethinicity ASC")
             
  end 
  #Insurance Type Summary
  def self.fetch_users_insurance_new_dashboard(role,org_id,procedure,start_date,end_date)
    puts 'Insurance Type Summary'
    Role.find_by_name(role).users.where("demo =0 "+procedure.to_s+" AND organization_users.organization_id = ? AND TIMESTAMP(users.created_at) BETWEEN ? and ? ",org_id,start_date,end_date)
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .select("COUNT(*) as total_users, 
          SUM(CASE WHEN users.insurance ='1' THEN 1 ELSE 0 END) AS 'Commercial',
          SUM(CASE WHEN users.insurance ='2' THEN 1 ELSE 0 END) AS 'Medicare',
          SUM(CASE WHEN users.insurance ='3' THEN 1 ELSE 0 END) AS 'Medicaid',
          SUM(CASE WHEN (users.insurance ='4' OR users.insurance is null) THEN 1 ELSE 0 END) AS 'Other'").first
           
  end 
  ##Fetching watched video count for analysis bar on dashboard 
  def self.fetch_content_watched(org_id,procedure)
    UserContentConsume.where("users.demo = 0 AND user_content_consumes.percent_consumed = 100 AND user_content_consumes.content_id IS NOT NULL AND organization_users.organization_id = ?"+procedure.to_s,org_id)
    .joins("join users on users.id=user_content_consumes.user_id")
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("join health_conditions_users on user_content_consumes.user_id = health_conditions_users.user_id")
    .select("SUM(IF (user_content_consumes.times_consumed=0,1, times_consumed))  AS 'Total_Count_of_Content_Watched'").first
    
  end
  #Fetching patient invied and logged in count
  def self.fetch_patient_invited_and_logged_in(start_date,end_date,duration,org_id,dur,procedure)
    if(dur == Constants::CUSTOM)
            

      Role.find_by_name(Constants::ROLE_PATIENT).users.where("users.demo = 0  and organization_users.organization_id = ? and DATE(users.created_at) BETWEEN ? and ?"+procedure.to_s,org_id,start_date,end_date)
      .joins("join organization_users on users.id = organization_users.user_id")
      .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .select("SUM(CASE WHEN DATE(users.created_at) BETWEEN '"+ start_date+"' and '"+ end_date+"' THEN 1 ELSE 0 END) AS invited,
             SUM(CASE WHEN DATE(users.created_at) BETWEEN '"+ start_date+"' and '"+ end_date+"' and users.last_sign_in_at is not null THEN 1 ELSE 0 END) AS logged_in
             ")
             
     else
       
      Role.find_by_name(Constants::ROLE_PATIENT).users.where("users.demo = 0 and organization_users.organization_id = ? and DATE(users.created_at) BETWEEN ? and ?"+procedure.to_s,org_id,start_date,end_date).order('users.created_at ASC')
      .joins("join organization_users on users.id = organization_users.user_id")
      .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .select(duration+"(users.created_at) as label,MONTHNAME(users.created_at)AS month,
             SUM(CASE WHEN DATE(users.created_at) BETWEEN '"+ start_date+"' and '"+ end_date+"' THEN 1 ELSE 0 END) AS invited,
             SUM(CASE WHEN DATE(users.created_at) BETWEEN '"+ start_date+"' and '"+ end_date+"' and users.last_sign_in_at is not null THEN 1 ELSE 0 END) AS logged_in
             ").group('label')
    end
    
  end
  
  
  #Fetching all time patient invited count 
  def self.fetch_count_all_patient_invited(org_id,procedure)
     Role.find_by_name(Constants::ROLE_PATIENT).users.where("users.demo = 0  and organization_users.organization_id = ? "+procedure.to_s,org_id)
      .joins("join organization_users on users.id = organization_users.user_id")
      .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .select("COUNT(*) AS invited")
     
  end
  
  #Fetching all time patient watched count
  def self.fetch_all_watched_count_patient(org_id,procedure)
     UserContentConsume
     .joins("join users on users.id=user_content_consumes.user_id")
     .joins("join organization_users on users.id = organization_users.user_id")
     .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
     .where("users.demo = 0 AND user_content_consumes.percent_consumed = 100 AND 
    user_content_consumes.content_id IS NOT NULL AND organization_users.organization_id = ?
    "+procedure.to_s,org_id)
    .select("COUNT(DISTINCT users.id) as watched")
  end
  
  
  #Fetching ease_of_use survey data
  def self.fetch_survey_data_by_question(org_id,que_id)
    puts 'Fetching ease_of_use survey data'
     SurveyUserAnswer
     .joins("left join answers on answers.id = survey_user_answers.answer_id")
     .joins("left join organization_users on organization_users.user_id = survey_user_answers.user_id")
     .where("question_id in (?) AND organization_users.organization_id = ?",que_id,org_id)
     .select("COUNT(*) as total_count, SUM(answers.ans_value) as answer_value_total")
   
  end
  
  
  
  #Fetching patient_invited and patient_watched by days for week
  def self.fetch_patient_invited_patient_watched_by_days(start_date,end_date,org_id,procedure)
    puts 'Fetching patient_invited and patient_watched by days'
    User
    .joins("left join user_content_consumes on users.id=user_content_consumes.user_id ")
    .joins("join organization_users on users.id = organization_users.user_id ")
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id ")
    .where("users.demo = 0  AND organization_users.organization_id = ?
            AND DATE(users.created_at) BETWEEN ? and ? "+ procedure.to_s,org_id,start_date,end_date)
    .select("SUM(CASE WHEN user_content_consumes.percent_consumed > 0 AND 
          user_content_consumes.content_id IS NOT NULL THEN 1 ELSE 0 END) as watched,
          COUNT(DISTINCT users.id) as invited,
          DATE_FORMAT(users.created_at,'%m-%d') as label")
    .group("DAY(users.created_at)")
  end
  
  def self.fetch_patient_invited_patient_watched_by_Period(start_date,end_date,org_id,procedure,label)
    puts 'Fetching patient_invited and patient_watched by days period'
    User
    .joins("left join user_content_consumes on users.id=user_content_consumes.user_id ")
    .joins("join organization_users on users.id = organization_users.user_id ")
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id ")
    .where("users.demo = 0  AND organization_users.organization_id = ?
            AND DATE(users.created_at) BETWEEN ? and ? "+ procedure.to_s,org_id,start_date,end_date)
    .select("SUM(CASE WHEN user_content_consumes.percent_consumed > 0 AND 
          user_content_consumes.content_id IS NOT NULL THEN 1 ELSE 0 END) as watched,
          COUNT(DISTINCT users.id) as invited,\""+ label +"\" AS label")
  end
    
  #Fetching patient watched count
  def self.engagement_video_watched(start_date,end_date,duration,org_id,dur,procedure)
    if (dur == Constants::CUSTOM)
       
    UserContentConsume
     .joins("join users on users.id=user_content_consumes.user_id")
     .joins("join organization_users on users.id = organization_users.user_id")
     .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
     .where("users.demo = 0 AND user_content_consumes.percent_consumed = 100 AND 
    user_content_consumes.content_id IS NOT NULL AND organization_users.organization_id = ?
     and DATE(users.created_at) BETWEEN ? and ?"+procedure.to_s,org_id,start_date,end_date)
    .select("COUNT(DISTINCT users.id) as watched")
    else
      UserContentConsume
     .joins("join users on users.id=user_content_consumes.user_id")
     .joins("join organization_users on users.id = organization_users.user_id")
     .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
     .where("users.demo = 0 AND user_content_consumes.percent_consumed = 100 AND 
    user_content_consumes.content_id IS NOT NULL AND organization_users.organization_id = ?
     and DATE(users.created_at) BETWEEN ? and ?"+procedure.to_s,org_id,start_date,end_date)
    .select(duration+"(users.created_at) as label,COUNT(DISTINCT users.id) as watched").group('label')
    end
  end
  #Fetching patient list who watched video 
 def self.at_least_one_video_watched(start_time,end_time,org_id,procedure)
   
   UserContentConsume.where("users.demo = 0 AND user_content_consumes.percent_consumed = 100 AND content_id IS NOT NULL AND 
   organization_users.organization_id =?  AND TIMESTAMP(user_content_consumes.updated_at) BETWEEN ? and ? "+procedure.to_s,org_id,start_time,end_time).limit(5)
   .joins("join users  ON users.id = user_content_consumes.user_id")
   .joins("join organization_users on users.id = organization_users.user_id")
   .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
   .select("users.id,AES_DECRYPT(UNHEX(users.first_name), 'Mytonomy') AS 'first_name',
AES_DECRYPT(UNHEX(users.last_name), 'Mytonomy') AS 'last_name',
AES_DECRYPT(UNHEX(users.gender), 'Mytonomy') AS 'Gender',
AES_DECRYPT(UNHEX(users.dob), 'Mytonomy') AS 'dob',
TIMESTAMPDIFF(YEAR, AES_DECRYPT(UNHEX(users.dob),'Mytonomy'), CURDATE()) AS 'Age',
users.email,COUNT(users.id) AS Unique_Videos_Watched").group('users.id')
 end 
 
 
 #Fetching patient list who watched video 
 def self.at_least_one_video_watched_model(start_time,end_time,org_id,page,no_of_records,procedure)
   
   UserContentConsume.where("users.demo = 0 AND user_content_consumes.percent_consumed = 100 AND content_id IS NOT NULL AND 
   organization_users.organization_id =? AND TIMESTAMP(user_content_consumes.updated_at) BETWEEN ? and ? "+procedure.to_s,org_id,start_time,end_time)
   .joins("join users  ON users.id = user_content_consumes.user_id")
   .joins("join organization_users on users.id = organization_users.user_id")
   .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
   .select("users.id,AES_DECRYPT(UNHEX(users.first_name), 'Mytonomy') AS 'first_name',
AES_DECRYPT(UNHEX(users.last_name), 'Mytonomy') AS 'last_name',
AES_DECRYPT(UNHEX(users.gender), 'Mytonomy') AS 'Gender',
AES_DECRYPT(UNHEX(users.dob), 'Mytonomy') AS 'dob',
TIMESTAMPDIFF(YEAR, AES_DECRYPT(UNHEX(users.dob),'Mytonomy'), CURDATE()) AS 'Age',
users.email,COUNT(users.id) AS Unique_Videos_Watched").group('users.id').limit(no_of_records).offset(page)
 end 
 
 #Fetching patient count who watched video count
 def self.at_least_one_video_watched_count(start_time,end_time,org_id,procedure)
  
   UserContentConsume.where("users.demo = 0 AND user_content_consumes.percent_consumed = 100 AND content_id IS NOT NULL AND 
   organization_users.organization_id = ?  AND TIMESTAMP(user_content_consumes.updated_at) BETWEEN ? and ? "+procedure.to_s,org_id,start_time,end_time)
   .joins("join users  ON users.id = user_content_consumes.user_id")
   .joins("join organization_users on users.id = organization_users.user_id")
   .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
   .select("COUNT(distinct users.id) AS total_count")
 end 
  #Fetching patient list invited in last 7 days
  def self.sign_up_data(org_id,page,no_of_records,procedure)
    Role.find_by_name(Constants::ROLE_PATIENT).users
    .joins("left join user_content_consumes  ON users.id = user_content_consumes.user_id")
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("left join health_conditions_users ON users.id = health_conditions_users.user_id")
    .where("users.demo = 0 AND organization_users.organization_id = ? 
AND (users.`created_at` BETWEEN ADDDATE(NOW(),-7) AND NOW())and 
(health_conditions_users.procedure_date BETWEEN  NOW() AND ADDDATE(NOW(),2))"+procedure.to_s,org_id).limit(no_of_records).offset(page)
.select("users.id,AES_DECRYPT(UNHEX(users.first_name), 'Mytonomy') AS 'first_name',
AES_DECRYPT(UNHEX(users.last_name), 'Mytonomy') AS 'last_name',
AES_DECRYPT(UNHEX(users.gender), 'Mytonomy') AS 'Gender',
AES_DECRYPT(UNHEX(users.dob), 'Mytonomy') AS 'dob',
TIMESTAMPDIFF(YEAR, AES_DECRYPT(UNHEX(users.dob),'Mytonomy'), CURDATE()) AS 'Age',users.email").
group('users.id').having('(SUM(percent_consumed)=0 or SUM(percent_consumed) is NULL)')
    
  end
  #Fetching patient loggied in location data
  def self.login_loaction_data(start_date,end_date,org_id,procedure)
    LoginHistory.where("users.demo = 0 AND roles_users.role_id = 1 and organization_users.organization_id = ? and DATE(login_histories.login_time) BETWEEN ? and ?
    AND login_histories.longitude is not null and login_histories.latitude is not null"+procedure.to_s,org_id,start_date,end_date)
    .joins("join users on login_histories.user_id= users.id")
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins('join roles_users on users.id= roles_users.user_id')
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .select("login_histories.user_id,login_histories.longitude,login_histories.latitude")
    
  end
  #Fetching most viewed content list by all patient
  def self.content_all_most_viewed_video(start_date,end_date,org_id,procedure)
     UserContentConsume.where("users.demo = 0 AND user_content_consumes.percent_consumed = 100  and 
     organization_users.organization_id= ? AND DATE(user_consumption_logs.updated_at) BETWEEN ? and ? "+procedure.to_s,org_id,start_date,end_date).order('content_watched_count DESC').limit(5)
     .joins("join users ON user_content_consumes.user_id=users.id")
     .joins("join organization_users on users.id = organization_users.user_id")
     .joins("join contents ON user_content_consumes.content_id=contents.id")
     .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
     .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
     .select("contents.name as content_name,
     COUNT(*)  AS content_watched_count").group('contents.id')
  end
  
    #Fetching least viewed content list by all patient 
  def self.content_all_least_viewed_video(start_date,end_date,org_id,procedure)
     UserContentConsume.where("users.demo = 0 AND user_content_consumes.percent_consumed = 100 and 
     organization_users.organization_id=?  AND DATE(user_consumption_logs.updated_at)
       BETWEEN ? and ?"+procedure.to_s,org_id,start_date,end_date).order('content_watched_count asc').limit(5)
     .joins("join users ON user_content_consumes.user_id=users.id")
     .joins("join organization_users on users.id = organization_users.user_id")
     .joins("join contents ON user_content_consumes.content_id=contents.id")
     .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
     .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
     .select("contents.name as content_name,
     COUNT(*) AS content_watched_count").group('contents.id')
  end
  
  #Fetching average all video watched by all patient
  def self.content_avg_all_video_watched(start_date,end_date,org_id,procedure)
    UserContentConsume.where("users.demo = 0 AND user_content_consumes.percent_consumed = 100 
    AND organization_users.organization_id=?  and DATE(user_consumption_logs.updated_at) BETWEEN ? and ?"+procedure.to_s,org_id,start_date,end_date)
    .joins("join contents ON user_content_consumes.content_id=contents.id")
    .joins("join users ON user_content_consumes.user_id=users.id")
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .select("ROUND(COUNT(*)/COUNT(DISTINCT user_content_consumes.user_id),2)  AS content_watched_count")  

    
  end
   #Fetching average unique video watched by all patient
  def self.avg_unique_all_patients(start_date,end_date,org_id,procedure)
    UserContentConsume.where("users.demo  = 0 AND user_content_consumes.percent_consumed = 100  AND organization_users.organization_id=? AND DATE(user_content_consumes.updated_at) BETWEEN ? and ? "+procedure.to_s,org_id,start_date,end_date)
    .joins("JOIN contents ON user_content_consumes.content_id=contents.id")
    .joins("join users ON user_content_consumes.user_id=users.id")
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .select(" ROUND(COUNT(*)/COUNT(DISTINCT user_content_consumes.user_id),2) 
    AS 'total_count_of_content_watched'").first
    
  end
  
  #Fetching most viewed content list by patient before procedure date
  def self.pre_arrival_most_viewed_video(start_date,end_date,org_id,procedure)
    
    HealthConditionsUser.where("users.demo = 0 AND user_content_consumes.percent_consumed = 100 AND 
    user_content_consumes.updated_at < health_conditions_users.procedure_date and 
     organization_users.organization_id= ?  AND DATE(users.created_at) 
     BETWEEN ? and ?"+procedure.to_s,org_id,start_date,end_date).order('content_watched_count DESC').limit(5)
     .joins("join user_content_consumes ON health_conditions_users.user_id=user_content_consumes.user_id")
     .joins("join users ON user_content_consumes.user_id=users.id")
     .joins("join organization_users on users.id = organization_users.user_id")
     .joins("join contents ON user_content_consumes.content_id=contents.id")
     .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
     .select("COUNT(*) AS 
     content_watched_count,contents.name AS content_name").group('user_content_consumes.content_id')
    
  end
  #Fetching least viewed content list by patient before procedure date
 def self.pre_arrival_least_viewed_video(start_date,end_date,org_id,procedure)
    
    HealthConditionsUser.where("users.demo = 0  AND user_content_consumes.percent_consumed = 100 AND 
    user_content_consumes.updated_at < health_conditions_users.procedure_date and 
     organization_users.organization_id= ? AND DATE(users.created_at) 
     BETWEEN ? and ?"+procedure.to_s,org_id,start_date,end_date).order('content_watched_count ASC').limit(5)
     .joins("join user_content_consumes ON health_conditions_users.user_id=user_content_consumes.user_id")
     .joins("join users ON user_content_consumes.user_id=users.id")
     .joins("join organization_users on users.id = organization_users.user_id")
     .joins("join contents ON user_content_consumes.content_id=contents.id")
     .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
     .select("COUNT(*) AS 
     content_watched_count,contents.name AS content_name").group('user_content_consumes.content_id')
    
  end
  #Fetching average all video watched by patient before procedure date
  def self.pre_avg_video_watched(start_date,end_date,org_id,procedure)
    HealthConditionsUser.where("users.demo = 0 AND user_content_consumes.percent_consumed = 100 AND 
    user_content_consumes.updated_at < health_conditions_users.procedure_date and 
     organization_users.organization_id=? AND DATE(user_consumption_logs.updated_at) 
     BETWEEN ? and ?"+procedure.to_s,org_id,start_date,end_date)
     .joins("join user_content_consumes ON health_conditions_users.user_id=user_content_consumes.user_id")
     .joins("join users ON user_content_consumes.user_id=users.id")
     .joins("join organization_users on users.id = organization_users.user_id")
     .joins("join contents ON user_content_consumes.content_id=contents.id")
     .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
     .select("ROUND(COUNT(*)/COUNT(DISTINCT user_content_consumes.user_id),2) 
              AS content_watched_count")
    
  end
  #Fetching average unique video watched by patient before procedure date
  def self.pre_avg_unique_patients(start_date,end_date,org_id,procedure)
    HealthConditionsUser.where("users.demo = 0 AND user_content_consumes.percent_consumed = 100 AND 
    user_content_consumes.updated_at < health_conditions_users.procedure_date and 
     organization_users.organization_id= ? AND  DATE(user_content_consumes.updated_at)
     BETWEEN ? and ?"+procedure.to_s,org_id,start_date,end_date)
     .joins("join user_content_consumes ON health_conditions_users.user_id=user_content_consumes.user_id")
     .joins("join users ON user_content_consumes.user_id=users.id")
     .joins("join organization_users on users.id = organization_users.user_id")
     .joins("join contents ON user_content_consumes.content_id=contents.id")
     .select("ROUND(COUNT(*)/COUNT(DISTINCT user_content_consumes.user_id),2) as total_count")
    
    
  end
  #Fetching most watched content list by patients at hospital
  def self.on_site_most_watched(start_date,end_date,org_id,procedure)
    
    ip_list = self.get_org_ip(org_id)
     address ||= Array.new
    string = "'null'"
    ip_list.each do |row| 
      #address.push(row.ip_address)
      #address.push(row.ip_address)
      string = string + ",'"+ row.ip_address+"'" 
    end
    ipaddress=string
    #ipaddress ="'"+ address[0]+"','"+address[1]+"'"
    
     UserContentConsume.where("users.demo = 0 "+procedure.to_s+" AND user_content_consumes.percent_consumed = 100 and 
     organization_users.organization_id="+org_id.to_s+" AND login_histories.login_ip in ("+ipaddress+") AND DATE(user_consumption_logs.updated_at) BETWEEN '"+start_date+"' and '"+end_date+"'").order('content_watched_count DESC').limit(5)
     .joins("join users ON user_content_consumes.user_id=users.id")
     .joins("join organization_users on users.id = organization_users.user_id")
     .joins("join contents ON user_content_consumes.content_id=contents.id")
     .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
     .joins("join login_histories on login_histories.id = user_consumption_logs.login_id")
     .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
     .select("contents.name as content_name,
      count(*) AS content_watched_count").group('contents.id')
    
    
  end
 
  #Fetching least watched content list by patients at hospital
   def self.on_site_least_watched(start_date,end_date,org_id,procedure)
     ip_list = self.get_org_ip(org_id)
     string = "'null'"
    ip_list.each do |row| 
      #address.push(row.ip_address)
      #address.push(row.ip_address)
      string = string + ",'"+ row.ip_address+"'" 
    end
    ipaddress=string
    #ipaddress ="'"+ address[0]+"','"+address[1]+"'"
   
     UserContentConsume.where("users.demo = 0 "+procedure.to_s+" AND user_content_consumes.percent_consumed = 100  and 
     organization_users.organization_id="+org_id.to_s+" AND login_histories.login_ip in ("+ipaddress+") AND DATE(user_consumption_logs.updated_at) BETWEEN '"+start_date+"' and '"+end_date+"'").order('content_watched_count ASC').limit(5)
     .joins("join users ON user_content_consumes.user_id=users.id")
     .joins("join organization_users on users.id = organization_users.user_id")
     .joins("join contents ON user_content_consumes.content_id=contents.id")
     .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
     .joins("join login_histories on login_histories.id = user_consumption_logs.login_id")
     .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
     .select("contents.name as content_name,
      count(*) AS content_watched_count").group('contents.id')
    
    
  end
  #Fetching average all video watched by patient at hospital
  def self.on_site_avg_all_video_watched(start_date,end_date,org_id,procedure)
    
    ip_list = self.get_org_ip(org_id)
     string = "'null'"
    ip_list.each do |row| 
      #address.push(row.ip_address)
      #address.push(row.ip_address)
      string = string + ",'"+ row.ip_address+"'" 
    end
    ipaddress=string
    #ipaddress ="'"+ address[0]+"','"+address[1]+"'"
    
    UserContentConsume.where("users.demo  = 0 AND user_content_consumes.percent_consumed = 100 AND login_histories.login_ip in ("+ipaddress+") AND  organization_users.organization_id=? 
    and  DATE(user_consumption_logs.updated_at) BETWEEN ? AND ?"+procedure.to_s,org_id,start_date,end_date)
    .joins("join contents ON user_content_consumes.content_id=contents.id")
    .joins("join users on user_content_consumes.user_id=users.id")
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
    .joins("join login_histories on login_histories.id = user_consumption_logs.login_id")
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .select("ROUND(COUNT(*)/COUNT(DISTINCT user_content_consumes.user_id),2) AS content_watched_count")  

    
  end
  #Fetching average unique video watched by patient
  def self.on_site_avg_unique_video_watched(start_date,end_date,org_id,procedure)
    
    ip_list = self.get_org_ip(org_id)
     string = "'null'"
    ip_list.each do |row| 
      #address.push(row.ip_address)
      #address.push(row.ip_address)
      string = string + ",'"+ row.ip_address+"'" 
    end
    ipaddress=string
    #ipaddress ="'"+ address[0]+"','"+address[1]+"'"
    
    
    
    UserContentConsume.where("users.demo = 0 AND user_content_consumes.percent_consumed = 100 AND login_histories.login_ip in ("+ipaddress+") AND  organization_users.organization_id=? AND DATE(user_content_consumes.updated_at) BETWEEN ? AND ?"+procedure.to_s,org_id,start_date,end_date)
    .joins("JOIN contents ON user_content_consumes.content_id=contents.id")
    .joins("join users on user_content_consumes.user_id=users.id")
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
    .joins("join login_histories on login_histories.id = user_consumption_logs.login_id")
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .select(" ROUND(COUNT(DISTINCT user_content_consumes.id)/COUNT(DISTINCT user_content_consumes.user_id),2)  
    AS 'content_watched_count'")
    
  end
  #Fetching count video watched at hospital
  def self.onsite_patient(start_date,end_date,org_id,procedure)
    
     ip_list = self.get_org_ip(org_id)
    string = "'null'"
    ip_list.each do |row| 
      #address.push(row.ip_address)
      #address.push(row.ip_address)
      string = string + ",'"+ row.ip_address+"'" 
    end
    ipaddress=string
   # ipaddress ="'"+ address[0]+"','"+address[1]+"'"
    
    
    UserContentConsume.where("users.demo = 0 AND user_content_consumes.percent_consumed = 100 AND login_histories.login_ip in ("+ipaddress+") AND  organization_users.organization_id=? AND DATE(user_content_consumes.updated_at) BETWEEN ? AND ?"+procedure.to_s,org_id,start_date,end_date)
    .joins("join users ON user_content_consumes.user_id=users.id")
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
    .joins("join login_histories on login_histories.id = user_consumption_logs.login_id")
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .select(" count(user_content_consumes.user_id) AS 'onsite_watched_count'")
    
  end
  #Fetching count video watched before procedure date
   def self.pre_patient(start_date,end_date,org_id,procedure)
    
     ip_list = self.get_org_ip(org_id)
     string = "'null' "
    ip_list.each do |row| 
      #address.push(row.ip_address)
      #address.push(row.ip_address)
      string = string + ",'"+ row.ip_address+"'" 
    end
    ipaddress=string

    #ipaddress ="'"+ address[0]+"','"+address[1]+"'"
    
    
    UserContentConsume.where("users.demo = 0 AND user_content_consumes.percent_consumed = 100 AND login_histories.login_ip not in ("+ipaddress+") AND organization_users.organization_id=? AND DATE(user_content_consumes.updated_at) BETWEEN ? AND ? "+procedure.to_s,org_id,start_date,end_date)
    .joins("join users ON user_content_consumes.user_id=users.id")
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("join user_consumption_logs on user_consumption_logs.ucc_id = user_content_consumes.id")
    .joins("join login_histories on login_histories.id = user_consumption_logs.login_id")
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .select(" count(user_content_consumes.user_id) AS 'pre_watched_count'")
    
  end
  
    def self.sign_up_data_all(org_id,procedure)
    User.joins("left join user_content_consumes  ON users.id = user_content_consumes.user_id")
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("join health_conditions_users on users.id = health_conditions_users.user_id")
    .where("users.demo = 0 AND organization_users.organization_id = ? 
      AND users.`created_at` BETWEEN ADDDATE(NOW(),-7) AND NOW()"+procedure.to_s,org_id)
      .select("users.id,AES_DECRYPT(UNHEX(users.first_name), 'Mytonomy') AS 'first_name',
        AES_DECRYPT(UNHEX(users.last_name), 'Mytonomy') AS 'last_name',
        AES_DECRYPT(UNHEX(users.gender), 'Mytonomy') AS 'Gender',
        AES_DECRYPT(UNHEX(users.dob), 'Mytonomy') AS 'dob',
        TIMESTAMPDIFF(YEAR, AES_DECRYPT(UNHEX(users.dob),'Mytonomy'), CURDATE()) AS 'Age',users.email").
        group('users.id').having('(SUM(percent_consumed)=0 or SUM(percent_consumed) is NULL)')
    
  end
  
  
  
  
  
def self.get_org_ip(org_id)
  #OrganizationIp.find_by organization_id: org_id
   OrganizationIp.where("organization_id= "+org_id.to_s).select("ip_address")
   
  
end

#fetching count of sign up last 7 days
def self.sign_up_count(org_id,procedure)
    Role.find_by_name(Constants::ROLE_PATIENT).users
    .joins("left join user_content_consumes  ON users.id = user_content_consumes.user_id")
    .joins("join organization_users on users.id = organization_users.user_id")
    .joins("left join health_conditions_users ON users.id = health_conditions_users.user_id")
.where("users.demo = 0 AND organization_users.organization_id = ?  
AND (users.`created_at` BETWEEN ADDDATE(NOW(),-7) AND NOW())and ( health_conditions_users.procedure_date BETWEEN NOW() AND ADDDATE(NOW(),2) )"+procedure.to_s,org_id)
.select("users.id,AES_DECRYPT(UNHEX(users.first_name), 'Mytonomy') AS 'first_name',
AES_DECRYPT(UNHEX(users.last_name), 'Mytonomy') AS 'last_name',
AES_DECRYPT(UNHEX(users.gender), 'Mytonomy') AS 'Gender',
AES_DECRYPT(UNHEX(users.dob), 'Mytonomy') AS 'dob',
TIMESTAMPDIFF(YEAR, AES_DECRYPT(UNHEX(users.dob),'Mytonomy'), CURDATE()) AS 'Age',users.email").
group('users.id').having('(SUM(percent_consumed)=0 or SUM(percent_consumed) is NULL)')
    
  end
  
  def self.fetch_average_age_by_organization_sanofi(role,org_id,start_date,end_date)
     User.where("demo =0 AND roles_users.role_id = ? AND FIND_IN_SET(organization_users.organization_id,?) and organization_users.deactivated = 0 AND users.created_at Between ? and ?",role,org_id,start_date,end_date)
   .joins("join organization_users on users.id = organization_users.user_id")
   .joins("join organizations on organizations.id = organization_users.organization_id")
   .joins("join roles_users on users.id = roles_users.user_id")
   .select("round(avg(TIMESTAMPDIFF(YEAR, AES_DECRYPT(UNHEX(dob),'Mytonomy'), CURDATE())),0) as 'Age', organizations.name AS 'Name'")
   .group("organization_users.organization_id")
           
  end
  
    def self.fetch_patient_engagement_after_interim_survey_completed_sanofi(role,org_id,start_date,end_date)
        ActiveRecord::Base.connection.execute("SELECT t.name as Name,COUNT(t.user_id) AS PatientCount FROM
          (SELECT o.id AS orgId,o.name,users.id AS user_id,COUNT(ucc.content_id) FROM users 
          JOIN user_content_consumes ucc ON ucc.user_id = users.id 
          JOIN terms_users tu ON tu.user_id = users.id 
          JOIN organization_users ou ON ou.user_id = users.id 
          JOIN organizations o ON o.id = ou.organization_id 
          JOIN roles_users ON users.id = roles_users.user_id 
          WHERE ou.deactivated = 0 AND tu.accepted = 1 AND FIND_IN_SET(o.id, '"+org_id.to_s+"') 
          AND roles_users.role_id = "+ role.to_s + " AND tu.term_id = 2 and ucc.content_id not in (137,179,181) and ucc.updated_at between '"+start_date+"' and '"+end_date+"'
          GROUP BY users.id HAVING COUNT(ucc.content_id) > 4)t
          GROUP BY t.orgId")
    end
    
    
    def self.fetch_gender_viewship_by_hospital_sanofi(role,org_id,start_date,end_date)     
      
      
      ActiveRecord::Base.connection.execute("SELECT o.name,male.MaleAverageTotalView,female.FemaleAverageTotalView
                FROM organizations o LEFT JOIN (SELECT AVG(t.count) AS MaleAverageTotalView,t.id
                FROM
               (SELECT COUNT(ucl.ucc_id) AS COUNT,ucc.user_id AS user_id,o.name,o.id FROM `users` JOIN `roles_users` ru ON ru.user_id=users.id JOIN `organization_users` ou ON ou.user_id=users.id 
                      JOIN `organizations` o ON o.id=ou.organization_id JOIN terms_users tu ON tu.user_id = users.id 
                      JOIN user_content_consumes ucc ON ucc.user_id = users.id AND content_id IS NOT NULL AND percent_consumed = 100
                      JOIN user_consumption_logs ucl ON ucl.ucc_id = ucc.id
                      WHERE (ru.role_id = 1 AND tu.term_id = 2 AND tu.accepted = 1 AND AES_DECRYPT(UNHEX(users.gender), 'Mytonomy') = 'Male' AND ou.deactivated = 0 AND FIND_IN_SET(o.id,'5,6,7') 
                      AND users.created_at  BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"') 
                      GROUP BY ucc.user_id)t
                      GROUP BY t.id) male ON male.id = o.id
                     LEFT JOIN 
                      (SELECT AVG(t.count) AS FemaleAverageTotalView,t.id
               FROM
               (SELECT COUNT(ucl.ucc_id) AS COUNT,ucc.user_id AS user_id,o.id FROM `users` JOIN `roles_users` ru ON ru.user_id=users.id JOIN `organization_users` ou ON ou.user_id=users.id 
                      JOIN `organizations` o ON o.id=ou.organization_id JOIN terms_users tu ON tu.user_id = users.id 
                      JOIN user_content_consumes ucc ON ucc.user_id = users.id AND content_id IS NOT NULL AND percent_consumed = 100
                      JOIN user_consumption_logs ucl ON ucl.ucc_id = ucc.id
                      WHERE (ru.role_id = 1 AND tu.term_id = 2 AND tu.accepted = 1 AND AES_DECRYPT(UNHEX(users.gender), 'Mytonomy') = 'Female' AND ou.deactivated = 0 AND FIND_IN_SET(o.id,'5,6,7') 
                      AND users.created_at BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"') 
                      GROUP BY ucc.user_id)t
                      GROUP BY t.id) female ON female.id=o.id        
                      WHERE FIND_IN_SET(o.id,'"+org_id.to_s+"')")
      
      
    end
    
    
    def self.fetch_consented_baseline_survey_complete_watched_videos_sanofi(role,org_id,start_date,end_date)
          ActiveRecord::Base.connection.execute(" SELECT COUNT(t.id) AS totalPatient FROM
            (SELECT u.id,COUNT(ucc.content_id)
            FROM user_content_consumes ucc
            JOIN users u ON u.id= ucc.user_id     
            JOIN organization_users ou ON ou.user_id = u.id
            JOIN terms_users tu ON tu.user_id = u.id
            JOIN roles_users ru ON u.id = ru.user_id
            WHERE ou.deactivated = 0 AND u.demo = 0 AND tu.term_id=2 AND accepted=1 
            AND FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"') AND u.created_at BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'  
            AND ucc.content_id NOT IN (SELECT c.content_id FROM `health_conditions` hc 
                                       JOIN collections c ON c.`playlist_id` = hc.`welcome_page_playlist`
                                       WHERE FIND_IN_SET(hc.organization_id,'"+org_id.to_s+"') AND c.content_id IS NOT NULL)
            GROUP BY u.id
            HAVING COUNT(ucc.content_id) >3)t")
    end
    
    def self.fetch_interim_survey_complete_watched_videos_sanofi(role,org_id,start_date,end_date)
      ActiveRecord::Base.connection.execute(" SELECT COUNT(t.id) as totalPatient FROM
           (SELECT u.id,COUNT(sua.id) FROM `survey_user_answers` sua
             JOIN users u ON u.id= sua.user_id     
             JOIN `questions` q ON q.id=sua.question_id
             JOIN organization_users ou ON ou.user_id = u.id
             JOIN terms_users tu ON tu.user_id = u.id
             JOIN roles_users ru ON u.id = ru.user_id
             WHERE ou.deactivated = 0 AND u.demo = 0 AND tu.term_id=2 AND accepted=1 
             AND FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"') AND q.`survey_section_id` = 15 
             AND u.created_at BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"' 
             GROUP BY u.id
             HAVING COUNT(sua.id) = (SELECT COUNT(*) FROM questions WHERE survey_section_id=15))t")
    end
    
    def self.fetch_post_survey_complete_watched_videos_sanofi(role,org_id,start_date,end_date)
      ActiveRecord::Base.connection.execute("SELECT COUNT(
t.id) AS totalPatient FROM
          (SELECT DISTINCT u.id
           FROM `survey_user_answers` sua
            JOIN users u ON u.id= sua.user_id     
            JOIN `questions` q ON q.id=sua.question_id
            JOIN answers a ON a.id= sua.`answer_id`
            JOIN organization_users ou ON ou.user_id = u.id
            JOIN terms_users tu ON tu.user_id = u.id
            JOIN roles_users ru ON u.id = ru.user_id
            WHERE ou.deactivated = 0 AND u.demo = 0 AND tu.term_id=2 AND accepted=1 AND ru.role_id=1
            AND (a.ans_value IS NOT NULL AND a.ans_value != 5) AND sua.answer_id IS NOT NULL 
            AND FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"')
            AND  sua.question_id  
           
IN (SELECT q.id FROM questions q WHERE q.`survey_section_id`
IN (SELECT id FROM `survey_section_titles` WHERE survey_id=4  ) )

             
            AND (DATE(u.created_at) BETWEEN DATE('"+start_date.to_s+"') AND DATE('"+end_date.to_s+"'))
           
            
)t")

    end
    
    
    
    
    
    
    def self.fetch_continued_watched_after_study_ended_sanofi(role,survey_id,org_id,start_date,end_date)
      ActiveRecord::Base.connection.execute("SELECT 
          COUNT(DISTINCT ucc.user_id) AS ContinuedWatchedAfterStudyEnded
          FROM 
          `user_content_consumes` ucc
          JOIN 
          (SELECT u.id,MAX(sua.created_at) AS CompleteDate  
          FROM `survey_user_answers` sua
          JOIN answers a ON a.id= sua.`answer_id`
          JOIN  questions q ON q.id=sua.`question_id`
          JOIN  `users` u ON u.id= sua.user_id
          JOIN `organization_users` ou ON ou.user_id=u.id
          JOIN terms_users tu ON tu.user_id=u.id
          WHERE q.`survey_section_id` in (9,15,16)
          AND FIND_IN_SET(ou.organization_id,'"+org_id.to_s+"') 
          AND tu.term_id=2 AND tu.accepted=1 AND ou.deactivated=0 AND u.demo=0 
          and u.created_at BETWEEN '"+start_date.to_s+"' and '"+end_date.to_s+"'
          GROUP BY u.id)t ON t.id = ucc.user_id
          WHERE ucc.`updated_at` > t.CompleteDate")
    end
      
    #-------------Heal-4181----------------
    def self.fetch_user_comment_excel_data(org_id,proc_id,start_date,end_date,patientgrp_id)
     where_condition = "FIND_IN_SET(u.organization_id,'"+org_id.to_s+"')"
    if !proc_id.blank?
        where_clause = where_condition+ "and FIND_IN_SET(hcu.health_condition_id,'"+proc_id.to_s+"')"
    else
        where_clause = where_condition 
    end
    
    if !patientgrp_id.blank?
     where_clause = where_condition+ "and FIND_IN_SET(uu.unit_id,'"+patientgrp_id.to_s+"')"
    end
    
    UserComment.select("u.id,u.email AS 'email', cn.name AS 'video_title',user_comments.comment AS 'feedback'") 
      .joins("JOIN users u on u.id = user_comments.user_id")
      .joins("LEFT JOIN user_units uu on uu.user_id = u.id")
      .joins("JOIN contents cn on cn.id = user_comments.content_id")
      .joins("JOIN health_conditions_users hcu on hcu.user_id =  u.id")
      .where(where_clause+" and (DATE(u.created_at) BETWEEN DATE('"+start_date+"') 
      AND DATE('"+end_date+"')) and u.demo = 0")
      .group("user_comments.id")
      .order("u.id desc")
    
    end
    
 end

 def health_conditions_where_clause(proc_id,health_cond)
  health_condition = ""
  proc_id.split(",").each_with_index do |hc_id,index|
    if index == (proc_id.split(",").length - 1)
      health_condition = health_condition + health_cond +" = '"+hc_id+"'"
    else
      health_condition = health_condition + health_cond +" = '"+hc_id+"' or "
    end      
  end 
  return health_condition
end

