class LoginAttempt < ActiveRecord::Base

  #Associations
  belongs_to :user

            
    def self.add_login_attempt(user_id, password,org_id,login_ip,user_agent)
      LoginAttempt.create(organization_id: org_id, user_name: user_id,password: password,login_ip: login_ip, user_agent: user_agent)
    
    end 
    
    
     def self.fetch_login_attempt_count(org_id,start_date,end_date)
      if org_id.blank?
        where_close = ''
      else
        where_close = "FIND_IN_SET(org.id,'"+org_id.to_s+"') AND"
      end
      
      select("count(distinct user_name) as count")
      .joins("left join organizations org on login_attempts.organization_id = org.id")
      .where(where_close+" login_attempts.user_name LIKE '%.com' and DATE(login_attempts.created_at)  BETWEEN ? and ? ",start_date,end_date)
      .order("login_attempts.created_at desc")
    end
    
    def self.fetch_login_attempt_users(org_id,start_date,end_date,page,no_of_records)
      if org_id.blank?
        where_close = ''
        
      else
        where_close = "FIND_IN_SET(org.id,'"+org_id.to_s+"') AND"
      end
      select("count(login_ip) login_attempt,user_name email,org.name org_id, DATE_FORMAT(CONVERT_TZ(login_attempts.created_at,'UTC','US/Eastern'),'%m/%d/%y %H:%i:%s')  as time")
      .joins("left join organizations org on login_attempts.organization_id = org.id")
      .where(where_close+" login_attempts.user_name LIKE '%.com' and DATE(login_attempts.created_at)
       BETWEEN ? and ? ",start_date,end_date)
      .group("user_name").order("login_attempts.created_at desc")
      .limit(no_of_records).offset(page)
    end

end
