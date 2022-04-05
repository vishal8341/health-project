class GoogleAnalyticsDatum < ActiveRecord::Base
  belongs_to :organization, foreign_key: "organization_id"
  
  
  def self.page_load_time(start_date,end_date,org_id,flag)
    where_condition = ''
     
   if !org_id.blank?
      where_clause="and FIND_IN_SET(google_analytics_data.organization_id ,'"+org_id.to_s+"')"
   else
     where_clause = where_condition
    end
    
 
   select("google_analytics_data.page_title,google_analytics_data.page_path,google_analytics_data.page_load_time,google_analytics_data.browser_type")
  .where("google_analytics_data.deactivated=0 "+where_clause+"and google_analytics_data.page_load_time>0.00 and google_analytics_data.flag=?",flag)
  .limit(10)
 
  end
end
