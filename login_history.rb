class LoginHistory < ActiveRecord::Base

  #Associations
  belongs_to :user
  belongs_to :content, foreign_key: :last_watched
  belongs_to :user_content_consume, foreign_key: :last_watched
  has_many :user_consumption_logs

  #Validations
  validates :user_id,
            :presence => true
  validates :login_time,
            :presence => true
            
            def self.add_history(user_id, longitude,latitude,login_ip,login_time, user_agent,city,state,org_id)
              LoginHistory.create(user_id: user_id, longitude: longitude,latitude: latitude,login_ip: login_ip, login_time: login_time, user_agent: user_agent,city: city,state: state,organization_id: org_id)
            
            end 

end
