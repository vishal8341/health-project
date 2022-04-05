class CareTeam < ActiveRecord::Base
  #Associations
  belongs_to :user
  belongs_to :task_phase

  def self.fetch_care_team_by_organisation_id(organization_id)
    # created query for care team api
    self.select("care_teams.id,care_teams.title,care_teams.user_id,organizations.name as org_name,care_teams.sequence,care_teams.task_phase_id,care_teams.title,care_teams.created_at,
    care_teams.updated_at,users.about_me,users.dob,users.education_level,users.first_name,users.gender,users.Has_Email,users.last_name,users.my_photo,users.phone_no")
                        .joins("left join users on care_teams.user_id = users.id")
                        .joins("left join organization_users on organization_users.user_id = users.id")
                        .joins("left join organizations on organization_users.organization_id = organizations.id")
                        .where("visible = 1 and organizations.id = "+organization_id)
    
  end

end
