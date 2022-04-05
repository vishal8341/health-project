class Cohort < ActiveRecord::Base

    def self.assign_cohorts_to_users(cohort)
      
        cohort_based_organizations=OrganizationDefault.where("`key`=? and `value`=1",AppDefaults::DUTCH_SURVEY_COHORT_BASIS)
        
        cohort_id=(Cohort.find_by_name(cohort["cohort_name"]).id)
        cohort_info=Hash.new
        cohort_based_organizations.each do |cohort_based_organization|
                  
          cohort_info=CohortOrganization.fetch_cohorts_info_of_organization(cohort_based_organization.organization_id,cohort_id).to_a
          puts cohort_info.inspect
          if !cohort_info.nil? && cohort_info.length > 0
  
            cohort_condition=fetch_condition_with_respect_to_cohort(cohort["cohort_name"])
  
            users_for_cohort_assignment=UserHealthInformation.fetch_users_for_cohort_assignment(cohort_info,cohort_condition)
          
            users_for_cohort_assignment.each do |user| 
                       
              user_cohort=UserCohort.new
              user_cohort.cohort_id=cohort_id
              user_cohort.user_id=user.USER_ID
              user_cohort.created_at=Time.zone.now
              user_cohort.updated_at=Time.zone.now
              user_cohort.save
              
            cohort_info.each do |cohort|
              notificationLog =NotificationLogs.new
              notificationLog.notification_master_id=cohort[5]
              notificationLog.notification_status_id=NotificationStatus.where("NotificationStatus=?",NotificationStatus::SEND_LATER).first.id
              notificationLog.user_id =user.USER_ID
              notificationLog.created_at = Time.zone.now
              notificationLog.updated_at = Time.zone.now
              notificationLog.expected_send_time = Time.zone.now
              notificationLog.content = cohort[4]
              notificationLog.organization_id = cohort[0]
              notificationLog.send_type = cohort[3]
              if cohort[3] == 1 && user.Has_Email.to_i == 1
                notificationLog.email = user.email
                notificationLog.save
              elsif cohort[3] == 2 && !user.phone_no.nil? && !user.phone_no.blank?
                notificationLog.phone_no = user.phone_no
                notificationLog.save
              end              
            end
          end
        end 
      end          
    end

    def self.fetch_condition_with_respect_to_cohort(cohort_name)
      
      cohort_condition=""

      case cohort_name
      when Constants::COHORT1
        cohort_condition="user_health_informations.FH='Y'"
      when Constants::COHORT4A
        cohort_condition="user_health_informations.CATH_IN_LAST_12WEEKS='Y'"
      when Constants::COHORT4B
        cohort_condition="user_health_informations.CATH_NEXT_6WEEKS='Y'"
      when Constants::COHORT2
        cohort_condition="(user_health_informations.FH is null and user_health_informations.LDLAbove190_IN_HISTORY ='Y')"
      when Constants::COHORT5
        cohort_condition=" (user_health_informations.DIABETES='Y' and user_health_informations.LDLAbove70_IN_LAST_12MONTHS='Y')"
      when Constants::COHORT3
        cohort_condition=" (
          (user_health_informations.MI_IN_HISTORY = 'Y' or user_health_informations.STROKE_IN_HISTORY = 'Y' 
            or user_health_informations.CAD_PAD_IN_HISTORY = 'Y' or user_health_informations.PCI_STENT_IN_HISTORY = 'Y') and 
          (user_health_informations.LDLAbove100_IN_HISTORY = 'Y')) "
      end

      return cohort_condition

    end

end