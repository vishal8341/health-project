class CampaignUser < ActiveRecord::Base
    
    def self.fetch_users_for_patient_invite_reminder_of_group3(notification_master,patient_invite_adhoc_notification_id)
        select("campaign_users.*")        
        .joins("join notification_logs nl on nl.email=campaign_users.email and nl.notification_master_id=#{patient_invite_adhoc_notification_id.to_i}")
        .joins(" left join users u on u.email=campaign_users.email and u.organization_id=#{notification_master.organization_id.to_i}")
        .joins("left join notification_logs nlog on nlog.email=campaign_users.email and nlog.notification_master_id=#{notification_master.id.to_i}")
        .where("u.id is null and campaign_users.status=1 and nlog.id is null and campaign_users.organization_id=#{notification_master.organization_id.to_i} 
        and campaign_users.group=3 and (TIMESTAMPDIFF(MINUTE, nl.send_time_Stamp, now())) >= #{notification_master.trigger_time_units.to_i}" )        
    end

end
