class NotificationDeliveryStatus < ActiveRecord::Base
    
    has_one :notification_logs
    has_many :notification_status

    def self.update_notification_details_of_user(email,mail_status,unique_response_id)
        notification_log=NotificationLogs.find_by("email=? and unique_response_id=?",email,unique_response_id)
        if !notification_log.nil?
            email_status_id=NotificationStatus.find_by(NotificationStatus: mail_status).try(:id)
            notification_delv_status=NotificationDeliveryStatus.find_by(notification_log_id: notification_log.id,notification_status_id: email_status_id)
            if notification_delv_status.nil?
                NotificationDeliveryStatus.create(notification_log_id: notification_log.id,notification_status_id: email_status_id,no_of_event_occurred: 1,created_at: Time.zone.now,updated_at: Time.zone.now)
            else
                notification_delv_status.update(no_of_event_occurred: (notification_delv_status.no_of_event_occurred + 1),updated_at: Time.zone.now)
            end
        end
    end

end
  