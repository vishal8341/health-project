class EventNotificationTypes < ActiveRecord::Base

  #Heal 4182 Fetch existing templates of rule to  Copy Campaign
  def self.fetch_rule_templates(rule_id, org_id)
  	if org_id.nil? || org_id.blank?
  		where("rule_id= " + rule_id.to_s )
  	  .joins("  join template_masters tm on tm.id=event_notification_types.template_id ")
   	 .select(" event_notification_types.template_id,tm.subject,tm.email_content,tm.email_type, event_notification_types.organization_id,event_notification_types.notification_type_id,event_notification_types.locale_id")
   	else
   		where("rule_id= " + rule_id.to_s + " and event_notification_types.organization_id ='" + org_id.to_s + "'")
  	  .joins("  join template_masters tm on tm.id=event_notification_types.template_id ")
   	 .select(" event_notification_types.template_id,tm.subject,tm.email_content,tm.email_type, event_notification_types.organization_id, event_notification_types.notification_type_id,event_notification_types.locale_id")
   	end
  end
end

