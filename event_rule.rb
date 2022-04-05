class EventRule < ActiveRecord::Base
  
  #Heal 4159 - define new variables to store days and minutes 
  def trigger_days(val)
    trigger_days = val
  end
  
  def trigger_days
  end
  
  def trigger_minutes(val)
    trigger_minutes = val
  end
  
  def trigger_minutes
  end
  
  def trigger_hours(val)
    trigger_hours = val
  end
  
  def trigger_hours
  end
  
  def trigger_time_id(val)
    trigger_time_id = val
  end
  
  def trigger_time_id
      #answer_id= answer_id.to_i
  end
    
  def template_content(val)
    template_content = val
  end
    
  def template_content
      #answer_id= answer_id.to_i
  end
    
  #Heal 4304 - to fetch type of notification
  def notification_type_id(val)
  end
   
  #Heal 4304 - to fetch type of notification 
  def notification_type_id
  end

  #Heal 4304 - Changes in query to retrieve rules of both notification types
  def self.fetch_rules(org_id)
    where("(ent.organization_id is null or  ent.organization_id =" + org_id.to_s + ") and event_rules.deactivated=0 and nm.isDeleted=0 ")
    .order("event_rules.id asc")
    .joins(" join event_notification_types ent on ent.rule_id = event_rules.id and ent.locale_id=1 ")
    .joins(" join notification_masters nm on nm.rule_id=event_rules.id and nm.organization_id=" + org_id.to_s)
    .joins(" left join notification_masters_hccs nmh on nmh.notification_master_id=nm.id ")
    .joins(" left join notification_groups ngs on ngs.notification_master_id=nm.id ")
    .group(" event_rules.id,nm.notification_type_id ")   
    .select(" event_rules.id,event_rules.rule_name,event_rules.event_id, (CASE 
    WHEN nm.template_master_id IS NULL 
       THEN ent.template_id
    ELSE nm.template_master_id
END )AS template_master_id, (CASE 
    WHEN nm.type_no IS NULL 
       THEN event_rules.type_no
    ELSE nm.type_no
END )AS type_no, (CASE 
  WHEN nm.notification_type_id IS NULL
     THEN ent.notification_type_id
  ELSE nm.notification_type_id
END )AS not_type_id, nm.trigger_time,nm.trigger_time_units, nm.deactivated,nm.id as notification_id,nm.organization_id, 
ent.organization_id as ent_org_id, 
CASE WHEN nm.rule_alias is not null then nm.rule_alias 
ELSE event_rules.rule_alias END as rule_alias,CASE WHEN nm.rule_label is not null then nm.rule_label 
ELSE event_rules.rule_label END as rule_label,
group_concat(nmh.health_condition_id) as health_condition_ids,group_concat(ngs.group_id) as group_ids,event_rules.show_groups,event_rules.show_topics,event_rules.show_time")
  end
  
  def self.fetch_rules_by_id_org(rule_id, org_id)
     where("id = ? AND organization_id = ?" ,rule_id, org_id)
    .select("event_rules.*").first
  end
  
  def self.fetch_rules_by_org_id_event(org_id, event_id)
     where("organization_id = ? and event_id = ?" ,org_id, event_id)
    .select("event_rules.*")
  end
  
end
