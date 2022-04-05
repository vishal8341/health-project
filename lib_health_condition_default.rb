class LibHealthConditionDefault < ActiveRecord::Base
  def self.fetch_value(key,health_condition_id)
    query='RIGHT JOIN app_defaults ON lib_health_condition_defaults.app_defaults_id=app_defaults.id AND health_condition_id='+health_condition_id.to_s
        joins(query)
        .where('app_defaults.key=?',key)
        .select('app_defaults.*,health_condition_defaults.value as org_default,
                 (CASE 
                      WHEN health_condition_defaults.value IS NULL 
                         THEN app_defaults.value
                      ELSE health_condition_defaults.value 
                 END )AS app_flag')
   end
end
