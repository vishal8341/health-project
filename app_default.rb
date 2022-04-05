class AppDefault < ActiveRecord::Base
  #use in api app default also for 
  def self.fetch_value(key,organization_id)
    query='LEFT JOIN organization_defaults ON organization_defaults.app_defaults_id=app_defaults.id AND organization_id='+organization_id.to_s
    joins(query)
    .where('app_defaults.key=?',key)
    .select('app_defaults.*,organization_defaults.value as org_default,
             (CASE 
                  WHEN organization_defaults.value IS NULL 
                     THEN app_defaults.value
                  ELSE organization_defaults.value 
             END )AS app_flag')
  end
  
  
  def self.fetch_value_with_locale(key,organization_id,language)
    query='LEFT JOIN organization_defaults ON organization_defaults.app_defaults_id=app_defaults.id AND organization_id='+organization_id.to_s+' Left join locales on locales.id = organization_defaults.locale_id  and locales.name="'+language.to_s+'"'
    joins(query)
    .where('app_defaults.key=?',key)
    .select('(CASE 
                  WHEN organization_defaults.value IS NULL 
                     THEN app_defaults.value
                  ELSE organization_defaults.value 
             END )AS app_flag')
  end
  
end
