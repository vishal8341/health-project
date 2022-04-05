class LibContent < ActiveRecord::Base
  has_one :lib_content_detail
  accepts_nested_attributes_for :lib_content_detail
  has_many :lib_content_role_types
  has_many :lib_content_locales,  foreign_key: "content_id"  #====Heal-4357
  
  def self.fetch_all_contents(hosp,cond,filetype,lang,title)
    
    where_query = "lib_contents.deactivated = 0"
    
      if !hosp.blank?
        where_query = where_query +" and o.id in ("+(hosp.collect{|i| i.to_i}).map(&:inspect).join(',').to_s+")"
      end
      if !cond.blank?
        where_query = where_query +" and hc.id in ("+(cond.collect{|i| i.to_i}).map(&:inspect).join(',').to_s+")"
      end
      if !lang.blank?
        where_query = where_query +" and lc.id = "+lang.to_s
      end
      if !filetype.blank?
        where_query = where_query +" and lib_contents.content_type like '%"+filetype.to_s+"%'"
      end
       if !title.blank?
        where_query = where_query +" and lcd.name like '%"+title.to_s+"%'"
      end
     
    
    
    puts 'fetch_all_contents'
    select(" lib_contents.id,CASE WHEN (o.name IS NULL) THEN 'NOT AVAILABLE' ELSE o.name END as source, lcd.name assetTitle, DATE_FORMAT(lib_contents.created_at,'%m-%d-%Y') dateUploaded, lcd.version, CASE when lib_contents.content_type = 'ooyala' THEN 'mp4' else lib_contents.content_type end content_type,hc.name diagnosis,lcd.content_ref ref, lc.name locale,lcd.file_name")
    .joins(" JOIN lib_content_details lcd ON lcd.lib_content_id = lib_contents.id ")
    .joins(" LEFT JOIN organizations o ON o.id = lcd.organization_id ") 
    .joins(" LEFT JOIN lib_health_conditions hc ON hc.id = lib_contents.health_condition_id ")
    .joins(" LEFT JOIN locales lc ON lcd.locale_id = lc.id ")
    .where(where_query) 
    .order("lib_contents.id desc") 
  end
  
  
   def self.fetch_all_contents_page(org_id,sort_direction,order_by,hosp,cond,filetype,lang,title,page,no_of_records)
    
    #========heal-4357===========
    if lang.nil? || lang.blank?
       lang=Constants::ENGLISH_LOCALE_ID
    end
    
    where_query = "lib_contents.deactivated = 0"
    
      if !hosp.blank?
        where_query = where_query +" and organization_source_liscences.source_org_id in ("+(hosp.collect{|i| i.to_i}).map(&:inspect).join(',').to_s+") and organization_source_liscences.org_id ="+org_id.to_s
      else
        where_query = where_query +" and organization_source_liscences.org_id ="+org_id.to_s
      end
      if !cond.blank?
        
        where_query = where_query +" and hc.id in ("+(cond.collect{|i| i.to_i}).map(&:inspect).join(',').to_s+")"
      
      end
      if !lang.blank?
        where_query = where_query 
      end
      if !filetype.blank?
        where_query = where_query +" and lib_contents.content_type like '%"+filetype.to_s+"%'"
      end
       if !title.blank?
        where_query = where_query +" and lower(lcd.name) like lower('%"+title.to_s+"%')"
      end
     
    
    
    LibContent.select(" lib_contents.id,CASE WHEN (o.name IS NULL) THEN 'NOT AVAILABLE' ELSE o.name END as source, CASE when lcl.name IS NULL then lcd.name else lcl.name end as 'assetTitle', 
    DATE_FORMAT(lib_contents.created_at,'%m-%d-%Y') dateUploaded, lcd.version, CASE when lib_contents.content_type = 'ooyala' THEN 'mp4' 
    else lib_contents.content_type end content_type,IFNULL(lho.alias, '') as diagnosis,CASE when lcl.content_ref IS NULL then lcd.content_ref else lcl.content_ref end as content_ref, lc.name locale,
      CASE when lcl.bc_video_id IS NULL then lcd.bc_video_id else lcl.bc_video_id end as bc_video_id,
      CASE when lcl.file_name IS NULL then lcd.file_name else lcl.file_name end as 'file_names',organization_source_liscences.is_modification as modified ")
    .joins("left join organization_source_liscences on lib_contents.health_condition_id = organization_source_liscences.health_condition_id")
.joins("JOIN lib_content_details lcd ON lcd.lib_content_id = lib_contents.id  and lcd.organization_id = organization_source_liscences.source_org_id")
.joins("JOIN lib_content_locales lcl ON lcl.content_id = lib_contents.id and lcl.locale_id="+lang.to_s) #========heal-4357===========
.joins("LEFT JOIN organizations o ON o.id = organization_source_liscences.source_org_id")
.joins("LEFT JOIN lib_health_conditions hc ON hc.id = lib_contents.health_condition_id") 
.joins("left join lib_health_condition_organizations lho on lho.health_condition_id =hc.id and lho.organization_id = o.id ")
.joins("LEFT JOIN locales lc ON lcd.locale_id = lc.id")
.where(where_query).group("lib_contents.id") 
    .order(order_by + " " + sort_direction).limit(no_of_records).offset(page)
  end
  
  def self.fetch_all_contents_count(org_id,hosp,cond,filetype,lang,title)
    
    #========heal-4357===========
    if lang.nil? || lang.blank?
      lang=Constants::ENGLISH_LOCALE_ID
   end

    where_query = "lib_contents.deactivated = 0"
    
      if !hosp.blank?
        where_query = where_query +" and organization_source_liscences.source_org_id in ("+(hosp.collect{|i| i.to_i}).map(&:inspect).join(',').to_s+") and organization_source_liscences.org_id ="+org_id.to_s
        else
        where_query = where_query +" and organization_source_liscences.org_id ="+org_id.to_s
      end
      if !cond.blank?
        puts cond
        
        where_query = where_query +" and hc.id in ("+(cond.collect{|i| i.to_i}).map(&:inspect).join(',').to_s+")"
   
      end
      if !lang.blank?
        where_query = where_query +" and lc.id = "+lang.to_s
      end
      if !filetype.blank?
        where_query = where_query +" and lib_contents.content_type like '%"+filetype.to_s+"%'"
      end
       if !title.blank?
        where_query = where_query +" and lower(lcd.name) like lower('%"+title.to_s+"%')"
      end
     
    
    
    puts 'fetch_all_contents'
    LibContent.select("COUNT(distinct lib_contents.id) as c_count")
    .joins("left join organization_source_liscences on lib_contents.health_condition_id = organization_source_liscences.health_condition_id")
.joins("JOIN lib_content_details lcd ON lcd.lib_content_id = lib_contents.id  and lcd.organization_id = organization_source_liscences.source_org_id")
.joins("JOIN lib_content_locales lcl ON lcl.content_id = lib_contents.id and lcl.locale_id="+lang.to_s) #========heal-4357===========
.joins("LEFT JOIN organizations o ON o.id = organization_source_liscences.source_org_id")
.joins("LEFT JOIN lib_health_conditions hc ON hc.id = lib_contents.health_condition_id")
.joins("left join lib_health_condition_organizations lho on lho.health_condition_id =hc.id and lho.organization_id = o.id ") 
.joins("LEFT JOIN locales lc ON lcd.locale_id = lc.id")
.where(where_query) 
     
  end
  
  def self.fetch_assets_for_content_module(org_id,sort_direction,order_by,hosp,cond,filetype,title,page,no_of_records)
    
    puts "======666666666666============"
    puts filetype.inspect
    puts title.inspect
    puts cond.inspect

    where_query = "lib_contents.deactivated = 0 and lcl.name is not null "
    
    if !hosp.blank? && !hosp.nil?
      where_query = where_query +" and organization_source_liscences.source_org_id in ("+(hosp.collect{|i| i.to_i}).map(&:inspect).join(',').to_s+") and organization_source_liscences.org_id ="+org_id.to_s
      else
      where_query = where_query +" and organization_source_liscences.org_id ="+org_id.to_s
    end

    if !cond.blank?
      where_query = where_query +" and hc.id in ("+(cond.collect{|i| i.to_i}).map(&:inspect).join(',').to_s+")"
    end

    if !filetype.blank?
      where_query = where_query +" and lib_contents.content_type like '%"+filetype.to_s+"%'"
    end

    if !title.blank?
      where_query = where_query +" and lower(lcl.name) like lower('%"+title.to_s+"%')"
    end

    LibContent.select(" lib_contents.id,CASE WHEN (o.name IS NULL) THEN 'NOT AVAILABLE' ELSE o.name END as source, 
    CASE when lcl.name IS NOT NULL THEN lcl.name
    WHEN lcd.name IS NOT NULL and lcd.locale_id=1 then lcd.name
    end as 'assetTitle', 
    DATE_FORMAT(lib_contents.created_at,'%m-%d-%Y') dateUploaded, lcd.version, 
    CASE when lib_contents.content_type = 'ooyala' THEN 'mp4' 
    else lib_contents.content_type end content_type,IFNULL(lho.alias, '') as diagnosis,
    CASE when lcl.content_ref IS NOT NULL THEN lcl.content_ref
    WHEN lcd.content_ref IS NOT NULL  and lcd.locale_id=1 THEN lcd.content_ref
    end as content_ref,
    CASE when lcl.bc_video_id IS NOT NULL THEN lcl.bc_video_id
    WHEN lcd.bc_video_id IS NOT NULL  and lcd.locale_id=1 THEN lcd.bc_video_id
    end as bc_video_id,
    lc.name locale,lcl.file_name  as 'file_names',organization_source_liscences.is_modification as modified,
    lcl.locale_id ")
    .joins(" left join organization_source_liscences on lib_contents.health_condition_id = organization_source_liscences.health_condition_id ")
    .joins(" JOIN lib_content_details lcd ON lcd.lib_content_id = lib_contents.id  and lcd.organization_id = organization_source_liscences.source_org_id ")
    .joins(" JOIN lib_content_locales lcl ON lcl.content_id = lib_contents.id and lcl.locale_id=1 ")
    .joins(" LEFT JOIN organizations o ON o.id = organization_source_liscences.source_org_id")
    .joins(" LEFT JOIN lib_health_conditions hc ON hc.id = lib_contents.health_condition_id ")
    .joins(" left join lib_health_condition_organizations lho on lho.health_condition_id =hc.id and lho.organization_id = o.id ")
    .joins(" LEFT JOIN locales lc ON lcl.locale_id = lc.id")
    .where(where_query)
    .group("lib_contents.id") 
    .order(order_by + " " + sort_direction).limit(no_of_records).offset(page)
  
  end

  def self.fetch_assets_count_for_content_module(org_id,hosp,cond,filetype,title)

    where_query = "lib_contents.deactivated = 0 and lcl.name is not null "

    if !hosp.blank? && !hosp.nil?
      where_query = where_query +" and organization_source_liscences.source_org_id in ("+(hosp.collect{|i| i.to_i}).map(&:inspect).join(',').to_s+") and organization_source_liscences.org_id ="+org_id.to_s
      else
      where_query = where_query +" and organization_source_liscences.org_id ="+org_id.to_s
    end

    if !cond.blank?
      where_query = where_query +" and hc.id in ("+(cond.collect{|i| i.to_i}).map(&:inspect).join(',').to_s+")"
    end

    if !filetype.blank?
      where_query = where_query +" and lib_contents.content_type like '%"+filetype.to_s+"%'"
    end

    if !title.blank?
      where_query = where_query +" and lower(lcl.name) like lower('%"+title.to_s+"%')"
    end

    LibContent.select(" COUNT(distinct lib_contents.id) as c_count ")
    .joins(" left join organization_source_liscences on lib_contents.health_condition_id = organization_source_liscences.health_condition_id ")
    .joins(" JOIN lib_content_details lcd ON lcd.lib_content_id = lib_contents.id  and lcd.organization_id = organization_source_liscences.source_org_id ")
    .joins(" JOIN lib_content_locales lcl ON lcl.content_id = lib_contents.id and lcl.locale_id=1 ")
    .joins(" LEFT JOIN organizations o ON o.id = organization_source_liscences.source_org_id")
    .joins(" LEFT JOIN lib_health_conditions hc ON hc.id = lib_contents.health_condition_id ")
    .joins(" left join lib_health_condition_organizations lho on lho.health_condition_id =hc.id and lho.organization_id = o.id ")
    .joins(" LEFT JOIN locales lc ON lcl.locale_id = lc.id")
    .where(where_query)

  end

  def self.assets_bulk_upload()
    csv_path="/home/deepesh/planfirma/Pictures/myto_test.xlsx"
    file_path="/home/deepesh/planfirma/Videos/SampleVideoNew.mp4"
    extracted_data=Util.read_data_from_csv(csv_path.to_s)
    puts extracted_data.inspect
    file_name="SampleVideoNew.mp4"
    folder_name='bulk_videos/'
    extracted_data.each do |data| 
      
      data['Language'].split('/').each do |locale|
        @asset=LibContent.new
        @asset.content_type="ooyala"
        @asset.health_condition_id=HealthCondition.where("name like ? and organization_id=? ", data['SubTopic'],2).first.id
        @asset.created_by=268
        @asset.updated_by=268
        @asset.save
        
        @asset_lib_content_detail=LibContentDetail.new
        @asset_lib_content_detail.content_id=@asset.id
        @asset_lib_content_detail.name=data['Program Title English']
        @asset_lib_content_detail.url="xyz"
        @asset_lib_content_detail.provider_id=1
        @asset_lib_content_detail.organization_id=2
        @asset_lib_content_detail.version=1
        @asset_lib_content_detail.lib_content_id=@asset.id
        @asset_lib_content_detail.version_content_id=@asset.id
        puts Locale.where("language like ? ",locale.to_s).first.id.inspect
        puts locale.to_s.inspect
        @asset_lib_content_detail.locale_id=Locale.where("language like ? ",locale.to_s.replace(" ","")).first.id
        @asset_lib_content_detail.fetch_ooyala_thumbnail
        @asset_lib_content_detail.save
        #uploaded_file_path=Util.upload_video_using_ooyala(folder_name.to_s,file_path.to_s,file_name.to_s)
      end
    end
  end


  def fetch_ooyala_thumbnail(uploaded_file_path)
         
    response = Ooyala::API.get("/v2/assets/#{self.content_ref}")
         #response = ooyala_obj.get("/v2/assets/#{self.content_ref}")
         response = JSON.parse response.last.first
         unless response['asset_type'].blank?
           if response['asset_type'] == Constants::VIDEO
             if response['preview_image_url']
               self.thumbnail_ref = response['preview_image_url'] 
             else
               self.thumbnail_ref = 'http://cf.c.ooyala.com/M0eXRqYTE6iZs_2-4AwaTAiCJvZZTg2x/3Gduepif0T1UGY8H4xMDoxOjA4MTsiGN'
               #fetch_ooyala_thumbnail
               #self.thumbnail_ref = 'http://cf.c.ooyala.com/'+self.content_ref+'/3Gduepif0T1UGY8H4xMDoxOjBrO-I4W8'
             end
           elsif response['asset_type'] == 'audio'
             self.thumbnail_ref = '/assets/audio-thumb.png'
           end
           self.duration = Time.at(response['duration']/1000).utc.strftime("%H:%M:%S") if response['duration']
        
           puts self.duration
         end
        return thumbnail_ref
  end

  def self.fetch_video_ids
    LibContent.select("bc_video_id") 
    .joins(" JOIN lib_content_locales lcd on lcd.content_id=lib_contents.id")
    .where("lib_contents.bc_job_id is not null and lcd.bc_thumbnail_ref is null and lcd.bc_video_id is not null")
  end
  
end
