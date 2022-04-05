class Content < ActiveRecord::Base
  #mount_uploader :content_ref, ContentUploader
  #Associations
  has_many :collections
  has_many :playlists, through: :collections
  has_many :collections
  has_many :playlists, through: :collections
  has_many :login_histories
  has_many :user_assignments

  extend FriendlyId
  friendly_id :name, use: :slugged

  #Validations
  validates :name,
            :presence => true
  validates :url,
            :presence => true
  validates :provider_id,
            :presence => true

  before_save :set_thumbnail
  
  def organization_id
  end
  
  def organization_id=(val)
     organization_id = val
  end
  
  def locale_id=(val)
     locale_id = val
  end
  
  def locale_id
  end
  
  def set_thumbnail
    #Setting content url for watching content
    unless self.content_ref.blank? || self.content_type.blank?
      if content_type == Constants::OOYALA
        fetch_ooyala_thumbnail
        # self.thumbnail_ref = 'http://cf.c.ooyala.com/'+self.content_ref+'/3Gduepif0T1UGY8H4xMDoxOjBrO-I4W8'
        
      elsif content_type == 'pdf'
          self.thumbnail_ref = 'https://s3.amazonaws.com/mytonomy-health/production/AdobePDF.jpg'
      else 
          self.thumbnail_ref = 'https://s3.amazonaws.com/mytonomy-health/production/content-marketing-1.png'
      end
    end
  end
  
 def fetch_ooyala_thumbnail
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
 end

 def self.fetch_contents_by_org_id(org_id)
   where("organization_id = ?",org_id)
      .joins("join organization_contents on contents.id = organization_contents.content_id")
      .group("contents.id")
 end
 
 def self.search(search,org_id)
   where("name LIKE ? AND organization_id = ?", "%#{search}%",org_id) 
      .joins("join organization_contents on contents.id = organization_contents.content_id")
      .group("contents.id")
 end
 
 def self.find_thumbnail_ref_by_parent_playlist_id(playlist_id)
      joins("join collections ON collections.content_id=contents.id ")
      .where("collections.parent_playlist = ? and contents.content_type=?", playlist_id, "ooyala")
      .order("sequence_no")
      .select("contents.id,name,collections.sequence_no,thumbnail_ref,content_type,bc_thumbnail_ref")
 end
 
  def self.find_thumbnail_ref_by_parent_playlist_id1(playlist_id)
      joins("join collections ON collections.content_id=contents.id ")
      .where("collections.parent_playlist = ? and contents.content_type=?", playlist_id, "ooyala")
      .order("sequence_no")
      .select("thumbnail_ref")
 end
 
 def self.fetch_contents_by_playlist_id(playlist_id)
      joins("join collections ON collections.content_id=contents.id ")
      .where("collections.playlist_id = ?", playlist_id)
      .order("sequence_no")
      .select("contents.id,name,collections.sequence_no,thumbnail_ref")
      
 end
 
 def self.get_asset_list(key,page,no_of_records,org_id)
   
   where("contents.name like ? and organizations.id = ?",key,org_id).limit(no_of_records).offset(page).group("contents.id")
  .joins("left join collections collections on contents.id = collections.content_id")
  .joins("left join library_playlists on collections.playlist_id = library_playlists.playlist_id")
  .joins("left join library_organizations on library_playlists.library_template_id = library_organizations.library_template_id")
  .joins("left join organizations  on organizations.id=library_organizations.organization_id")
  .select("contents.*, (GROUP_CONCAT(DISTINCT organizations.abbreviation)) as org_name")
   
   
   
  
  #self.all.where("name like ?",key).limit(no_of_records).offset(page)
 
 end
 
 def self.get_asset_count(key,org_id)
   where("contents.name like ? and organizations.id = ?",key,org_id).group("contents.id")
  .joins("left join collections collections on contents.id = collections.content_id")
  .joins("left join library_playlists on collections.playlist_id = library_playlists.playlist_id")
  .joins("left join library_organizations on library_playlists.library_template_id = library_organizations.library_template_id")
  .joins("left join organizations  on organizations.id=library_organizations.organization_id")
  .select("count(contents.id) as assets")
   
 end
 
   def self.add_content(provider_id,content_ref,content_type,name,created_by,updated_by)
    
    self.create(provider_id: provider_id,content_ref: content_ref,content_type: content_type,name: name,created_by: created_by,updated_by: updated_by)
    
  end
  def self.find_thumbnail_ref_by_health_cond_id(hid)
                           joins("inner join playlists  on health_conditions_playlists.playlist_id = playlists.id")
                          .joins("left join playlist_locales  on playlist_locales.playlist_id = playlists.id AND playlist_locales.locale_id =" + "1")
                          .joins("left join playlist_locales pl on pl.playlist_id = playlists.id AND pl.locale_id = 1 ")
                          .joins("inner join collections  on health_conditions_playlists.playlist_id = collections.playlist_id")
                          .joins("inner join contents  on collections.content_id = contents.id")
                          .joins("left join content_locales  on contents.id = content_locales.content_id AND content_locales.locale_id = " + "1")
                          .joins("left join content_locales cl on contents.id = cl.content_id AND cl.locale_id = 1 ")
                         .select('thumbnail_ref').first
  end 
 
def self.fetch_playlistcontent_by_content_id(contentId,userId,language_id)
    
    self.select("contents.id,(CASE WHEN content_locales.name is NOT NULL THEN content_locales.name ELSE cl.name END) as content_name,
                                  contents.url as content_url,
                                  contents.provider_id as content_provider_id,
                                  contents.description as content_desc,
                                  contents.transcription as content_trans,
                                  contents.slug as content_slug,
                                  contents.thumbnail_ref as content_thumbnail,
                                  contents.content_type as content_type,
                                  TIME_TO_SEC(DATE_FORMAT(contents.duration,'%H:%i:%s'))*1000 as content_duration,
                                  contents.content_ref as content_ref,
                                  '' as content_att_fname,
                                  (SELECT ucc.id FROM user_content_consumes ucc WHERE ucc.content_id=contents.id AND ucc.user_id="+userId.to_s+"  limit 1) ucc_id,
                                  IFNULL((SELECT ucc.percent_consumed FROM user_content_consumes ucc WHERE ucc.content_id=contents.id AND ucc.user_id="+userId.to_s+" limit 1),0) content_percent,
                                  0 as content_like,
                                  0 as content_rating,
                                  '' as content_comment")
      .joins("left join content_locales  on contents.id = content_locales.content_id AND content_locales.locale_id = "+language_id.to_s )
      .joins("left join content_locales cl on contents.id = cl.content_id AND cl.locale_id = "+language_id.to_s)
      .where("contents.id ="+contentId)
  end


end
