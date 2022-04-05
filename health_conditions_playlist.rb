class HealthConditionsPlaylist < ActiveRecord::Base

  #Associations
  belongs_to :health_condition
  belongs_to :playlist

  #Validations
  validates :health_condition_id,
            :presence => true
  validates :playlist_id,
            :presence => true

  #Reuse for fetch Tabs
 def self.fetch_by_health_condition(health_id,language_id)
      where("health_condition_id in (?) and collections.parent_playlist is null and playlists.deactivated=0",health_id)
      .select("health_condition_id,pl.name,health_conditions_playlists.playlist_id,pl.name as playlist_url")
      .joins("join playlists on playlists.id = health_conditions_playlists.playlist_id")
      .joins("join playlist_locales pl on pl.playlist_id = playlists.id AND pl.locale_id = "+language_id.to_s)
      .joins("join collections on collections.playlist_id = playlists.id")
      .group(" playlists.id").order('collections.sequence_no')
  end

  #v2 fetchtabs
  def self.fetch_by_health_conditionv2(health_id,language_id)
    where("health_condition_id in (?) and collections.parent_playlist is null and playlists.deactivated=0",health_id)
    .select("health_condition_id as TopicId,pl.name as TabName,health_conditions_playlists.playlist_id as TabId,pl.name as playlist_url")
    .joins("join playlists on playlists.id = health_conditions_playlists.playlist_id")
    .joins("join playlist_locales pl on pl.playlist_id = playlists.id AND pl.locale_id = "+language_id.to_s)
    .joins("join collections on collections.playlist_id = playlists.id")
    .group(" playlists.id").order('collections.sequence_no')
end

#end v2 fetchtabs
  

#v2 methods of fetchplaylist
def self.check_tab_have_rights_for_org(tabid,orgid,parentid)
  whereclause = ""
  if parentid.nil? 
    whereclause = "or hc.organization_id in (select id from organizations where parent_id = #{orgid} )" 
  end   
  where("playlist_id = ? and (hc.organization_id = ? #{whereclause}) and hc.deactivated = 0 and playlists.deactivated=0",tabid,orgid)
  .select("health_condition_id ")
  .joins("join health_conditions hc on hc.id = health_condition_id ")
  .joins("join playlists on playlists.id = health_conditions_playlists.playlist_id").first
end

#end v2 methods of fetchplaylist



  def self.fetch_content_detail_by_health_condition(health_id)
    where("health_conditions_playlists.health_condition_id in (?) and collections.content_id is not null and p.deactivated=0 and c.deactivated=0 and c.content_type ='ooyala'",health_id)
    .select("cl.name as content_name,c.id as video_id,c.description as video_description,cl.bc_video_id,cl.bc_thumbnail_ref,(TIME_TO_SEC(TIME(duration)) * 1000) as duration")
    .joins("join playlists p on p.id = health_conditions_playlists.playlist_id")
    .joins("join collections on collections.playlist_id = p.id") 
    .joins("join contents c on c.id = collections.content_id")
    .joins("join content_locales cl on cl.content_id=c.id AND cl.locale_id=1")    
  end
 
 def self.find_playlist_id_with_helth_cond(h_id)
   where("health_conditions.id = ?", h_id)
   .joins("join health_conditions ON health_conditions.id=health_conditions_playlists.health_condition_id")
   .select("health_conditions_playlists .*")
  end    
  
  def self.find_playlist_id_with_helth_condion(h_id)
   where("health_conditions.id = ? and playlists.deactivated = 0 and collections.sequence_no is not null and collections.sequence_no != 0", h_id)
   .joins("join health_conditions ON health_conditions.id=health_conditions_playlists.health_condition_id")
   .joins("join playlists on playlists.id = health_conditions_playlists.playlist_id")
   .joins("join  collections ON collections.parent_playlist= playlists.id")
   .select("health_conditions_playlists.id,health_conditions_playlists.health_condition_id,health_conditions_playlists.playlist_id ,collections.sequence_no,playlists.name")
   .order("collections.sequence_no")
  end 
  
 
def self.find_playlist_id_with_helth_condion_pre(h_id)
   where("health_conditions.id = ? and playlists.deactivated = 0 and collections.sequence_no is not null and collections.sequence_no != 0", h_id)
   .joins("join health_conditions ON health_conditions.id=health_conditions_playlists.health_condition_id")
   .joins("join playlists on playlists.id = health_conditions_playlists.playlist_id")
   .joins("join  collections ON collections.parent_playlist= playlists.id")
   .select("health_conditions_playlists.id,health_conditions_playlists.health_condition_id,health_conditions_playlists.playlist_id ,collections.sequence_no,playlists.name")
   .group("health_conditions_playlists.id")
   .order("collections.sequence_no")
  end 

end
