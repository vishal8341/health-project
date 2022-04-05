class Collection < ActiveRecord::Base

  #Associations
  belongs_to :content
  belongs_to :playlist
  # has_many :user_content_consumes

  #Validations
  validates :playlist_id,
            :presence => true


  def self.fetch_playlist_by_parent_id(parent_id,language_id)
    where("parent_playlist in (?) and playlists.deactivated=0",parent_id)
      .select("playlists.id,collections.id as collection_id,collections.parent_playlist,collections.playlist_id,pl.name,pl.name as playname,pl.name as playlist_url,health_conditions_playlists.health_condition_id")
      .joins("join playlists on playlists.id = collections.playlist_id")
      .joins("join playlist_locales pl on pl.playlist_id = playlists.id AND pl.locale_id = "+language_id.to_s)
       .joins("join health_conditions_playlists ON health_conditions_playlists.playlist_id = playlists.id ")
      .group("collections.playlist_id").order('collections.sequence_no')
  end

  def self.fetch_playlist_by_parent_idv2(parent_id,language_id)
    where("parent_playlist in (?) and playlists.deactivated=0",parent_id)
      .select("playlists.id as playlistId,collections.parent_playlist as tabId,pl.name as playlistName,pl.name as playlist_url,health_conditions_playlists.health_condition_id as TopicId")
      .joins("join playlists on playlists.id = collections.playlist_id")
      .joins("join playlist_locales pl on pl.playlist_id = playlists.id AND pl.locale_id = "+language_id.to_s)
       .joins("join health_conditions_playlists ON health_conditions_playlists.playlist_id = playlists.id ")
      .group("collections.playlist_id").order('collections.sequence_no')
  end

  def self.fetch_contents_by_playlist_id(playlist_id)
    where("collections.playlist_id in (?) and contents.deactivated=0",playlist_id)
      .select("collections.id as collection_id,collections.playlist_id,collections.content_id,contents.content_type,contents.name,contents.name as content_name,contents.thumbnail_ref as thumbnail_ref,contents.content_ref")
      .joins("join contents ON contents.id = collections.content_id")
      .group("contents.id").order('sequence_no')
  end
  # created By content API
  def self.fetch_playlistcontent_by_playlistid(playlist_id,userId,language_id)
    self.where("collections.playlist_id in (?) and collections.content_id is not null and ifnull(contents.deactivated,0)=0",playlist_id)
                           .select("(CASE WHEN content_locales.name is NOT NULL THEN content_locales.name ELSE cl.name END) as content_name,
                                  contents.id,contents.health_condition_id,collections.id as collection_id,collections.playlist_id,collections.content_id,collections.sequence_no as content_sequence,
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
                                  (SELECT ucc.id FROM user_content_consumes ucc WHERE ucc.content_id=contents.id AND ucc.playlist_id="+playlist_id+" AND ucc.user_id="+userId.to_s+") ucc_id,
                                  IFNULL((SELECT ucc.percent_consumed FROM user_content_consumes ucc WHERE ucc.content_id=contents.id AND ucc.playlist_id="+playlist_id+"  AND ucc.user_id="+userId.to_s+"),0) content_percent,
                                  0 as content_like,
                                  0 as content_rating,
                                  '' as content_comment,
                                  collections.parent_playlist as parent_id")
      .joins("left join contents ON contents.id = collections.content_id")
      .joins("left join content_locales  on contents.id = content_locales.content_id AND content_locales.locale_id = "+language_id.to_s )
      .joins("left join content_locales cl on contents.id = cl.content_id AND cl.locale_id = "+language_id.to_s)
      .order('collections.id asc')
  end
  
  # created By ext content API
  def self.fetch_content_by_playlistid(playlist_id,language_id)
    self.where("collections.playlist_id in (?) and collections.content_id is not null and ifnull(contents.deactivated,0)=0",playlist_id)
                           .select("(CASE WHEN content_locales.name is NOT NULL THEN content_locales.name ELSE '' END) as content_name,
                                  contents.id,health_conditions_playlists.health_condition_id,collections.id as collection_id,collections.playlist_id,collections.content_id,collections.sequence_no as content_sequence,
                                  contents.url as content_url,
                                  contents.provider_id as content_provider_id,
                                  contents.description as content_desc,
                                  contents.transcription as content_trans,
                                  contents.slug as content_slug,
                                  contents.thumbnail_ref as content_thumbnail,
                                  contents.content_type as content_type,
                                  TIME_TO_SEC(DATE_FORMAT(contents.duration,'%H:%i:%s'))*1000 as content_duration,
                                  contents.content_ref as content_ref,
                                  '' as content_att_fname")
                                  .joins("left join health_conditions_playlists ON health_conditions_playlists.playlist_id = "+playlist_id)
      .joins("left join contents ON contents.id = collections.content_id")
      .joins("left join content_locales  on contents.id = content_locales.content_id AND content_locales.locale_id = "+language_id.to_s )
      .order('collections.id asc')
  end

  # v2 api to fetch content start
  def self.fetch_content_by_playlistidv2(playlist_id,language_id)
    self.where("collections.playlist_id in (?) and collections.content_id is not null and ifnull(contents.deactivated,0)=0",playlist_id)
                           .select("(CASE WHEN content_locales.name is NOT NULL THEN content_locales.name ELSE '' END) as content_name,
                                  contents.id ,health_conditions_playlists.health_condition_id as TopicId,collections.playlist_id as playlistId,collections.parent_playlist as tabId,collections.sequence_no as content_sequence,
                                  contents.url as content_url,
                                  contents.description as content_desc,
                                  (CASE WHEN content_locales.bc_thumbnail_ref is NOT NULL THEN content_locales.bc_thumbnail_ref ELSE contents.bc_thumbnail_ref END) as content_thumbnail,
                                  (CASE WHEN contents.content_type = 'ooyala'  THEN 'Video' else contents.content_type  END) as content_type,
                                  TIME(contents.duration) as content_duration")
                                  .joins("left join health_conditions_playlists ON health_conditions_playlists.playlist_id = "+playlist_id)
      .joins("left join contents ON contents.id = collections.content_id")
      .joins("left join content_locales  on contents.id = content_locales.content_id AND content_locales.locale_id = "+language_id.to_s )
      .order('collections.sequence_no asc')
  end

  # v2 end api

  
  def self.fetct_contentcount_by_tabid(parent_id)
      self.select("count(content_id) as count")
      .where("parent_playlist = "+parent_id.to_s)
  end
end
