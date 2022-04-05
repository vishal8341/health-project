class LibCollection < ActiveRecord::Base

  #Associations
  belongs_to :lib_content
  belongs_to :lib_playlist
  # has_many :user_content_consumes

  #Validations
  validates :playlist_id,
            :presence => true

 def self.fetch_playlist_by_parent_id(parent_id,locale_id)
    where("parent_playlist in (?) and content_id is null and lib_playlists.deactivated=0",parent_id)
      .select("lib_collections.id as collection_id,lib_collections.parent_playlist as parent_playlist,lib_collections.playlist_id as playlist_id,
      (case when lib_playlist_locales.name is not null then lib_playlist_locales.name 
          when lpl.name is not null then lpl.name 
          else lib_playlist_details.name end) as playname,CONCAT('#{Constants::SHORTEN_DOMAIN}',su.unique_identifier) as shorten_url")
      .joins("join lib_playlists on lib_playlists.id = lib_collections.playlist_id")
      .joins("join lib_playlist_details on lib_playlist_details.lib_playlist_id = lib_playlists.id")
      .joins("left join lib_playlist_locales on lib_playlists.id=lib_playlist_locales.playlist_id and lib_playlist_locales.locale_id="+locale_id.to_s)
      .joins("left join lib_playlist_locales lpl on lib_playlists.id=lpl.playlist_id and lpl.locale_id=1 ")
      .joins("left join shorten_urls su on su.collection_id = lib_collections.id")
      .group("lib_collections.playlist_id").order('lib_collections.sequence_no asc')
  end

  def self.fetch_contents_by_playlist_id(playlist_id,locale_id)
    where("lib_collections.playlist_id in (?) and lib_collections.content_id is not null and ifnull(lib_contents.deactivated,0)=0",playlist_id)
      .select("lib_collections.id as collection_id,lib_collections.playlist_id,lib_collections.content_id,
        CASE when lcl.name IS NOT NULL then lcl.name
        WHEN lib_content_locales.name IS NOT NULL THEN lib_content_locales.name
      else lib_content_details.name end as name,CONCAT('#{Constants::SHORTEN_DOMAIN}',su.unique_identifier) as shorten_url")
      .joins("left join lib_contents ON lib_contents.id = lib_collections.content_id")
      .joins("left join lib_content_details ON lib_content_details.lib_content_id = lib_collections.content_id")
      .joins("LEFT JOIN lib_content_locales lcl ON lcl.content_id = lib_contents.id and lcl.locale_id="+locale_id.to_s)
      .joins("LEFT JOIN lib_content_locales  ON lib_content_locales.content_id = lib_contents.id and lib_content_locales.locale_id=1")
      .joins("left join shorten_urls su on su.collection_id = lib_collections.id")
      .order('lib_collections.sequence_no asc')
  end



  def self.fetch_contents_by_playlist_id_prev(playlist_id,locale_id)
    where("lib_collections.playlist_id in (?) and ifnull(lib_contents.deactivated,0)=0 and lib_collections.content_id is not null",playlist_id)
      .select("lib_collections.id as collection_id,lib_collections.playlist_id,lib_collections.content_id,lib_contents.content_type,
        CASE when lcl.name IS NOT NULL then lcl.name
        WHEN lib_content_locales.name IS NOT NULL then lib_content_locales.name
        else lib_content_details.name  end as content_name,
        CASE when lcl.thumbnail_ref IS NOT NULL then lcl.thumbnail_ref
        WHEN lib_content_locales.thumbnail_ref IS NOT NULL then lib_content_locales.thumbnail_ref
        else lib_content_details.thumbnail_ref end as thumbnail_ref,
        CASE when lcl.bc_thumbnail_ref IS NOT NULL then lcl.bc_thumbnail_ref
        WHEN lib_content_locales.bc_thumbnail_ref IS NOT NULL then lib_content_locales.bc_thumbnail_ref
        else lib_content_details.bc_thumbnail_ref end as bc_thumbnail_ref,
        CASE when lcl.content_ref IS NOT NULL then lcl.content_ref
        WHEN lib_content_locales.content_ref IS NOT NULL THEN lib_content_locales.content_ref
        else lib_content_details.content_ref end as content_ref,
        CASE when lcl.bc_video_id IS NOT NULL then lcl.bc_video_id
        WHEN lib_content_locales.bc_video_id IS NOT NULL THEN lib_content_locales.bc_video_id
        else lib_content_details.bc_video_id end as bc_video_id,
       locales.name as locale")
      .joins("left join lib_contents ON lib_contents.id = lib_collections.content_id")
      .joins("left join lib_content_details ON lib_content_details.lib_content_id = lib_collections.content_id")
      .joins("LEFT JOIN lib_content_locales lcl ON lcl.content_id = lib_contents.id and lcl.locale_id="+locale_id.to_s)
      .joins("LEFT JOIN lib_content_locales  ON lib_content_locales.content_id = lib_contents.id and lib_content_locales.locale_id=1")
      .joins("left join locales ON lib_content_details.locale_id = locales.id")
      .order('lib_collections.sequence_no asc')
  end  

end
