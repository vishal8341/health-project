class LibLibraryPlaylist < ActiveRecord::Base
  belongs_to :lib_library_template
  belongs_to :lib_playlist
  
  def self.fetch_tabs(template_id,locale_id)
    where("library_template_id =? and lib_collections.parent_playlist is null  and lib_playlists.deactivated=0",template_id.to_s)
      .select("lib_collections.id as collection_id,lib_library_playlists.playlist_id,
        (case when lib_playlist_locales.name is not null then lib_playlist_locales.name 
          when lpl.name is not null then lpl.name 
          else lpb.name end) as name,CONCAT('#{Constants::SHORTEN_DOMAIN}',su.unique_identifier) as shorten_url ")
      .joins("join lib_playlists on lib_playlists.id = lib_library_playlists.playlist_id")
      .joins("join lib_playlist_details lpb on lib_playlists.id = lpb.lib_playlist_id")
      .joins("left join lib_playlist_locales on lib_playlists.id=lib_playlist_locales.playlist_id and lib_playlist_locales.locale_id="+locale_id.to_s)
      .joins("left join lib_playlist_locales lpl on lib_playlists.id=lpl.playlist_id and lpl.locale_id=1 ")
      .joins("join lib_collections on lib_collections.playlist_id = lib_playlists.id")
      .joins("left join shorten_urls su on su.collection_id = lib_collections.id")
      .group(" lib_playlists.id").order('lib_library_playlists.sequence_no asc')
  end
 
end
