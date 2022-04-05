class LibraryPlaylist < ActiveRecord::Base
  belongs_to :library_template
  belongs_to :playlist
  
  #resuse query for Instruction API 
  def self.fetch_tabs(template_id)
    where("library_template_id =? and collections.parent_playlist is null  and playlists.deactivated=0",template_id)
      .select("collections.id as collection_id,playlists.name,library_playlists.playlist_id")
      .joins("join playlists on playlists.id = library_playlists.playlist_id")
      .joins("join collections on collections.playlist_id = playlists.id")
      .group(" playlists.id").order('sequence_no')
  end
  # query created for lib plalist API
  def self.fetch_playlist_by_library_id(libraryteemplateId)
    self.select("library_playlists.id,library_playlists.playlist_id,library_playlists.library_template_id,library_playlists.created_at,library_playlists.updated_at,playlists.name as playlist_name,library_templates.name as lib_name")
                                  .joins("left join playlists on library_playlists.playlist_id = playlists.id")
                                  .joins("left join library_templates on library_playlists.library_template_id = library_templates.id")
                                  .where("library_template_id = "+libraryteemplateId)
  end
    # query created for lib plalist API

  def self.fetch_playlist_by_playlist_id(playlistId)
    self.select("library_playlists.id,library_playlists.playlist_id,library_playlists.library_template_id,library_playlists.created_at,library_playlists.updated_at,playlists.name as playlist_name,library_templates.name as lib_name")
                                      .joins("left join playlists on library_playlists.playlist_id = playlists.id")
                                      .joins("left join library_templates on library_playlists.library_template_id = library_templates.id")
                                      .where("playlist_id = "+playlistId)
  end
end
