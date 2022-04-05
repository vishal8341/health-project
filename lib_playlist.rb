class LibPlaylist < ActiveRecord::Base

 def self.find_playlists_by_template(template_id)
    joins('join lib_library_playlists ON  lib_playlists.id = lib_library_playlists.playlist_id')
    .where('lib_library_playlists.library_template_id = ?',template_id)
    
  end
 
end
