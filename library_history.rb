class LibraryHistory < ActiveRecord::Base

#Associations
  belongs_to :playlist
  belongs_to :content
  
  def self.add_lib_history(cont_id,play_id,cname,bc_video_id,bc_thumbnail_ref,cont_type,cslug,caction,ctype,cr_by,up_by)
    LibraryHistory.create(content_id: cont_id, playlist_id: play_id, name: cname, bc_video_id: bc_video_id, bc_thumbnail_ref: bc_thumbnail_ref, content_type: cont_type, slug: cslug, action: caction, block_type: ctype, created_by: cr_by, updated_by: up_by )
  end 

end
