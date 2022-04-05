class LibraryApprover < ActiveRecord::Base

 def self.check_approvers_count_remaining(library_id)
   self.select("COUNT(*) As COUNT")
   .where("library_id = ? AND is_approved = 0 ",library_id).first
 end  
 
 def self.fetch_lib_app_by_user_id_library_id(user_id,library_id)
   self.where(" user_id = ? AND library_id = ? AND is_approved = 0 ",user_id,library_id).first
 end

end
