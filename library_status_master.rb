class LibraryStatusMaster < ActiveRecord::Base

  STATUS = {
          :published =>"Published",
          :approved => "Approved",
          :pending_approval =>"Pending Approval",
          :draft => "Draft",
          :draft_published =>"Draft Published"
        }

end
