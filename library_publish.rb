class LibraryPublish < ActiveRecord::Base
  belongs_to :library_status_masters,  foreign_key: "library_status_master_id"
end
