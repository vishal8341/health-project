class EventTriggersTime < ActiveRecord::Base
  belongs_to :event, foreign_key: "event_id"
end
