class GiftCard < ActiveRecord::Base
  
  
 def self.fetch_gift(org_id)
  joins("join gift_card_organizations gco on gift_cards.id = gco.gift_card_id")
  .where("gco.organization_id = ?",org_id)
end

     
end