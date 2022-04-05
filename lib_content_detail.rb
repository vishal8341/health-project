class LibContentDetail < ActiveRecord::Base
  attr_accessor :spanish_text
  extend FriendlyId
    friendly_id :name, use: :slugged
  
  def spanish_text   
  end
  
  def spanish_text=(val)
     spanish_text = val
  end
 
  def fetch_ooyala_thumbnail
   response = Ooyala::API.get("/v2/assets/#{self.content_ref}")
        #response = ooyala_obj.get("/v2/assets/#{self.content_ref}")
        
        response = JSON.parse response.last.first
        
        
        unless response['asset_type'].blank?
          if response['asset_type'] == Constants::VIDEO
            if response['preview_image_url']
              self.thumbnail_ref = response['preview_image_url'] 
              self.duration = Time.at(response['duration']/1000).utc.strftime("%H:%M:%S")             
            else
              fetch_ooyala_thumbnail
              #self.thumbnail_ref = 'http://cf.c.ooyala.com/'+self.content_ref+'/3Gduepif0T1UGY8H4xMDoxOjBrO-I4W8'
            end
          elsif response['asset_type'] == 'audio'
            self.thumbnail_ref = '/assets/audio-thumb.png'
          end
          #self.duration = Time.at(response['duration']/1000).utc.strftime("%H:%M:%S")  if response['duration']
        end
  end

  def self.update_thumbnail_ref_from_brightcove
    
    video_ids=LibContent.fetch_video_ids()
    
    if !video_ids.nil? && !video_ids.blank?      
      client_id = Constants::BRIGHTCOVE_CLIENT_ID
      client_secret = Constants::BRIGHTCOVE_CLIENT_SECRETE
      response = RestClient.post 'https://oauth.brightcove.com/v4/access_token', :client_id=>client_id,:client_secret=>client_secret,:grant_type=>'client_credentials'
      token = JSON.parse(response)["access_token"]
      puts token.inspect
      video_ids.each do |video_id|
        begin          
          get_video_detail_url = "https://cms.api.brightcove.com/v1/accounts/6057940605001/videos/#{video_id.bc_video_id}"
          video_detail_response = RestClient.get get_video_detail_url , { 'Authorization' => "Bearer #{token}", 'Accept' => 'application/json' }  
          video_detail_obj=JSON.parse(video_detail_response)
          images=video_detail_obj["images"]
          thumbnail=images["poster"]
          duration=video_detail_obj["duration"]
          LibContentDetail.where("bc_video_id=?",video_id.bc_video_id).update_all(:bc_thumbnail_ref => thumbnail["src"],:duration => Time.at(duration/1000).strftime("%Y-%m-%d %H:%M:%S"))
          LibContentLocale.where("bc_video_id=?",video_id.bc_video_id).update_all(:bc_thumbnail_ref => thumbnail["src"],:duration => Time.at(duration/1000).strftime("%Y-%m-%d %H:%M:%S"))
          Content.where("bc_video_id=?",video_id.bc_video_id).update_all(:bc_thumbnail_ref => thumbnail["src"],:duration => Time.at(duration/1000).strftime("%Y-%m-%d %H:%M:%S"))
          ContentLocale.where("bc_video_id=?",video_id.bc_video_id).update_all(:bc_thumbnail_ref => thumbnail["src"])
        rescue => e
          Rails.logger.error { "error : , #{e.message} #{e.backtrace.join("\n")}" }
        end
      end
    end
  end
 
end
