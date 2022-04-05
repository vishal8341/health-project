class LibContentLocale < ActiveRecord::Base
    belongs_to :lib_contents, foreign_key: "content_id"
    
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
    
    end
    