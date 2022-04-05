class Question < ActiveRecord::Base
  has_many :answers,  foreign_key: "question_id"
  has_many :question_locales,  foreign_key: "question_id"
  belongs_to :survey_section_titles
  belongs_to :question_type_master, foreign_key: "question_type" 
  
  def answer_id(val)
     answer_id = val
  end
  def answer_id
    #answer_id= answer_id.to_i
  end
  
  def self.fetch_question_with_user_responses(user_id,section_title_id)
    joins(" LEFT JOIN survey_user_answers sua ON sua.question_id=questions.id and user_id="+user_id.to_s)
    .select("questions.id,questions.question,sua.answer_id as answer,questions.question_type,sua.answer_text,seq_no,'' as subtitle,questions.required,questions.tooltip")
    .where("questions.deactivated=0 and survey_section_id=?",section_title_id).order("seq_no")
  end
  
  def self.fetch_question_with_subtitles_user_responses(user_id,section_title_id)
    joins(" JOIN section_subtitles ss ON ss.id = questions.section_subtitle_id ")
    .joins(" LEFT JOIN survey_user_answers sua ON sua.question_id=questions.id and user_id="+user_id.to_s)
    .select("questions.id,questions.question,sua.answer_id as answer,questions.question_type,sua.answer_text,seq_no,ss.name as subtitle,questions.required")
    .where("survey_section_id=?",section_title_id).order("seq_no")
  end
  
  def self.fetch_count_of_questions_answered(user_id,survey_type,organization_id)
    survey_id=SurveyMaster.joins(" JOIN survey_types st ON st.id=survey_masters.survey_type_id")
    .joins(" JOIN survey_organizations org ON survey_masters.id=org.survey_id")
    .where("org.organization_id=? and st.name=?",organization_id,survey_type).first.try(:id)
    Question.joins("JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id ")
    .joins("LEFT  JOIN `survey_user_answers` sua  ON questions.id=sua.`question_id` AND user_id="+user_id.to_s)
    .select("COUNT(*) as total_ques,COUNT(sua.question_id) as ques_answered,questions.`survey_section_id`,sua.created_at")
    .where("sst.`survey_id`=?",survey_id).first
  end
  
  def self.fetch_survey_data(sort_column,sort_direction,organization_id)
    
    key = 'Mytonomy'
    
    role = Role.find_by_name(Constants::ROLE_PATIENT).id
    
   
    select("users.id as user_id,organization_users.organization_id as org_id,organization_users.patient_code,(CASE WHEN ((SELECT COUNT(sua.question_id) FROM survey_user_answers sua
            LEFT JOIN questions ON sua.question_id = questions.id
            LEFT JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id
            where sst.`survey_id`=3 and sua.user_id = users.id
            ) >= 
           (SELECT COUNT(*) FROM  questions JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id WHERE sst.`survey_id`=3)) then 
(SELECT DATE_SUB(MAX(sua.updated_at), INTERVAL 4 HOUR) FROM survey_user_answers sua
            LEFT JOIN questions ON sua.question_id = questions.id
            LEFT JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id
            WHERE sst.`survey_id`=3 AND sua.user_id = users.id
            ) 

 ELSE '' END )as per_survey_done,
    (CASE WHEN ((SELECT COUNT(sua.question_id) FROM survey_user_answers sua
     JOIN questions ON sua.question_id = questions.id
     JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id
      where sst.`survey_id`=4 and sua.user_id = users.id
      ) =
    ((SELECT COUNT(*) FROM  questions JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id WHERE sst.`survey_id`=4))) then (SELECT DATE_SUB(MAX(sua.updated_at), INTERVAL 4 HOUR) FROM survey_user_answers sua
            LEFT JOIN questions ON sua.question_id = questions.id
            LEFT JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id
            WHERE sst.`survey_id`=4 AND sua.user_id = users.id
            )  ELSE '' END )as post_survey_done,
            
    (CASE WHEN ((SELECT COUNT(sua.question_id) FROM survey_user_answers sua
     JOIN questions ON sua.question_id = questions.id
     JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id
      where sst.`survey_id`=8 and sua.user_id = users.id
      ) =
    ((SELECT COUNT(*) FROM  questions JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id WHERE sst.`survey_id`=8))) then (SELECT DATE_SUB(MAX(sua.updated_at), INTERVAL 4 HOUR) FROM survey_user_answers sua
            LEFT JOIN questions ON sua.question_id = questions.id
            LEFT JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id
            WHERE sst.`survey_id`=8 AND sua.user_id = users.id
            )  ELSE '' END )as inter_survey_done,
            CASE WHEN v.watchedCount > 3 AND v.days <= 7 THEN 'Yes' ELSE 'No'        
        END
    AS four_video_watched,

    
    
    gift_cards.name as gift ,user_gift_cards.delivered_at as delivered, 
    users.Has_Email as Has_Email,organization_users.deactivated,
    (CASE WHEN ((SELECT COUNT(*) FROM user_content_consumes
    WHERE user_id=users.id and percent_consumed = 100))>0 then 'YES' else 'NO' end) as first_watched_Date")
    .joins("JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id")
    .joins("LEFT  JOIN `survey_user_answers` sua  ON questions.id=sua.`question_id`")
    .joins("left JOIN users ON sua.user_id=users.id ")
    .joins(" join terms_users tr on tr.user_id = users.id and tr.accepted = 1 and tr.term_id = 2 ")
    .joins("LEFT JOIN organization_users ON organization_users.user_id = users.id")
    .joins( "LEFT JOIN (SELECT un.user_id,COUNT(un.content_id) AS watchedCount, DATEDIFF(MAX(un.created_at),MIN(un.created_at)) AS days

FROM ( SELECT ucc.id,ucc.user_id,ucc.content_id,ucc.created_at,
 @rank_user := IF(@current_user = ucc.user_id, @rank_user + 1, 1) AS user_rank,
                  @current_user := ucc.user_id 
 FROM user_content_consumes ucc WHERE ucc.content_id IS NOT NULL AND ucc.percent_consumed=100 AND ucc.content_id NOT IN (SELECT content_id FROM collections WHERE playlist_id 
    IN  (341,457,468)) ORDER BY ucc.user_id ) un 
WHERE user_rank <=4 
GROUP BY un.user_id) v ON v.user_id = users.id")
    .joins("left JOIN user_gift_cards on user_gift_cards.user_id = users.id")
    .joins("left join gift_cards on gift_cards.id = user_gift_cards.gift_card_id")
     .joins("INNER JOIN `roles_users` ON `users`.`id` = `roles_users`.`user_id` ")
    .where("organization_users.organization_id = ? and roles_users.role_id = 1 and organization_users.deactivated = 0 and users.demo = 0",organization_id)
    .group("users.id").order("users.created_at desc")
    
   
    
    
  end
  
  def self.fetch_total_user_baseline_survey_complete(org_id)
    
    select("(CASE WHEN (COUNT(sua.answer_id) >= 
    (SELECT COUNT(*) FROM  questions 
    JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id 
    WHERE sst.`survey_id`=3)) then 1 ELSE 0 END )as pre_survey_done")
    .joins("LEFT  JOIN `survey_user_answers` sua  ON questions.id=sua.`question_id`")
    .joins("JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id")
    .joins("RIGHT JOIN users ON sua.user_id=users.id OR sua.user_id IS NULL")
    .joins("LEFT JOIN organization_users ON users.id = organization_users.user_id")
    .joins("LEFT JOIN roles_users ON roles_users.user_id = users.id")
    .where("organization_users.organization_id = ? and roles_users.role_id = 1 AND organization_users.deactivated = 0 and users.demo = 0",org_id)
    .group("users.id")
    
  end
   
 def self.fetch_latest_answered_post_survey(user_id,survey_type,organization_id)
   survey_id=SurveyMaster.joins(" JOIN survey_types st ON st.id=survey_masters.survey_type_id")
   .joins(" JOIN survey_organizations org ON survey_masters.id=org.survey_id")
   .where("org.organization_id=? and st.name=?",organization_id,survey_type).first.try(:id)
   puts 'fetch_latest_answered_post_survey'
   Question.joins("JOIN survey_user_answers sua ON questions.id = sua.question_id")
   .joins("JOIN `survey_section_titles` sst ON questions.`survey_section_id`=sst.id")
   .joins("LEFT JOIN organization_users ON sua.user_id = organization_users.user_id")
   .where("organization_users.organization_id = ? and sua.user_id = ? and sst.survey_id=?",organization_id, user_id, survey_id)
   .order("sua.id desc")
   .select("questions.`survey_section_id`, sua.question_id, sua.id") 
   .first   
   
 end  
 
 def self.to_csv(records = [], options = {})
  
    CSV.generate(options) do |csv|
      
      csv << ["Patient Code", "Baseline Survey", "1st Video Watched" ,"4 Videos Watched by Deadline","Intermediate Survey Complete", "Post Survey Complete", "Gift Card Selection", "Gift Card Delivered"]
    
      records.each do |call|
        code_initials = AppDefault.fetch_value(AppDefaults::PATIENT_CODE_INITIALS,call.org_id).first.try(:app_flag)
        p_code = code_initials.to_s+""+call.patient_code.to_s
        puts code_initials
        csv << [p_code,call.per_survey_done, call.first_watched_Date, call.four_video_watched, call.inter_survey_done,  call.post_survey_done,  call.gift, call.delivered]
      end
    end
  end  
   
end
