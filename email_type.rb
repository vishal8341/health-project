class EmailType < ActiveRecord::Base
  PATIENT_INVITE = 'PATIENT_INVITE'
  PATIENT_NEW_INVITE = 'PATIENT_NEW_INVITE'
  RECOVER_USER_ID = 'RECOVER_USER_ID'
  FORGOT_PASSWORD = 'FORGOT_PASSWORD'
  REMINDER_EMAIL = 'REMINDER_EMAIL'
  VIDEO_REMINDER_EMAIL_ONE = 'VIDEO_REMINDER_EMAIL_ONE'
  VIDEO_REMINDER_EMAIL_TWO = 'VIDEO_REMINDER_EMAIL_TWO'
  VIDEO_REMINDER_EMAIL_THREE = 'VIDEO_REMINDER_EMAIL_THREE'
  POST_VIDEO_REMINDER_EMAIL_ONE = 'POST_VIDEO_REMINDER_EMAIL_ONE'
  POST_VIDEO_REMINDER_EMAIL_TWO = 'POST_VIDEO_REMINDER_EMAIL_TWO'
  PENDING_APROVAL = 'PENDING_APROVAL'
  APPROVE_LIBRARY = 'APPROVE_LIBRARY'
  VIDEO_REMINDER_EMAIL_SPANISH_ONE = 'VIDEO_REMINDER_EMAIL_SPANISH_ONE'
  VIDEO_REMINDER_EMAIL_SPANISH_TWO = 'VIDEO_REMINDER_EMAIL_SPANISH_TWO'
  VIDEO_REMINDER_EMAIL_SPANISH_THREE = 'VIDEO_REMINDER_EMAIL_SPANISH_THREE'
  POST_VIDEO_REMINDER_EMAIL_SPANISH_ONE = 'POST_VIDEO_REMINDER_EMAIL_SPANISH_ONE'
  POST_VIDEO_REMINDER_EMAIL_SPANISH_TWO = 'POST_VIDEO_REMINDER_EMAIL_SPANISH_TWO'
  NEW_PATIENT_MULTI_PROCEDURE = 'NEW_PATIENT_MULTI_PROCEDURE'
  ADD_PROCEDURE_MULTI = 'ADD_PROCEDURE_MULTI'
  SEND_TRAINING_INVITE = 'SEND_TRAINING_INVITE'
  STAFF_INVITE = 'STAFF_INVITE'
  ONDUO_REPORT_SEND_CSV = 'ONDUO_REPORT_SEND_CSV'
  DAILY_REPORT_SEND_CSV = 'DAILY_REPORT_SEND_CSV'
  PATIENT_NEW_INVITE = 'PATIENT_NEW_INVITE'
  SURVEY_RESULT='SURVEY_RESULT'
  APPOINTMENT_DATE='APPOINTMENT_DATE'
  AUTOMATED_USER='AUTOMATED_USER'
  WELCOME_AFTER_LOGIN='WELCOME_AFTER_LOGIN'
  FLAGGED_ANSWER_EMAIL='FLAGGED_ANSWER_EMAIL'
  AUTO_RESPOND_SMS='AUTO_RESPOND_SMS'
  AUTO_RESPOND_NO_KEYWORD_SMS='AUTO_RESPOND_NO_KEYWORD_SMS'
  
  def self.fetch_email_type(email_type)
    where("email_type = ?" ,email_type)
    .select("email_types.*").first
  end
end