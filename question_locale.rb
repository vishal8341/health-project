class QuestionLocale < ActiveRecord::Base
  belongs_to :questions, foreign_key: "question_id"

end
