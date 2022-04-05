class Answer < ActiveRecord::Base
  has_many :answer_locales,  foreign_key: "answer_id"
  belongs_to :survey_section_titles
  belongs_to :questions,  foreign_key: "question_id"
  def self.fetch_answer_by_question_id(question_id)
    where("question_id = ?",question_id).order("id")
  end
end
