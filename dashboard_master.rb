class DashboardMaster < ActiveRecord::Base
  has_many :organization_dashboards,foreign_key: 'dashboard_id'
  has_many :organizations, through: :organization_dashboards
end
