class Department < ActiveRecord::Base
  has_many :health_conditions, foreign_key: "department_id"
  has_many :lib_health_conditions, foreign_key: "department_id"
  
  def update=(val)
     update = val
  end
  
  def update
  end

  def self.fecth_department_data(user_id,organization_id,dept_id,where_clause)
    self.select("case when dodn.alias is null then departments.name else dodn.alias end as name,departments.id,group_concat(hc.id) hc_ids,departments.img")
    .joins("join health_conditions hc on hc.department_id = departments.id")
    .joins("join health_conditions_users hcu on  hc.id =hcu.health_condition_id")
    .joins("left join department_organization_display_names dodn on dodn.department_id = departments.id and dodn.organization_id = hc.organization_id")
    .where("hcu.user_id = ? and hc.organization_id=? and departments.id = ? #{where_clause}",user_id,organization_id,dept_id)
    .first
  end  
  
  def self.fetch_department_user_health(user_id,organization_id)
    
    order_clause = ''
    if AppDefault.fetch_value(AppDefaults::MODULE_DEPT_RANK_SORT,organization_id).first.try(:app_flag)=='1'
      order_clause = 'ds.sequence_no'
    else
      order_clause = '"count(hc.id) DESC"'
    end


    self.select("case when dodn.alias is null then departments.name else dodn.alias end as name,departments.id,group_concat(hc.id) hc_ids,min(hcu.created_at) as min_date,departments.img")
    .joins("join health_conditions hc on hc.department_id = departments.id")
    .joins("join health_conditions_users hcu on  hc.id =hcu.health_condition_id")
    .joins("left join department_sequences ds on ds.department_id = departments.id and ds.organization_id = #{organization_id}")
    .joins("left join department_organization_display_names dodn on dodn.department_id = departments.id and dodn.organization_id = hc.organization_id")
    .where("hcu.user_id = ? and hc.organization_id=?",user_id,organization_id)
    .group("departments.id")
    .order(order_clause)
  end
  
  def self.fetch_recommended_department_user_health(user_id,organization_id)
    order_clause = ''
    if AppDefault.fetch_value(AppDefaults::MODULE_DEPT_RANK_SORT,organization_id).first.try(:app_flag)=='1'
      order_clause = 'ds.sequence_no'
    else
      order_clause = '"count(hc.id) DESC"'
    end
    # HealthConditionsUser.select("case when dodn.alias is null then dept.name else dodn.alias end as name,dept.id,group_concat(hc.id) hc_ids")
    # .joins("join health_conditions hc on hc.id = health_conditions_users.health_condition_id")
    # .joins("join department_users du on du.user_id = health_conditions_users.user_id and du.department_id = hc.department_id")
    # .joins("join departments dept on dept.id = du.department_id")
    # .joins("left join department_organization_display_names dodn on dodn.department_id = dept.id and dodn.organization_id = hc.organization_id")
    # .where("health_conditions_users.user_id = ? and hc.organization_id=?",user_id,organization_id).group("dept.id")
    # .order("count(hc.id) DESC")

    HealthConditionsUser.select("case when dodn.alias is null then dept.name else dodn.alias end as name,dept.id,group_concat(hc.id) hc_ids,min(health_conditions_users.created_at) as min_date,dept.img")
    .joins("join health_conditions hc on hc.id = health_conditions_users.health_condition_id")
    .joins("join departments dept on dept.id = hc.department_id")
    .joins("left join department_sequences ds on ds.department_id = dept.id and ds.organization_id = #{organization_id}")
    .joins("left join department_organization_display_names dodn on dodn.department_id = dept.id and dodn.organization_id = hc.organization_id")
    .where("health_conditions_users.user_id = ? and health_conditions_users.is_recommended = ? and health_conditions_users.is_from_epic = ? and hc.organization_id=?",user_id,Constants::IS_RECOMMENDED,Constants::IS_FROM_EPIC,organization_id).group("dept.id")
    .order(order_clause)

  end

  def self.fetch_clinician_assignment_department_user_health(user_id,organization_id)
    order_clause = ''
    if AppDefault.fetch_value(AppDefaults::MODULE_DEPT_RANK_SORT,organization_id).first.try(:app_flag)=='1'
      order_clause = 'ds.sequence_no'
    else
      order_clause = '"count(hc.id) DESC"'
    end
    HealthConditionsUser.select("case when dodn.alias is null then dept.name else dodn.alias end as name,dept.id,group_concat(hc.id) hc_ids,min(health_conditions_users.created_at) as min_date,dept.img")
    .joins("join health_conditions hc on hc.id = health_conditions_users.health_condition_id")
    .joins("join departments dept on dept.id = hc.department_id")
    .joins("left join department_sequences ds on ds.department_id = dept.id and ds.organization_id = #{organization_id}")
    .joins("left join department_organization_display_names dodn on dodn.department_id = dept.id and dodn.organization_id = hc.organization_id")
    .where("health_conditions_users.user_id = ? and health_conditions_users.is_recommended = ? and health_conditions_users.is_from_epic = 0 and hc.organization_id=?",user_id,Constants::IS_RECOMMENDED,organization_id).group("dept.id")
    .order(order_clause)
  end

  def self.fetch_condition_by_dept_and_user_id(dept_id,user_id)
    HealthCondition.select("health_conditions.*").joins("join health_conditions_users hcu on  health_conditions.id =hcu.health_condition_id")
    .where("health_conditions.department_id =? and hcu.user_id = ?",dept_id,user_id)
  end

  def self.fetch_department_alias_name(dept_id)
    DepartmentOrganizationDisplayName.select("o.name as org_name,o.id as oid,department_organization_display_names.id as dept_id,department_organization_display_names.alias as display_name,d.name as name")
    .joins("left join departments d on d.id = department_organization_display_names.department_id")
    .joins("left join organizations o on o.id = department_organization_display_names.organization_id")
    .where("department_organization_display_names.department_id=? and o.parent_id is null",dept_id)
  end

  def self.fetch_assigned_department_and_health_condition(user_id,org_id,auto_assigned_hc)
    self.select("case when dodn.alias is null then departments.name else dodn.alias end as name,departments.id,hc.id as hc_id,hc.name as hc_name,hcu.created_at as assigned_date,hcu.procedure_date,hcu.do_not_know,hcu.created_by")
    .joins("join health_conditions hc on hc.department_id = departments.id")
    .joins("join health_conditions_users hcu on  hc.id =hcu.health_condition_id")
    .joins("left join department_organization_display_names dodn on dodn.department_id = departments.id and dodn.organization_id = hc.organization_id")
    .where("hcu.user_id = ? and hc.organization_id=? #{auto_assigned_hc}",user_id,org_id)
  end  

end
