class CohortOrganization < ActiveRecord::Base

    def self.fetch_cohorts_info_of_organization(organization_id,cohort_id)
        puts "&***********************************"
        # joins("join cohort_organization_communications coc on coc.cohort_organization_id=cohort_organizations.id")
        # .joins("join notification_masters nm on nm.id = coc.notification_master_id and notification_type_id in (1,2)")
        # .joins("join template_masters tm on tm.id=nm.template_master_id") 
        # .where("cohort_organizations.organization_id=? and cohort_id=?",organization_id,cohort1_id)
        # .select("cohort_organizations.organization_id,nm.notification_type_id,tm.email_content")

        cohort_info=ActiveRecord::Base.connection.execute("

            select cohort_organizations.organization_id,cohort_organizations.cohort_id,cohort_organizations.no_of_days,
            nm.notification_type_id,tm.email_content,coc.notification_master_id 
            from cohort_organizations
            join cohort_organization_communications coc on coc.cohort_organization_id=cohort_organizations.id
            join notification_masters nm on nm.id = coc.notification_master_id and notification_type_id in (1,2)
            join template_masters tm on tm.id=nm.template_master_id 
            where cohort_organizations.organization_id=#{organization_id} and cohort_id=#{cohort_id}

        ");

        return cohort_info
    end
end
