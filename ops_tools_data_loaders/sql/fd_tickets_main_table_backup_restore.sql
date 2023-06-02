/*
Can use this script to backup & restore the fd tickets table in case a new column is removed/added.
-- Update "MMDDYY" value with the current date (Date of when the script will be executed)
-- Update columns
-- New columns will need to default to NULL when inserting the data
*/

CREATE TABLE IF NOT EXISTS customer_success_freshdesk.fd_tickets_backup_082322 AS
SELECT *
FROM customer_success_freshdesk.fd_tickets
;

SET FOREIGN_KEY_CHECKS=0;

DROP TABLE customer_success_freshdesk.fd_tickets;

SET FOREIGN_KEY_CHECKS=1;


CREATE TABLE IF NOT EXISTS customer_success_freshdesk.fd_tickets (
      ticket_id VARCHAR(64)
    , ticket_group_id VARCHAR(64)
    , ticket_agent_id VARCHAR(64)
    , ticket_company_id VARCHAR(64)
    , ticket_priority VARCHAR(8)
    , ticket_status VARCHAR(8)
    , ticket_subject VARCHAR(256)
    , ticket_type VARCHAR(256)
    , ticket_due_by DATETIME
    , ticket_is_escalated BIT
    , ticket_contact_id VARCHAR(64)
    , ticket_account_type VARCHAR(32)
    , ticket_app_id VARCHAR(256)
    , ticket_assigned_team VARCHAR(64)
    , ticket_created_at DATETIME
    , ticket_updated_at DATETIME
    , ticket_associated_count INT
    , ticket_tags JSON
    , ticket_status_updated_at DATETIME
    , ticket_reopened_at DATETIME
    , ticket_resolved_at DATETIME
    , ticket_closed_at DATETIME
    , ticket_pending_since DATETIME
    , ticket_platform VARCHAR(64)
    , ticket_planning_period VARCHAR(256)
    , ticket_clickup_task_url VARCHAR(256)
    , ticket_url VARCHAR(256)
    , ticket_assigned_csm_se VARCHAR(256)
    , ticket_clickup_task_id VARCHAR(64)
    , ticket_clickup_custom_task_id VARCHAR(64)
    , ticket_clickup_status VARCHAR(64)
    , record_inserted_time DATETIME
    , last_update DATETIME
    , PRIMARY KEY(ticket_id)
    , FOREIGN KEY(ticket_group_id) REFERENCES customer_success_freshdesk.fd_groups(group_id)
    , FOREIGN KEY(ticket_agent_id) REFERENCES customer_success_freshdesk.fd_agents(agent_id)
    , FOREIGN KEY(ticket_company_id) REFERENCES customer_success_freshdesk.fd_companies(company_id)
    , FOREIGN KEY(ticket_contact_id) REFERENCES customer_success_freshdesk.fd_contacts(contact_id)
);


SET FOREIGN_KEY_CHECKS=0;

REPLACE INTO customer_success_freshdesk.fd_tickets
SELECT
      ticket_id
    , ticket_group_id
    , ticket_agent_id
    , ticket_company_id
    , ticket_priority
    , ticket_status
    , ticket_subject
    , ticket_type
    , ticket_due_by
    , ticket_is_escalated
    , ticket_contact_id
    , ticket_account_type
    , ticket_app_id
    , ticket_assigned_team
    , ticket_created_at
    , ticket_updated_at
    , ticket_associated_count
    , ticket_tags
    , ticket_status_updated_at
    , ticket_reopened_at
    , ticket_resolved_at
    , ticket_closed_at
    , ticket_pending_since
    , ticket_platform
    , ticket_planning_period
    , ticket_clickup_task_url
    , ticket_url
    , ticket_assigned_csm_se
	, ticket_clickup_task_id
	, ticket_clickup_custom_task_id
	, ticket_clickup_status
    , record_inserted_time
    , last_update
FROM customer_success_freshdesk.fd_tickets_backup_082322
;

SET FOREIGN_KEY_CHECKS=1;


DROP TABLE customer_success_freshdesk.fd_tickets_backup_082322;
