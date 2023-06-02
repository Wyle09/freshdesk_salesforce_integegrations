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
WITH latest_records AS (
	SELECT
		  id
		, REPLACE(REPLACE(REPLACE(custom_fields, '\\xa0', SPACE(1)), '\\n', SPACE(1)), '\\r\\n', SPACE(1)) AS custom_fields_obj
		, MAX(updated_at) AS updated_at
    FROM customer_success_freshdesk.fd_tickets_raw
    GROUP BY
		  id
        , custom_fields
)
, ticket_records AS (
    SELECT
          SUBSTRING_INDEX(id, '.', 1) AS ticket_id
        , IF(group_id = '%null%' OR group_id = 'nan', NULL, SUBSTRING_INDEX(group_id, '.', 1)) AS ticket_group_id
        , IF(responder_id = 'null' OR responder_id = 'nan', NULL, SUBSTRING_INDEX(responder_id, '.', 1)) AS ticket_agent_id
        , IF(company_id = 'null' OR company_id = 'nan', NULL, SUBSTRING_INDEX(company_id, '.', 1)) AS ticket_company_id
        , priority AS ticket_priority
        , status AS ticket_status
        , subject AS ticket_subject
        , IF(type = 'null' OR type = 'nan', NULL, type) AS ticket_type
        , CAST(DATE_FORMAT(REPLACE(REPLACE(due_by, '"', ''), 'Z', ''), '%Y-%m-%d %H:%i:%S') AS DATETIME) AS ticket_due_by
        , IF(is_escalated = '1', 1, 0) AS ticket_is_escalated
        , IF(requester_id = 'null' OR requester_id = 'nan', NULL, SUBSTRING_INDEX(requester_id, '.', 1)) AS ticket_contact_id
        , IF(custom_fields_obj->>'$.cf_account_type' = 'null' OR custom_fields_obj->>'$.cf_account_type' = 'nan', NULL, custom_fields_obj->>'$.cf_account_type') AS ticket_account_type
        , IF(custom_fields_obj->>'$.cf_app_id' = 'null' OR custom_fields_obj->>'$.cf_app_id' = 'nan', NULL, custom_fields_obj->>'$.cf_app_id') AS ticket_app_id
        , IF(custom_fields_obj->>'$.cf_assigned_team' = 'null' OR custom_fields_obj->>'$.cf_assigned_team' = 'nan', NULL, custom_fields_obj->>'$.cf_assigned_team') AS ticket_assigned_team
        , CAST(DATE_FORMAT(REPLACE(REPLACE(created_at, '"', ''), 'Z', ''), '%Y-%m-%d %H:%i:%S') AS DATETIME) AS ticket_created_at
        , CAST(DATE_FORMAT(REPLACE(REPLACE(updated_at, '"', ''), 'Z', ''), '%Y-%m-%d %H:%i:%S') AS DATETIME) AS ticket_updated_at
        , CAST(IF(associated_tickets_count = 'null' OR associated_tickets_count = 'nan', 0, SUBSTRING_INDEX(associated_tickets_count, '.', 1)) AS UNSIGNED INT) AS ticket_associated_count
        , CAST(IF(tags = '[]' OR tags = 'null' OR tags = 'nan', NULL, tags) AS JSON) AS ticket_tags
        , CAST(DATE_FORMAT(IF(stats->>'$.status_updated_at' = 'null' OR stats->>'$.status_updated_at' = 'nan', NULL, REPLACE(REPLACE(stats->>'$.status_updated_at', '"', ''), 'Z', '')), '%Y-%m-%d %H:%i:%S') AS DATETIME) AS ticket_status_updated_at
		, CAST(DATE_FORMAT(IF(stats->>'$.reopened_at' = 'null' OR stats->>'$.reopened_at' = 'nan', NULL, REPLACE(REPLACE(stats->>'$.reopened_at', '"', ''), 'Z', '')), '%Y-%m-%d %H:%i:%S') AS DATETIME) AS ticket_reopened_at
        , CAST(DATE_FORMAT(IF(stats->>'$.resolved_at' = 'null' OR stats->>'$.resolved_at' = 'nan', NULL, REPLACE(REPLACE(stats->>'$.resolved_at', '"', ''), 'Z', '')), '%Y-%m-%d %H:%i:%S') AS DATETIME) AS ticket_resolved_at
        , CAST(DATE_FORMAT(IF(stats->>'$.closed_at' = 'null' OR stats->>'$.closed_at' = 'nan', NULL, REPLACE(REPLACE(stats->>'$.closed_at', '"', ''), 'Z', '')), '%Y-%m-%d %H:%i:%S') AS DATETIME) AS ticket_closed_at
        , CAST(DATE_FORMAT(IF(stats->>'$.pending_since' = 'null' OR stats->>'$.pending_since' = 'nan', NULL, REPLACE(REPLACE(stats->>'$.pending_since', '"', ''), 'Z', '')), '%Y-%m-%d %H:%i:%S') AS DATETIME) AS ticket_pending_since
        , IF(custom_fields_obj->>'$.cf_platform' = 'null' OR custom_fields_obj->>'$.cf_platform' = 'nan', NULL, custom_fields_obj->>'$.cf_platform') AS ticket_platform
        , IF(custom_fields_obj->>'$.cf_planning_week515231' = 'null' OR custom_fields_obj->>'$.cf_planning_week515231' = 'nan', NULL, custom_fields_obj->>'$.cf_planning_week515231') AS ticket_planning_period
        , IF(custom_fields_obj->>'$.cf_clickup_task_url' = 'null' OR custom_fields_obj->>'$.cf_clickup_task_url' = 'nan', NULL, custom_fields_obj->>'$.cf_clickup_task_url') AS ticket_clickup_task_url
        , CONCAT('https://support.embrace.io/a/tickets/', SUBSTRING_INDEX(id, '.', 1)) AS ticket_url
        , IF(custom_fields_obj->>'$.cf_main_cs_contact' = 'null' OR custom_fields_obj->>'$.cf_main_cs_contact' = 'nan', NULL, custom_fields_obj->>'$.cf_main_cs_contact') AS ticket_assigned_csm_se
        , IF(custom_fields_obj->>'$.cf_clickup_task_id' = 'null' OR custom_fields_obj->>'$.cf_clickup_task_id' = 'nan', NULL, custom_fields_obj->>'$.cf_clickup_task_id') AS ticket_clickup_task_id
        , IF(custom_fields_obj->>'$.cf_clickup_custom_task_id' = 'null' OR custom_fields_obj->>'$.cf_clickup_custom_task_id' = 'nan', NULL, custom_fields_obj->>'$.cf_clickup_custom_task_id') AS ticket_clickup_custom_task_id
        , IF(custom_fields_obj->>'$.cf_clickup_task_status' = 'null' OR custom_fields_obj->>'$.cf_clickup_task_status' = 'nan', NULL, custom_fields_obj->>'$.cf_clickup_task_status') AS ticket_clickup_status
        , CAST(DATE_FORMAT(inserted_time, '%Y-%m-%d %H:%i:%S') AS DATETIME) AS record_inserted_time
		, CAST(DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%S') AS DATETIME) AS last_update
        , ROW_NUMBER() OVER(PARTITION BY id ORDER BY id ASC, updated_at DESC) AS row_no
    FROM customer_success_freshdesk.fd_tickets_raw
    INNER JOIN latest_records USING (id, updated_at)
)
, ticket_summary AS (
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
    FROM ticket_records
    WHERE row_no = 1
)
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
FROM ticket_summary
;

-- Delete tickets that have been flagged as spame or deleted.
DELETE FROM customer_success_freshdesk.fd_tickets USING customer_success_freshdesk.fd_tickets
INNER JOIN customer_success_freshdesk.fd_tickets_spam_deleted 
	ON fd_tickets_spam_deleted.ticket_id = fd_tickets.ticket_id
    AND fd_tickets_spam_deleted.last_update >= fd_tickets.last_update
;

SET FOREIGN_KEY_CHECKS=1;
