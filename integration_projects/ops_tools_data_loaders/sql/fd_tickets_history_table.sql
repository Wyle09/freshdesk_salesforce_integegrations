CREATE TABLE IF NOT EXISTS customer_success_freshdesk.fd_tickets_history (
      id VARCHAR(256)
    , ticket_id VARCHAR(64)
    , subject VARCHAR(256)
    , ticket_url VARCHAR(256)
    , clickup_task_url VARCHAR(256)
    , company_name VARCHAR(256)
    , company_arr DECIMAL(10,2)
    , company_band VARCHAR(32)
    , company_risk_rating DECIMAL(10,2)
    , company_renewal_date DATE
	, account_type VARCHAR(32)
	, assigned_csm_se VARCHAR(64)
	, assigned_team VARCHAR(64)
	, status VARCHAR(64)
	, priority VARCHAR(16)
	, type VARCHAR(256)
	, platform VARCHAR(32)
	, planning_period VARCHAR(64)
    , ticket_is_escalated BIT
	, agent_name VARCHAR(64)
	, group_name VARCHAR(64)
	, resolution_sla_due_date DATE
	, created_date DATE
	, resolved_date DATE
	, closed_date DATE
	, pending_since DATE
    , last_update DATE
    , clickup_task_id VARCHAR(64)
    , clickup_custom_task_id VARCHAR(64)
    , clickup_status VARCHAR(64)
    , days_open INT
	, inserted_time DATETIME
    , PRIMARY KEY(id)
    , INDEX idx_tickets(ticket_id, inserted_time)
);

REPLACE INTO customer_success_freshdesk.fd_tickets_history
WITH fd_tickets AS (
    SELECT
          ticket_id
        , ticket_subject AS subject
        , ticket_url
        , ticket_clickup_task_url AS clickup_task_url
        , COALESCE(NULLIF(company_name, ''), 'NA') AS company_name
		, company_arr
        , COALESCE(NULLIF(company_band, ''), 'NA') AS company_band
        , company_risk_rating
        , company_renewal_date
        , COALESCE(NULLIF(ticket_account_type, ''), 'NA') AS account_type
        , COALESCE(NULLIF(ticket_assigned_csm_se, ''), 'NA') AS assigned_csm_se
        , COALESCE(NULLIF(ticket_assigned_team, ''), 'NA') AS assigned_team
        , ticket_status AS status_id
        , ticket_priority AS priority_id
        , COALESCE(NULLIF(ticket_type, ''), 'NA') AS type
        , COALESCE(NULLIF(ticket_platform, ''), 'NA') AS platform
        , COALESCE(NULLIF(ticket_planning_period, ''), 'NA') AS planning_period
        , ticket_is_escalated
        , COALESCE(NULLIF(agent_name, ''), 'NA') AS agent_name
        , COALESCE(NULLIF(group_name, ''), 'NA') AS group_name
        , DATE_FORMAT(ticket_due_by, '%Y-%m-%d') AS resolution_sla_due_date
        , DATE_FORMAT(ticket_created_at, '%Y-%m-%d') AS created_date
        , DATE_FORMAT(ticket_resolved_at, '%Y-%m-%d') AS resolved_date
        , DATE_FORMAT(ticket_closed_at, '%Y-%m-%d') AS closed_date
        , DATE_FORMAT(ticket_pending_since, '%Y-%m-%d') AS pending_since
        , DATE_FORMAT(fd_tickets.last_update, '%Y-%m-%d') AS last_update
        , COALESCE(NULLIF(ticket_clickup_task_id, ''), 'NA') AS clickup_task_id
        , COALESCE(NULLIF(ticket_clickup_custom_task_id, ''), 'NA') AS clickup_custom_task_id
        , COALESCE(NULLIF(ticket_clickup_status, ''), 'NA') AS clickup_status
        , CAST(NOW() AS DATETIME) AS inserted_time
    FROM customer_success_freshdesk.fd_tickets
    LEFT JOIN customer_success_freshdesk.fd_companies
        ON ticket_company_id = company_id
	LEFT JOIN customer_success_freshdesk.fd_agents
		ON ticket_agent_id = agent_id
	LEFT JOIN customer_success_freshdesk.fd_groups
		ON ticket_group_id = group_id
)
, ticket_status_values AS (
	SELECT
		  s.status_id
		, JSON_UNQUOTE(JSON_EXTRACT(ticket_field_choices, CONCAT('$.', '"', s.status_id, '"', '[0]'))) AS status
	FROM 
		  customer_success_freshdesk.fd_ticket_fields
		, JSON_TABLE(json_keys(ticket_field_choices->'$[0]'), '$[*]' COLUMNS (status_id INT PATH '$')) AS s
	WHERE ticket_field_name = 'status'
)
, ticket_priority_values AS (
	SELECT
          CASE p.priority
              WHEN 'Urgent' THEN 'P0-Urgent'
              WHEN 'High' THEN 'P1-High'
              WHEN 'Medium' THEN 'P2-Medium'
              WHEN 'Low' THEN 'P3-Low'
          END AS priority
		, JSON_UNQUOTE(JSON_EXTRACT(ticket_field_choices, CONCAT('$.', '"', p.priority, '"', '[0]'))) AS priority_id
	FROM 
		  customer_success_freshdesk.fd_ticket_fields
		, JSON_TABLE(json_keys(ticket_field_choices->>'$[0]'), '$[*]' COLUMNS (priority VARCHAR(24) PATH '$')) AS p
	WHERE ticket_field_name = 'priority'
)
, summary AS (
	SELECT DISTINCT
          MD5(CONCAT(ticket_id, COALESCE(CAST(last_update AS CHAR), ''))) AS id
        , ticket_id
        , subject
        , ticket_url
        , clickup_task_url
        , company_name
		, company_arr
        , company_band
        , company_risk_rating
        , company_renewal_date
		, account_type
        , assigned_csm_se
        , assigned_team
        , status
        , CONVERT(priority USING UTF8MB4) AS priority
        , type
        , platform
        , planning_period
        , ticket_is_escalated
        , agent_name
        , group_name
        , resolution_sla_due_date
        , created_date
        , resolved_date
        , closed_date
        , pending_since
        , fd_tickets.last_update
        , clickup_task_id
        , clickup_custom_task_id
        , clickup_status
        , DATEDIFF(COALESCE(resolved_date, closed_date, CURDATE()), created_date) AS days_open
        , inserted_time
    FROM fd_tickets
    INNER JOIN ticket_status_values USING (status_id)
    INNER JOIN ticket_priority_values USING (priority_id)
)
SELECT
      id
    , ticket_id
    , subject
    , ticket_url
    , clickup_task_url
    , company_name
    , company_arr
    , company_band
    , company_risk_rating
    , company_renewal_date
	, account_type
	, assigned_csm_se
	, assigned_team
	, status
	, priority
	, type
	, platform
	, planning_period
    , ticket_is_escalated
	, agent_name
	, group_name
	, resolution_sla_due_date
	, created_date
	, resolved_date
	, closed_date
	, pending_since
    , last_update
	, clickup_task_id
	, clickup_custom_task_id
	, clickup_status
    , days_open
	, inserted_time
FROM summary
;
