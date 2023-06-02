CREATE TABLE IF NOT EXISTS customer_success_freshdesk.fd_tickets_spam_deleted (
	  ticket_id VARCHAR(64)
    , last_update DATETIME
    , description VARCHAR(16)
    , PRIMARY KEY(ticket_id)
);

REPLACE INTO customer_success_freshdesk.fd_tickets_spam_deleted
WITH spam_tickets AS (
	SELECT
		  id AS ticket_id
		, CAST(DATE_FORMAT(REPLACE(REPLACE(updated_at, '"', ''), 'Z', ''), '%Y-%m-%d %H:%i:%S') AS DATETIME) AS last_update
        , 'spam' AS description
	FROM customer_success_freshdesk.fd_tickets_spam_raw
)
, deleted_tickets AS (
	SELECT
		  id AS ticket_id
		, CAST(DATE_FORMAT(REPLACE(REPLACE(updated_at, '"', ''), 'Z', ''), '%Y-%m-%d %H:%i:%S') AS DATETIME) AS last_update
        , 'deleted' AS description
	FROM customer_success_freshdesk.fd_tickets_deleted_raw
)
, combine_data AS (
	SELECT 
		  ticket_id
		, last_update
        , description
    FROM spam_tickets
    
	UNION ALL
    
	SELECT 
		  ticket_id
		, last_update
        , description
    FROM deleted_tickets
)
, summary AS (
	SELECT
		  ticket_id
		, last_update
        , description
        , ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY last_update DESC) AS rn
    FROM combine_data
)
SELECT 
	  ticket_id
	, last_update
    , description
FROM summary
WHERE rn = 1
;
