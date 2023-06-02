CREATE TABLE IF NOT EXISTS customer_success_freshdesk.fd_agents (
	  agent_id VARCHAR(64)
	, agent_email VARCHAR(256)
    , agent_name VARCHAR(256)
    , agent_type VARCHAR(256)
    , record_inserted_time DATETIME
    , last_update DATETIME
	, PRIMARY KEY(agent_id)
);

SET FOREIGN_KEY_CHECKS=0;

REPLACE INTO customer_success_freshdesk.fd_agents
WITH latest_record AS (
	SELECT
		  id
		, MAX(updated_at) AS updated_at
    FROM customer_success_freshdesk.fd_agents_raw
    GROUP BY
		  id
)
, agents_records AS (
	SELECT
		  SUBSTRING_INDEX(id, '.', 1) AS agent_id
		, contact->>'$.email' AS agent_email
		, contact->>'$.name' AS agent_name
		, type AS agent_type
		, CAST(DATE_FORMAT(inserted_time, '%Y-%m-%d %H:%i:%S') AS DATETIME) AS record_inserted_time
		, CAST(DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%S') AS DATETIME) AS last_update
        , ROW_NUMBER() OVER(PARTITION BY id ORDER BY id ASC, updated_at DESC) AS row_no
	FROM customer_success_freshdesk.fd_agents_raw
	INNER JOIN latest_record USING (id, updated_at)
)
, agents_summary AS (
    SELECT
          agent_id
        , agent_email
        , agent_name
        , agent_type
        , record_inserted_time
        , last_update
    FROM agents_records
    WHERE row_no = 1
)
SELECT
	  agent_id
	, agent_email
    , agent_name
    , agent_type
    , record_inserted_time
    , last_update
FROM agents_summary
;

SET FOREIGN_KEY_CHECKS=1;
