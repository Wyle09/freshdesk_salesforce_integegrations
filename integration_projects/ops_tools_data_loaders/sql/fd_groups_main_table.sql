CREATE TABLE IF NOT EXISTS customer_success_freshdesk.fd_groups (
      group_id VARCHAR(64)
    , group_name VARCHAR(256)
    , record_inserted_time DATETIME
    , last_update DATETIME
    , PRIMARY KEY(group_id)
);

SET FOREIGN_KEY_CHECKS=0;

REPLACE INTO customer_success_freshdesk.fd_groups
WITH latest_records AS (
	SELECT
		  id
		, MAX(updated_at) AS updated_at
    FROM customer_success_freshdesk.fd_groups_raw
    GROUP BY
		  id
)
, groups_records AS (
    SELECT
          SUBSTRING_INDEX(id, '.', 1) AS group_id
        , name AS group_name
		, CAST(DATE_FORMAT(inserted_time, '%Y-%m-%d %H:%i:%S') AS DATETIME) AS record_inserted_time
		, CAST(DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%S') AS DATETIME) AS last_update
        , ROW_NUMBER() OVER(PARTITION BY id ORDER BY id ASC, updated_at DESC) AS row_no
    FROM customer_success_freshdesk.fd_groups_raw
    INNER JOIN latest_records USING (id, updated_at)
)
, groups_summary AS (
    SELECT
          group_id
        , group_name
        , record_inserted_time
        , last_update
    FROM groups_records
    WHERE row_no = 1
)
SELECT
      group_id
    , group_name
    , record_inserted_time
    , last_update
FROM groups_summary
;

SET FOREIGN_KEY_CHECKS=1;
