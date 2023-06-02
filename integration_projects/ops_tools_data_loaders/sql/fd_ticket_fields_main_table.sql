CREATE TABLE IF NOT EXISTS customer_success_freshdesk.fd_ticket_fields (
      ticket_filed_id VARCHAR(64)
    , ticket_field_name VARCHAR(256)
    , ticket_field_description VARCHAR(256)
    , ticket_field_choices JSON
    , record_inserted_time DATETIME
    , last_update DATETIME
    , PRIMARY KEY(ticket_filed_id)
);

SET FOREIGN_KEY_CHECKS=0;

REPLACE INTO customer_success_freshdesk.fd_ticket_fields
WITH latest_records AS (
	SELECT
		  id
		, MAX(updated_at) AS updated_at
    FROM customer_success_freshdesk.fd_ticket_fields_raw
    GROUP BY
		  id
)
, ticket_fields_records AS (
    SELECT
          SUBSTRING_INDEX(id, '.', 1) AS ticket_filed_id
        , name AS ticket_field_name
        , IF(description = 'null' OR description = 'nan', NULL, description) AS ticket_field_description
        , CAST(IF(choices = 'null' OR choices = 'nan' OR choices = '[]', NULL, choices) AS JSON) AS ticket_field_choices
		, CAST(DATE_FORMAT(inserted_time, '%Y-%m-%d %H:%i:%S') AS DATETIME) AS record_inserted_time
		, CAST(DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%S') AS DATETIME) AS last_update
        , ROW_NUMBER() OVER(PARTITION BY id ORDER BY id ASC, updated_at DESC) AS row_no
    FROM customer_success_freshdesk.fd_ticket_fields_raw
    INNER JOIN latest_records USING (id, updated_at)
)
, ticket_field_summary AS (
    SELECT
          ticket_filed_id
        , ticket_field_name
        , ticket_field_description
        , ticket_field_choices
		, record_inserted_time
		, last_update
    FROM ticket_fields_records
    WHERE row_no = 1
)
SELECT
      ticket_filed_id
    , ticket_field_name
    , ticket_field_description
    , ticket_field_choices
    , record_inserted_time
    , last_update
FROM ticket_field_summary
;

SET FOREIGN_KEY_CHECKS=1;
