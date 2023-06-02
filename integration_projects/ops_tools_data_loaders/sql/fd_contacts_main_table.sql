CREATE TABLE IF NOT EXISTS customer_success_freshdesk.fd_contacts (
      contact_id VARCHAR(64)
    , contact_name VARCHAR(256)
    , contact_email VARCHAR(256)
    , contact_company_id VARCHAR(64)
    , record_inserted_time DATETIME
    , last_update DATETIME
    , PRIMARY KEY(contact_id)
    , FOREIGN KEY(contact_company_id) REFERENCES customer_success_freshdesk.fd_companies(company_id)
);

SET FOREIGN_KEY_CHECKS=0;

REPLACE INTO customer_success_freshdesk.fd_contacts
WITH latest_records AS (
	SELECT
		  id
		, MAX(updated_at) AS updated_at
    FROM customer_success_freshdesk.fd_contacts_raw
    GROUP BY
		  id
)
, contacts_records AS (
    SELECT
          SUBSTRING_INDEX(id, '.', 1) AS contact_id
        , IF(name = 'null' OR name = 'nan', NULL, name) AS contact_name
        , IF(email = 'null' OR email = 'nan', NULL, email) AS contact_email
        , IF(company_id = 'null' OR company_id = 'nan', NULL, SUBSTRING_INDEX(company_id, '.', 1)) AS contact_company_id
		, CAST(DATE_FORMAT(inserted_time, '%Y-%m-%d %H:%i:%S') AS DATETIME) AS record_inserted_time
		, CAST(DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%S') AS DATETIME) AS last_update
        , ROW_NUMBER() OVER(PARTITION BY id ORDER BY id ASC, updated_at DESC) AS row_no
    FROM customer_success_freshdesk.fd_contacts_raw
    INNER JOIN latest_records USING (id, updated_at)
)
, contacts_summary AS (
    SELECT
          contact_id
        , contact_name
        , contact_email
        , contact_company_id
        , record_inserted_time
        , last_update
    FROM contacts_records
    WHERE row_no = 1
)
SELECT
      contact_id
    , contact_name
    , contact_email
    , contact_company_id
    , record_inserted_time
    , last_update
FROM contacts_summary
;

SET FOREIGN_KEY_CHECKS=1;
