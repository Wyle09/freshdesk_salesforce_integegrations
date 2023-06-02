CREATE TABLE IF NOT EXISTS customer_success_salesforce.sf_contacts (
      contact_id VARCHAR(64)
    , contact_name VARCHAR(256)
    , contact_email VARCHAR(256)
    , contact_status VARCHAR(64)
    , contact_status_detail VARCHAR(256)
    , contact_account_id VARCHAR(64)
    , contact_account_name VARCHAR(256)
    , PRIMARY KEY(contact_id)
);

REPLACE INTO customer_success_salesforce.sf_contacts
WITH latest_records AS (
    SELECT
          id
        , MAX(inserted_time) AS inserted_time
    FROM customer_success_salesforce.sf_contacts_raw
    GROUP BY
          id
)
, contact_records AS (
    SELECT
          id AS contact_id
        , name AS contact_name
        , email AS contact_email
        , contact_status__c AS contact_status
        , contact_status_detail__c AS contact_status_detail
        , accountid AS contact_account_id
        , IF(account->>'$.Name' = 'null' OR account->>'$.Name' = 'nan', NULL, REPLACE(account->>'$.Name', '"', '')) AS contact_account_name
        , ROW_NUMBER() OVER(PARTITION BY id ORDER BY id ASC, inserted_time DESC) AS row_no
    FROM customer_success_salesforce.sf_contacts_raw
    INNER JOIN latest_records USING (id, inserted_time)
)
, contact_summary AS (
    SELECT
          contact_id
        , contact_name
        , contact_email
        , contact_status
        , contact_status_detail
        , contact_account_id
        , contact_account_name
    FROM contact_records
    WHERE row_no = 1
)
SELECT
      contact_id
    , contact_name
    , contact_email
    , contact_status
    , contact_status_detail
    , contact_account_id
    , contact_account_name
FROM contact_summary
;
