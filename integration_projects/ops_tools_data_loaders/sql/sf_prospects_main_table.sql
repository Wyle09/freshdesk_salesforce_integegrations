CREATE TABLE IF NOT EXISTS customer_success_salesforce.sf_prospects (
      sf_prospects_account_id VARCHAR(64)
    , sf_prospects_account_name VARCHAR(256)
    , sf_prospects_domain VARCHAR(256)
    , sf_prospects_se VARCHAR(256)
    , sf_prospects_account_stage VARCHAR(256)
    , sf_prospects_account_stage_detail VARCHAR(256)
    , sf_prospects_band VARCHAR(16)
    , sf_prospects_arr DECIMAL(10,1)
    , sf_prospects_close_date DATE
    , record_inserted_time DATETIME
    , PRIMARY KEY(sf_prospects_account_id)
);

REPLACE INTO customer_success_salesforce.sf_prospects
WITH latest_records AS (
    SELECT
          id
        , MAX(inserted_time) AS inserted_time
    FROM customer_success_salesforce.sf_prospects_raw
    GROUP BY
          id
)
, prospects_records AS (
    SELECT
          id AS sf_prospects_account_id
        , IF(account->>'$.Name' = 'null' OR account->>'$.Name' = 'nan', NULL, REPLACE(account->>'$.Name', '"', '')) AS sf_prospects_account_name
        , IF(account->>'$.Email_Domain__c' = 'null' OR account->>'$.Email_Domain__c' = 'nan', NULL, REPLACE(account->>'$.Email_Domain__c', '"', '')) AS sf_prospects_domain
        , IF(se_owner__r->>'$.Name' = 'null' OR se_owner__r->>'$.Name' = 'nan', NULL, REPLACE(se_owner__r->>'$.Name', '"', '')) AS sf_prospects_se
        , IF(account->>'$.Account_Stage__c' = 'null' OR account->>'$.Account_Stage__c' = 'nan', NULL, REPLACE(account->>'$.Account_Stage__c', '"', '')) AS sf_prospects_account_stage
        , IF(account->>'$.Account_Stage_Detail__c' = 'null' OR account->>'$.Account_Stage_Detail__c' = 'nan', NULL, REPLACE(account->>'$.Account_Stage_Detail__c', '"', '')) AS sf_prospects_account_stage_detail
        , IF(deal_band__c = 'null' OR deal_band__c = 'nan', NULL, deal_band__c) AS sf_prospects_band
        , CAST(IF(bookings_amt__c = 'null' OR bookings_amt__c = 'nan', 0, bookings_amt__c) AS DECIMAL(10,1)) AS sf_prospects_arr
        , CAST(DATE_FORMAT(IF(closedate = 'null' OR closedate = 'nan', NULL, closedate), '%Y-%m-%d') AS DATE) AS sf_prospects_close_date
		, CAST(DATE_FORMAT(inserted_time, '%Y-%m-%d %H:%i:%S') AS DATETIME) AS record_inserted_time
        , ROW_NUMBER() OVER(PARTITION BY id ORDER BY id ASC, inserted_time DESC) AS row_no
    FROM customer_success_salesforce.sf_prospects_raw
    INNER JOIN latest_records USING (id, inserted_time)
)
, prospects_summary AS (
    SELECT
          sf_prospects_account_id
        , sf_prospects_account_name
        , sf_prospects_domain
        , sf_prospects_se
        , sf_prospects_account_stage
        , sf_prospects_account_stage_detail
        , sf_prospects_band
        , sf_prospects_arr
        , sf_prospects_close_date
        , record_inserted_time
    FROM prospects_records
    WHERE row_no = 1
)
SELECT
      sf_prospects_account_id
    , sf_prospects_account_name
    , sf_prospects_domain
    , sf_prospects_se
    , sf_prospects_account_stage
    , sf_prospects_account_stage_detail
    , sf_prospects_band
    , sf_prospects_arr
    , sf_prospects_close_date
    , record_inserted_time
FROM prospects_summary
;
