CREATE TABLE IF NOT EXISTS customer_success_salesforce.sf_unmanaged_customers (
      sf_unmanaged_customer_account_id VARCHAR(64)
    , sf_unmanaged_customer_account_name VARCHAR(256)
    , sf_unmanaged_customer_domain VARCHAR(256)
    , sf_unmanaged_customer_csm VARCHAR(256)
    , sf_unmanaged_customer_account_stage VARCHAR(256)
    , sf_unmanaged_customer_account_stage_detail VARCHAR(256)
    , sf_unmanaged_customer_band VARCHAR(16)
    , sf_unmanaged_customer_arr DECIMAL(10,1)
    , sf_unmanaged_customer_renewal_date DATE
    , sf_unmanaged_customer_risk_rating DECIMAL(10,1)
    , sf_unmanaged_customer_won_date DATE
    , sf_unmanaged_customer_churned_date DATE
    , record_inserted_time DATETIME
    , PRIMARY KEY(sf_unmanaged_customer_account_id)
);

REPLACE INTO customer_success_salesforce.sf_unmanaged_customers
WITH latest_records AS (
    SELECT
          id
        , MAX(inserted_time) AS inserted_time
    FROM customer_success_salesforce.sf_unmanaged_customers_raw
    GROUP BY
          id
)
, unmanaged_customers_records AS(
    SELECT
          id AS sf_unmanaged_customer_account_id
        , name AS sf_unmanaged_customer_account_name
        , email_domain__c AS sf_unmanaged_customer_domain
        , NULL AS sf_unmanaged_customer_csm
        , IF(account_stage__c = 'null' OR account_stage__c = 'nan', NULL, account_stage__c) AS sf_unmanaged_customer_account_stage
        , IF(account_stage_detail__c = 'null' OR account_stage_detail__c = 'nan', NULL, account_stage_detail__c) AS sf_unmanaged_customer_account_stage_detail
        , IF(band__c = 'null' OR band__c = 'nan', NULL, band__c) AS sf_unmanaged_customer_band
        , CAST(IF(arr__c = 'null' OR arr__c = 'nan', 0, arr__c) AS DECIMAL(10,1)) AS sf_unmanaged_customer_arr
        , CAST(DATE_FORMAT(IF(global_renewal_date__c = 'null' OR global_renewal_date__c = 'nan', NULL, global_renewal_date__c), '%Y-%m-%d') AS DATE) AS sf_unmanaged_customer_renewal_date
        , CAST(IF(risk_rating__c = 'null' OR risk_rating__c = 'nan', 0, risk_rating__c) AS DECIMAL(10,1)) AS sf_unmanaged_customer_risk_rating
        , CAST(DATE_FORMAT(IF(customer_won_date__c = 'null' OR customer_won_date__c = 'nan', NULL, customer_won_date__c), '%Y-%m-%d') AS DATE) AS sf_unmanaged_customer_won_date
        , CAST(DATE_FORMAT(IF(customer_churned_date__c = 'null' OR customer_churned_date__c = 'nan', NULL, customer_churned_date__c), '%Y-%m-%d') AS DATE) AS sf_unmanaged_customer_churned_date
		, CAST(DATE_FORMAT(inserted_time, '%Y-%m-%d %H:%i:%S') AS DATETIME) AS record_inserted_time
        , ROW_NUMBER() OVER(PARTITION BY id ORDER BY id ASC, inserted_time DESC) AS row_no
    FROM customer_success_salesforce.sf_unmanaged_customers_raw
    INNER JOIN latest_records USING (id, inserted_time)
)
, unmanaged_customers_summary AS (
    SELECT
          sf_unmanaged_customer_account_id
        , sf_unmanaged_customer_account_name
        , sf_unmanaged_customer_domain
        , sf_unmanaged_customer_csm
        , sf_unmanaged_customer_account_stage
        , sf_unmanaged_customer_account_stage_detail
        , sf_unmanaged_customer_band
        , sf_unmanaged_customer_arr
        , sf_unmanaged_customer_renewal_date
        , sf_unmanaged_customer_risk_rating
        , sf_unmanaged_customer_won_date
        , sf_unmanaged_customer_churned_date
        , record_inserted_time
    FROM unmanaged_customers_records
    WHERE row_no = 1
)
SELECT
      sf_unmanaged_customer_account_id
    , sf_unmanaged_customer_account_name
    , sf_unmanaged_customer_domain
    , sf_unmanaged_customer_csm
    , sf_unmanaged_customer_account_stage
    , sf_unmanaged_customer_account_stage_detail
    , sf_unmanaged_customer_band
    , sf_unmanaged_customer_arr
    , sf_unmanaged_customer_renewal_date
    , sf_unmanaged_customer_risk_rating
    , sf_unmanaged_customer_won_date
    , sf_unmanaged_customer_churned_date
    , record_inserted_time
FROM unmanaged_customers_summary
;
