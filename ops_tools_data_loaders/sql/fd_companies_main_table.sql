SET FOREIGN_KEY_CHECKS=0;

-- Will need to drop the table, in case companies are deleted from Freshdesk.
DROP TABLE IF EXISTS customer_success_freshdesk.fd_companies;

CREATE TABLE IF NOT EXISTS customer_success_freshdesk.fd_companies (
      company_id VARCHAR(64)
    , company_name VARCHAR(256)
    , company_account_type VARCHAR(256)
    , company_arr DECIMAL(10,2)
    , company_assigned_csm_se VARCHAR(256)
    , company_renewal_date DATE
    , company_domains JSON
    , company_risk_rating DECIMAL(10,2)
    , company_band VARCHAR(32)
    , company_won_date DATE
    , company_account_stage VARCHAR(64)
    , company_account_stage_detail VARCHAR(128)
    , record_inserted_time DATETIME
    , last_update DATETIME
    , PRIMARY KEY(company_id)
);

REPLACE INTO customer_success_freshdesk.fd_companies
WITH latest_records AS (
	SELECT
		  id
		, MAX(updated_at) AS updated_at
    FROM customer_success_freshdesk.fd_companies_raw
    GROUP BY
		  id
)
, companies_records AS (
    SELECT
          SUBSTRING_INDEX(id, '.', 1) AS company_id
        , IF(name = 'null' OR name = 'nan', NULL, name) AS company_name
        , IF(custom_fields->>'$.account_type2' = 'null' OR custom_fields->>'$.account_type2' = 'nan', NULL, custom_fields->>'$.account_type2') AS company_account_type
        , CAST(IF(custom_fields->>'$.arr' = 'null' OR custom_fields->>'$.arr' = 'nan', 0, custom_fields->>'$.arr') AS DECIMAL(10,2)) AS company_arr
        , IF(custom_fields->>'$.assigned_csmse2' = 'null' OR custom_fields->>'$.assigned_csmse2' = 'nan', NULL, custom_fields->>'$.assigned_csmse2') AS company_assigned_csm_se
        , CAST(DATE_FORMAT(IF(renewal_date = 'null' OR renewal_date = 'nan', NULL, REPLACE(REPLACE(renewal_date, '"', ''), 'Z', '')), '%Y-%m-%d') AS DATE) AS company_renewal_date
        , CAST(IF(domains = '[]' OR domains = 'null' OR domains = 'nan', NULL, domains) AS JSON) AS company_domains
        , CAST(IF(custom_fields->>'$.risk_rating' = 'null' OR custom_fields->>'$.risk_rating' = 'nan', 0, custom_fields->>'$.risk_rating') AS DECIMAL(10,2)) AS company_risk_rating
        , IF(custom_fields->>'$.band2' = 'null' OR custom_fields->>'$.band2' = 'nan', NULL, custom_fields->>'$.band2') AS company_band
        , CAST(DATE_FORMAT(IF(custom_fields->>'$.customer_won_date' = 'null' OR custom_fields->>'$.customer_won_date' = 'nan', NULL, REPLACE(REPLACE(custom_fields->>'$.customer_won_date', '"', ''), 'Z', '')), '%Y-%m-%d') AS DATE) AS company_won_date
        , IF(custom_fields->>'$.account_stage' = 'null' OR custom_fields->>'$.account_stage' = 'nan', NULL, custom_fields->>'$.account_stage') AS company_account_stage
        , IF(custom_fields->>'$.account_stage_detail' = 'null' OR custom_fields->>'$.account_stage_detail' = 'nan', NULL, custom_fields->>'$.account_stage_detail') AS company_account_stage_detail
		, CAST(DATE_FORMAT(inserted_time, '%Y-%m-%d %H:%i:%S') AS DATETIME) AS record_inserted_time
		, CAST(DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%S') AS DATETIME) AS last_update
        , ROW_NUMBER() OVER(PARTITION BY id ORDER BY id ASC, updated_at DESC) AS row_no
    FROM customer_success_freshdesk.fd_companies_raw
    INNER JOIN latest_records USING (id, updated_at)
)
, companies_summary AS (
    SELECT
          company_id
        , company_name
        , company_account_type
        , company_arr
        , company_assigned_csm_se
        , company_renewal_date
        , company_domains
        , company_risk_rating
        , company_band
        , company_won_date
        , company_account_stage
        , company_account_stage_detail
        , record_inserted_time
        , last_update
    FROM companies_records
    WHERE row_no = 1
)
SELECT
      company_id
    , company_name
    , company_account_type
    , company_arr
    , company_assigned_csm_se
    , company_renewal_date
    , company_domains
    , company_risk_rating
    , company_band
    , company_won_date
    , company_account_stage
    , company_account_stage_detail
    , record_inserted_time
    , last_update
FROM companies_summary
;

SET FOREIGN_KEY_CHECKS=1;
