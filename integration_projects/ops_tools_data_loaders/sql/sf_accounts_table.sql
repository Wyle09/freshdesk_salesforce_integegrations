DROP TABLE IF EXISTS customer_success_salesforce.sf_accounts;

CREATE TABLE customer_success_salesforce.sf_accounts AS
WITH customer_accounts AS (
	SELECT
		  sf_managed_customer_account_name AS account_name
        , sf_managed_customer_account_id AS account_id
        , sf_managed_customer_csm AS assigned_csm_se
        , 'Managed Customer' AS account_type
        , sf_managed_customer_arr AS arr
        , sf_managed_customer_band AS band
        , sf_managed_customer_risk_rating AS risk_rating
        , sf_managed_customer_renewal_date AS renewal_date
        , sf_managed_customer_account_stage AS account_stage
        , sf_managed_customer_account_stage_detail AS account_stage_detail
        , sf_managed_customer_won_date AS customer_won_date
        , sf_managed_customer_domain AS domain
        , record_inserted_time
    FROM customer_success_salesforce.sf_managed_customers

    UNION ALL

	SELECT
		  sf_unmanaged_customer_account_name AS account_name
        , sf_unmanaged_customer_account_id AS account_id
        , sf_unmanaged_customer_csm AS assigned_csm_se
        , 'Unmanaged Customer' AS account_type
        , sf_unmanaged_customer_arr AS arr
        , sf_unmanaged_customer_band AS band
        , sf_unmanaged_customer_risk_rating AS risk_rating
        , sf_unmanaged_customer_renewal_date AS renewal_date
        , sf_unmanaged_customer_account_stage AS account_stage
        , sf_unmanaged_customer_account_stage_detail AS account_stage_detail
        , sf_unmanaged_customer_won_date AS customer_won_date
        , sf_unmanaged_customer_domain AS domain
        , record_inserted_time
    FROM customer_success_salesforce.sf_unmanaged_customers
)
, customer_accounts_dedup AS (
	SELECT
		  account_name
        , account_id
		, assigned_csm_se
        , account_type
        , arr
        , band
        , risk_rating
        , renewal_date
        , account_stage
        , account_stage_detail
        , customer_won_date
        , domain
        , ROW_NUMBER() OVER(PARTITION BY account_name ORDER BY record_inserted_time DESC) AS row_no
    FROM customer_accounts
)

, opportunities AS (
	SELECT
		  sf_prospects_account_name AS account_name
        , sf_prospects_account_id AS account_id
        , sf_prospects_se AS assigned_csm_se
        , 'Prospect' AS account_type
        , sf_prospects_arr AS arr
        , sf_prospects_band AS band
        , NULL AS risk_rating
        , NULL AS renewal_date
        , sf_prospects_account_stage AS account_stage
        , sf_prospects_account_stage_detail AS account_stage_detail
        , NULL AS customer_won_date
        , sf_prospects_domain AS domain
        , ROW_NUMBER() OVER(PARTITION BY sf_prospects_account_name ORDER BY sf_prospects_close_date DESC) AS row_no
    FROM customer_success_salesforce.sf_prospects
    WHERE sf_prospects_account_name NOT IN (SELECT account_name FROM customer_accounts)
)
, combine_data AS (
	SELECT
		  account_name
        , account_id
		, assigned_csm_se
        , account_type
        , arr
        , band
        , risk_rating
        , renewal_date
        , account_stage
        , account_stage_detail
        , customer_won_date
        , domain
    FROM customer_accounts_dedup
    WHERE row_no = 1

    UNION

    SELECT
		  account_name
        , account_id
		, assigned_csm_se
        , account_type
        , arr
        , band
        , risk_rating
        , renewal_date
        , account_stage
        , account_stage_detail
        , customer_won_date
        , domain
    FROM opportunities
    WHERE row_no = 1
)
SELECT
	  account_name
    , account_id
	, assigned_csm_se
	, account_type
	, arr
	, band
	, risk_rating
	, renewal_date
	, account_stage
	, account_stage_detail
    , customer_won_date
	, domain
FROM combine_data
ORDER BY
      account_name
;
