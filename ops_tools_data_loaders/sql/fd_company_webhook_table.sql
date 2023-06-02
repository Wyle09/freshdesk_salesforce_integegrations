DROP TABLE IF EXISTS customer_success_freshdesk.fd_company_webhook;

CREATE TABLE customer_success_freshdesk.fd_company_webhook AS
WITH sf_accounts AS (
	SELECT
		  account_name
		, assigned_csm_se
        , account_type
        , CAST(arr AS UNSIGNED) AS arr
        , band
        , CAST(risk_rating AS SIGNED) AS risk_rating
        , renewal_date
        , account_stage
        , account_stage_detail
        , customer_won_date
    FROM customer_success_salesforce.sf_accounts
)
, combine_data AS (
	SELECT DISTINCT
		  fd_companies.company_id
		, fd_companies.company_name
		, IF(assigned_csm_se IS NULL, 'N/A', assigned_csm_se) AS assigned_csm_se
		, IF(account_type IS NULL, 'N/A', account_type) AS account_type
		, arr
		, IF(band IS NULL, 'N/A', band) AS band
		, risk_rating
		, IF(YEAR(renewal_date) < '2000', '2000-01-01', renewal_date) AS renewal_date
        , account_stage
        , account_stage_detail
        , customer_won_date
		, fd_company_domains.domains
        , JSON_OBJECT('note', '', 'domains', domains, 'renewal_date', renewal_date, 'custom_fields', JSON_OBJECT('account_type2', account_type, 'arr', arr, 'assigned_csmse2', assigned_csm_se, 'risk_rating', risk_rating, 'band2', band, 'customer_won_date', customer_won_date, 'account_stage', account_stage, 'account_stage_detail', account_stage_detail)) AS fields_to_update
	FROM customer_success_freshdesk.fd_companies
	INNER JOIN sf_accounts
		ON company_name = account_name
	LEFT JOIN customer_success_freshdesk.fd_company_domains
		ON fd_companies.company_id = fd_company_domains.company_id
)
SELECT
	  company_id
	, company_name
	, assigned_csm_se
	, account_type
	, arr
	, band
	, risk_rating
	, renewal_date
	, account_stage
	, account_stage_detail
	, customer_won_date
	, domains
    , fields_to_update
FROM combine_data
ORDER BY
	  company_name
;
