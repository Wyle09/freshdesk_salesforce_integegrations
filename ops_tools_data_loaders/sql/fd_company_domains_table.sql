DROP TABLE IF EXISTS customer_success_freshdesk.fd_company_domains;

CREATE TABLE customer_success_freshdesk.fd_company_domains AS
WITH fd_company_domains AS (
	SELECT
		  company_id
		, company_name
		, d.domain
		, '1_fd_companies' AS source
	FROM
		  customer_success_freshdesk.fd_companies
		, JSON_TABLE(company_domains, '$[*]' COLUMNS (domain VARCHAR(64) PATH '$')) AS d
)
, fd_contacts_domains AS (
	SELECT DISTINCT
		  company_id
		, company_name
        , SUBSTRING_INDEX(contact_email, '@', -1) AS domain
        , '2_fd_contacts' AS source
    FROM customer_success_freshdesk.fd_contacts
    INNER JOIN customer_success_freshdesk.fd_companies
		ON contact_company_id = company_id
	WHERE company_name IS NOT NULL
)
, sf_account_domains AS (
	SELECT
		  fd_companies.company_id
		, account_name AS company_name
		, domain
        , '3_sf_accounts_view' AS source
    FROM customer_success_salesforce.sf_accounts
    INNER JOIN customer_success_freshdesk.fd_companies
		ON account_name = company_name
)
, sf_contacts_domains AS (
	SELECT DISTINCT
		  fd_companies.company_id
		, contact_account_name AS company_name
		, SUBSTRING_INDEX(contact_email, '@', -1) AS domain
        , '4_sf_contacts' AS source
    FROM customer_success_salesforce.sf_contacts
    INNER JOIN customer_success_freshdesk.fd_companies
		ON contact_account_name = company_name
)
, all_combine_domains AS (
	SELECT
		  company_id
		, company_name
        , domain
        , source
    FROM fd_company_domains

    UNION ALL

	SELECT
		  company_id
		, company_name
        , domain
        , source
    FROM fd_contacts_domains

	UNION ALL

	SELECT
		  company_id
		, company_name
        , domain
        , source
    FROM sf_account_domains

    UNION ALL

	SELECT
		  company_id
		, company_name
        , domain
        , source
    FROM sf_contacts_domains
)
, dedup_domains AS (
	SELECT
		  company_id
		, company_name
        , domain
        , source
        , ROW_NUMBER() OVER (PARTITION BY company_id, domain ORDER BY source ASC) AS domain_row
    FROM all_combine_domains
    WHERE domain IS NOT NULL
    AND LOWER(domain) NOT IN ('gmail.com', 'outlook.com', 'customer_success_freshdesk.com', 'yahoo.com', 'icloud.com')
    AND LOWER(company_name) NOT LIKE '%freshdesk%'
)
-- Check if domains exists at multiple companies, if yes default domain to the company
-- name that match. Domains must be unique for each company to avoid errors in
-- Freshdesk
, unique_domains_check AS (
	SELECT
		  company_id
		, company_name
		, IF(domain LIKE '%null%', NULL, domain) AS domain
        , source
		, ROW_NUMBER() OVER (PARTITION BY domain ORDER BY IF(LOWER(company_name) LIKE CONCAT('%', SUBSTRING_INDEX(domain, '.', 1), '%'), 1, 2)) AS row_no
    FROM dedup_domains
    WHERE domain_row = 1
)
, unique_domains AS (
	SELECT DISTINCT
		  company_id
		, company_name
		, JSON_ARRAYAGG(domain) AS domains
    FROM unique_domains_check
    WHERE row_no = 1
    AND domain IS NOT NULL
    GROUP BY
		  company_id
		, company_name
)
SELECT
      company_id
    , company_name
    , domains
FROM unique_domains
ORDER BY
	  company_name
;
