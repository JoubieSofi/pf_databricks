-- =============================================================================
-- 01_provisions_temp.sql  (Databricks)
--
-- STEP 1: Match every OLI from SFDC to provisions (p) using a 3-tier strategy.
-- This is an exploratory wide table — all candidate OLI↔provision pairings are
-- kept before any de-duplication or best-match ranking.
--
-- Match priority (captured in `match_type` column):
--   1a. deployment_id_product_code_acct  — provision has an SFDC deployment ID,
--           account matches, AND OLI base_product_code = deployment product code
--   1b. deployment_id_family_acct        — provision has an SFDC deployment ID,
--           account matches, AND product family maps to sn_sys_class_name
--           (OLI not already matched in 1a)
--   2.  account_family                   — account matches AND product family maps
--           to sn_sys_class_name, regardless of deployment ID
--           (OLI not already matched in 1a or 1b)
--   3.  no_match                         — no provision could be found for this OLI
--
-- Output → dev_dm.revops_analytics.provisions_temp
-- =============================================================================

CREATE OR REPLACE TABLE dev_dm.revops_analytics.provisions_temp AS

WITH

-- ── SFDC OLIs ─────────────────────────────────────────────────────────────────
sfdc_olis AS (
  SELECT
    o.id                                AS opp_id,
    ol.id                               AS oli_id,
    ol.pathfinder_enabled_c             AS pathfinder_enabled_ol,
    ql.pathfinder_enabled_c             AS pathfinder_enabled_ql,
    ql.id                               AS qli_id,
    ol.quantity                         AS quantity,
    ol.quote_line_acv_c                 AS quote_acv,
    ql.transaction_type_c,
    o.type                              AS opp_type,
    o.account_id,
    CAST(o.close_date AS DATE)          AS close_date,
    p.product_code,
    pp.product_code                     AS base_product_code,
    p.family                            AS product_family,
    CASE WHEN qr.sfdc_id IS NOT NULL
          OR r.sfdc_id  IS NOT NULL
         THEN 'reversal' ELSE NULL
    END                                 AS reversal_opp
  FROM dev_dm.salesforce_bronze.opportunity o
  LEFT JOIN dev_dm.salesforce_bronze.opportunity_line_item ol
    ON ol.opportunity_id = o.id
  LEFT JOIN dev_dm.salesforce_bronze.sbqq_quote_line_c ql
    ON ol.sbqq_quote_line_c = ql.id
  LEFT JOIN dev_dm.salesforce_bronze.sbqq_quote_c q
    ON ql.sbqq_quote_c = q.id
  LEFT JOIN dev_dm.salesforce_bronze.product_2 p
    ON ol.product_2_id = p.id
  LEFT JOIN (
    SELECT DISTINCT id, product_code FROM dev_dm.salesforce_bronze.product_2
  ) pp ON p.base_product_c = pp.id
  LEFT JOIN (
    SELECT DISTINCT sfdc_id FROM dev_dm.revops_analytics.reversals WHERE link_type = 'Opportunity'
  ) r  ON o.id  = r.sfdc_id
  LEFT JOIN (
    SELECT DISTINCT sfdc_id FROM dev_dm.revops_analytics.reversals WHERE link_type = 'Quote'
  ) qr ON ql.id = qr.sfdc_id
  WHERE o.stage_name    = '8 - Closed Won'
    AND o.close_date    > '2022-12-31'
    AND q.sbqq_primary_c = 'true'
    AND ql.sbqq_charge_type_c = 'Recurring'
    AND p.base_product_c IS NOT NULL
    AND p.arrclass_c    = 'SaaS'
    AND p.family IN (
      'Privileged Remote Access',
      'Password Safe',
      'Remote Support',
      'PM for Unix and Linux Servers',
      'PM for Desktops',
      'Entitle Platform',
      'PM for Windows Servers',
      'Workforce Passwords',
      'Password Safe with PRA',
      'Identity Security Insights'
    )
),

-- ── Raw provisions (union of all 7 ServiceNow product tables) ─────────────────
provi AS (
  SELECT
    ps.servicenow_cmdb_ci_password_safe_cloud_resource_group_sys_id                    AS provision_sys_id,
    ps.servicenow_cmdb_ci_password_safe_cloud_resource_group_pathfinder_enabled        AS sn_pathfinder_enabled,
    ps.servicenow_cmdb_ci_password_safe_cloud_resource_group_used_for                  AS sn_used_for,
    ps.servicenow_cmdb_ci_password_safe_cloud_resource_group_install_status            AS sn_install_status,
    ps.servicenow_cmdb_ci_password_safe_cloud_resource_group_account_value             AS sn_acct_id,
    ps.servicenow_cmdb_ci_password_safe_cloud_resource_group_model_category_value      AS sn_base_id,
    ps.servicenow_cmdb_ci_password_safe_cloud_resource_group_sys_class_name            AS sn_sys_class_name,
    ps.servicenow_cmdb_ci_password_safe_cloud_resource_group_sys_created_on_date       AS sys_created_date,
    ps.servicenow_cmdb_ci_password_safe_cloud_resource_group_sys_updated_on_date       AS sys_updated_date,
    ps.servicenow_cmdb_ci_password_safe_cloud_resource_group_start_date                AS start_date,
    split_part(ps.servicenow_cmdb_ci_password_safe_cloud_resource_group_fqdn, '.', 1) AS u_resource_key
  FROM prod_dm.alteryx.alteryx_servicenow_u_cmdb_ci_password_safe_cloud_resource_group ps
  WHERE ps.servicenow_cmdb_ci_password_safe_cloud_resource_group_sys_id IS NOT NULL

  UNION ALL

  SELECT
    pm.servicenow_cmdb_ci_privilege_management_cloud_sys_id,
    pm.servicenow_cmdb_ci_privilege_management_cloud_pathfinder_enabled,
    pm.servicenow_cmdb_ci_privilege_management_cloud_used_for,
    pm.servicenow_cmdb_ci_privilege_management_cloud_install_status,
    pm.servicenow_cmdb_ci_privilege_management_cloud_account_value,
    pm.servicenow_cmdb_ci_privilege_management_cloud_model_id_value,
    pm.servicenow_cmdb_ci_privilege_management_cloud_sys_class_name,
    pm.servicenow_cmdb_ci_privilege_management_cloud_sys_created_on_date,
    pm.servicenow_cmdb_ci_privilege_management_cloud_sys_updated_on_date,
    pm.servicenow_cmdb_ci_privilege_management_cloud_start_date,
    split_part(pm.servicenow_cmdb_ci_privilege_management_cloud_fqdn, '.', 1)
  FROM prod_dm.alteryx.alteryx_servicenow_u_cmdb_ci_privilege_management_cloud pm
  WHERE pm.servicenow_cmdb_ci_privilege_management_cloud_sys_id IS NOT NULL

  UNION ALL

  SELECT
    ra.servicenow_cmdb_ci_privileged_remote_access_cloud_instance_sys_id,
    ra.servicenow_cmdb_ci_privileged_remote_access_cloud_instance_pathfinder_enabled,
    ra.servicenow_cmdb_ci_privileged_remote_access_cloud_instance_used_for,
    ra.servicenow_cmdb_ci_privileged_remote_access_cloud_instance_install_status,
    ra.servicenow_cmdb_ci_privileged_remote_access_cloud_instance_account_value,
    ra.servicenow_cmdb_ci_privileged_remote_access_cloud_instance_model_id_value,
    ra.servicenow_cmdb_ci_privileged_remote_access_cloud_instance_sys_class_name,
    ra.servicenow_cmdb_ci_privileged_remote_access_cloud_instance_sys_created_on_date,
    ra.servicenow_cmdb_ci_privileged_remote_access_cloud_instance_sys_updated_on_date,
    ra.servicenow_cmdb_ci_privileged_remote_access_cloud_instance_start_date,
    split_part(ra.servicenow_cmdb_ci_privileged_remote_access_cloud_instance_fqdn, '.', 1)
  FROM prod_dm.alteryx.alteryx_servicenow_u_cmdb_ci_privileged_remote_access_cloud_instance ra
  WHERE ra.servicenow_cmdb_ci_privileged_remote_access_cloud_instance_sys_id IS NOT NULL

  UNION ALL

  SELECT
    rs.servicenow_cmdb_ci_remote_support_cloud_instance_sys_id,
    rs.servicenow_cmdb_ci_remote_support_cloud_instance_pathfinder_enabled,
    rs.servicenow_cmdb_ci_remote_support_cloud_instance_used_for,
    rs.servicenow_cmdb_ci_remote_support_cloud_instance_install_status,
    rs.servicenow_cmdb_ci_remote_support_cloud_instance_account_value,
    rs.servicenow_cmdb_ci_remote_support_cloud_instance_model_id_value,
    rs.servicenow_cmdb_ci_remote_support_cloud_instance_sys_class_name,
    rs.servicenow_cmdb_ci_remote_support_cloud_instance_sys_created_on_date,
    rs.servicenow_cmdb_ci_remote_support_cloud_instance_sys_updated_on_date,
    rs.servicenow_cmdb_ci_remote_support_cloud_instance_start_date,
    split_part(rs.servicenow_cmdb_ci_remote_support_cloud_instance_fqdn, '.', 1)
  FROM prod_dm.alteryx.alteryx_servicenow_u_cmdb_ci_remote_support_cloud_instance rs
  WHERE rs.servicenow_cmdb_ci_remote_support_cloud_instance_sys_id IS NOT NULL

  UNION ALL

  SELECT
    ep.servicenow_entitle_provisions_sys_id,
    ep.servicenow_entitle_provisions_u_pathfinder_enabled,
    ep.servicenow_entitle_provisions_u_used_for,
    ep.servicenow_entitle_provisions_install_status,
    ep.servicenow_entitle_provisions_u_account_value,
    ep.servicenow_entitle_provisions_model_number,
    ep.servicenow_entitle_provisions_sys_class_name,
    ep.servicenow_entitle_provisions_sys_created_on,
    ep.servicenow_entitle_provisions_sys_updated_on,
    ep.servicenow_entitle_provisions_start_date,
    ep.servicenow_entitle_provisions_u_resource_key
  FROM prod_dm.alteryx.alteryx_servicenow_u_entitle_provisions ep
  WHERE ep.servicenow_entitle_provisions_sys_id IS NOT NULL

  UNION ALL

  SELECT
    i.servicenow_is_platform_provisions_sys_id,
    i.servicenow_is_platform_provisions_pathfinder_enabled,
    i.servicenow_is_platform_provisions_used_for,
    i.servicenow_is_platform_provisions_install_status,
    i.servicenow_is_platform_provisions_account_value,
    NULL AS sn_base_id,
    i.servicenow_is_platform_provisions_sys_class_name,
    i.servicenow_is_platform_provisions_sys_created_on_date,
    i.servicenow_is_platform_provisions_sys_updated_on_date,
    i.servicenow_is_platform_provisions_start_date,
    i.servicenow_is_platform_provisions_resource_key
  FROM prod_dm.alteryx.alteryx_servicenow_u_is_platform_provisions i
  WHERE i.servicenow_is_platform_provisions_sys_id IS NOT NULL

  UNION ALL

  SELECT
    pm.servicenow_pmlc_platform_provisions_sys_id,
    pm.servicenow_pmlc_platform_provisions_u_pathfinder_enabled,
    pm.servicenow_pmlc_platform_provisions_used_for,
    pm.servicenow_pmlc_platform_provisions_install_status,
    pm.servicenow_pmlc_platform_provisions_u_account_value,
    pm.servicenow_pmlc_platform_provisions_model_number,
    pm.servicenow_pmlc_platform_provisions_sys_class_name,
    pm.servicenow_pmlc_platform_provisions_sys_created_date,
    pm.servicenow_pmlc_platform_provisions_sys_updated_date,
    pm.servicenow_pmlc_platform_provisions_start_date,
    pm.servicenow_pmlc_platform_provisions_u_resource_key
  FROM prod_dm.alteryx.alteryx_servicenow_u_pmlc_platform_provisions pm
  WHERE pm.servicenow_pmlc_platform_provisions_sys_id IS NOT NULL
),

-- ── Provisions enriched with SFDC account ID and pathfinder deployment link ───
provisions AS (
  SELECT
    m.salesforce_account_id                                                    AS sfdc_acct_id,
    p.provision_sys_id,
    p.sn_pathfinder_enabled,
    p.sn_used_for,
    p.sn_install_status,
    p.sn_acct_id,
    p.sn_base_id,
    p.sn_sys_class_name,
    p.sys_created_date,
    p.sys_updated_date,
    p.start_date,
    p.u_resource_key,
    -- Deployment link: not null means this provision is tracked in SFDC Pathfinder
    pf.salesforce_pathfinder_deployment_service_now_cloud_instance_id_c        AS sfdc_pf_deployment_id,
    pf.salesforce_pathfinder_deployment_id                                     AS sfdc_pathfinder_deployment_id,
    pf.salesforce_pathfinder_deployment_product_code_c                         AS sfdc_deployment_product_code
  FROM provi p
  LEFT JOIN prod_dm.alteryx.alteryx_master_account m
    ON p.sn_acct_id = m.servicenow_customer_account_sys_id
  LEFT JOIN prod_dm.alteryx.alteryx_salesforce_pathfinder_deployment_c pf
    ON pf.salesforce_pathfinder_deployment_service_now_cloud_instance_id_c = p.provision_sys_id
),

-- ── Lookup: sn_sys_class_name ↔ product_family (from sys_class_name_mapping) ──
sys_class_lookup AS (
  SELECT sn_sys_class_name, product_family
  FROM (VALUES
    ('u_cmdb_ci_password_safe_cloud_resource_group',      'Password Safe'),
    ('u_cmdb_ci_password_safe_cloud_resource_group',      'Password Safe with PRA'),
    ('u_cmdb_ci_privilege_management_cloud',              'PM for Desktops'),
    ('u_cmdb_ci_privilege_management_cloud',              'PM for Windows Servers'),
    ('u_cmdb_ci_privileged_remote_access_cloud_instance', 'Password Safe with PRA'),
    ('u_cmdb_ci_privileged_remote_access_cloud_instance', 'Privileged Remote Access'),
    ('u_cmdb_ci_remote_support_cloud_instance',           'Remote Support'),
    ('u_entitle_provisions',                              'Entitle Platform'),
    ('u_is_platform_provisions',                          'Identity Security Insights'),
    ('u_pmlc_platform_provisions',                        'PM for Unix and Linux Servers')
  ) AS t(sn_sys_class_name, product_family)
),

-- =============================================================================
-- MATCH 1A: SFDC deployment ID exists + account match + product code match
-- =============================================================================
match_1a AS (
  SELECT
    o.oli_id,
    o.opp_id,
    o.qli_id,
    o.account_id,
    o.base_product_code,
    o.product_family,
    o.product_code,
    o.pathfinder_enabled_ol,
    o.pathfinder_enabled_ql,
    o.transaction_type_c,
    o.opp_type,
    o.close_date,
    o.quantity,
    o.quote_acv,
    o.reversal_opp,
    p.provision_sys_id,
    p.sfdc_acct_id,
    p.sfdc_pf_deployment_id,
    p.sfdc_pathfinder_deployment_id,
    p.sfdc_deployment_product_code,
    p.sn_pathfinder_enabled,
    p.sn_used_for,
    p.sn_install_status,
    p.sn_sys_class_name,
    p.sn_base_id,
    p.u_resource_key,
    p.sys_created_date,
    p.sys_updated_date,
    p.start_date,
    'deployment_id_product_code_acct'   AS match_type
  FROM sfdc_olis o
  INNER JOIN provisions p
    ON  p.sfdc_pf_deployment_id IS NOT NULL
    AND p.sfdc_acct_id               = o.account_id
    AND p.sfdc_deployment_product_code = o.base_product_code
),

-- =============================================================================
-- MATCH 1B: SFDC deployment ID exists + account match + family lookup match
-- (OLI must not have already matched in 1a)
-- =============================================================================
match_1b AS (
  SELECT
    o.oli_id,
    o.opp_id,
    o.qli_id,
    o.account_id,
    o.base_product_code,
    o.product_family,
    o.product_code,
    o.pathfinder_enabled_ol,
    o.pathfinder_enabled_ql,
    o.transaction_type_c,
    o.opp_type,
    o.close_date,
    o.quantity,
    o.quote_acv,
    o.reversal_opp,
    p.provision_sys_id,
    p.sfdc_acct_id,
    p.sfdc_pf_deployment_id,
    p.sfdc_pathfinder_deployment_id,
    p.sfdc_deployment_product_code,
    p.sn_pathfinder_enabled,
    p.sn_used_for,
    p.sn_install_status,
    p.sn_sys_class_name,
    p.sn_base_id,
    p.u_resource_key,
    p.sys_created_date,
    p.sys_updated_date,
    p.start_date,
    'deployment_id_family_acct'         AS match_type
  FROM sfdc_olis o
  INNER JOIN provisions p
    ON  p.sfdc_pf_deployment_id IS NOT NULL
    AND p.sfdc_acct_id = o.account_id
  INNER JOIN sys_class_lookup lk
    ON  lk.sn_sys_class_name = p.sn_sys_class_name
    AND lk.product_family    = o.product_family
  WHERE o.oli_id NOT IN (SELECT oli_id FROM match_1a)
),

-- =============================================================================
-- MATCH 2: Account match + family lookup (no deployment ID requirement)
-- (OLI must not have already matched in 1a or 1b)
-- =============================================================================
match_2 AS (
  SELECT
    o.oli_id,
    o.opp_id,
    o.qli_id,
    o.account_id,
    o.base_product_code,
    o.product_family,
    o.product_code,
    o.pathfinder_enabled_ol,
    o.pathfinder_enabled_ql,
    o.transaction_type_c,
    o.opp_type,
    o.close_date,
    o.quantity,
    o.quote_acv,
    o.reversal_opp,
    p.provision_sys_id,
    p.sfdc_acct_id,
    p.sfdc_pf_deployment_id,
    p.sfdc_pathfinder_deployment_id,
    p.sfdc_deployment_product_code,
    p.sn_pathfinder_enabled,
    p.sn_used_for,
    p.sn_install_status,
    p.sn_sys_class_name,
    p.sn_base_id,
    p.u_resource_key,
    p.sys_created_date,
    p.sys_updated_date,
    p.start_date,
    'account_family'                    AS match_type
  FROM sfdc_olis o
  INNER JOIN provisions p
    ON  p.sfdc_acct_id = o.account_id
  INNER JOIN sys_class_lookup lk
    ON  lk.sn_sys_class_name = p.sn_sys_class_name
    AND lk.product_family    = o.product_family
  WHERE o.oli_id NOT IN (SELECT oli_id FROM match_1a)
    AND o.oli_id NOT IN (SELECT oli_id FROM match_1b)
),

-- =============================================================================
-- NO MATCH: OLIs with no provision found across any match tier
-- =============================================================================
no_match AS (
  SELECT
    o.oli_id,
    o.opp_id,
    o.qli_id,
    o.account_id,
    o.base_product_code,
    o.product_family,
    o.product_code,
    o.pathfinder_enabled_ol,
    o.pathfinder_enabled_ql,
    o.transaction_type_c,
    o.opp_type,
    o.close_date,
    o.quantity,
    o.quote_acv,
    o.reversal_opp,
    NULL  AS provision_sys_id,
    NULL  AS sfdc_acct_id,
    NULL  AS sfdc_pf_deployment_id,
    NULL  AS sfdc_pathfinder_deployment_id,
    NULL  AS sfdc_deployment_product_code,
    NULL  AS sn_pathfinder_enabled,
    NULL  AS sn_used_for,
    NULL  AS sn_install_status,
    NULL  AS sn_sys_class_name,
    NULL  AS sn_base_id,
    NULL  AS u_resource_key,
    NULL  AS sys_created_date,
    NULL  AS sys_updated_date,
    NULL  AS start_date,
    'no_match'                          AS match_type
  FROM sfdc_olis o
  WHERE o.oli_id NOT IN (SELECT oli_id FROM match_1a)
    AND o.oli_id NOT IN (SELECT oli_id FROM match_1b)
    AND o.oli_id NOT IN (SELECT oli_id FROM match_2)
)

-- =============================================================================
-- FINAL UNION — all OLIs with their best available provision matches
-- =============================================================================
SELECT * FROM match_1a
UNION ALL
SELECT * FROM match_1b
UNION ALL
SELECT * FROM match_2
UNION ALL
SELECT * FROM no_match;
