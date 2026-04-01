-- =============================================================================
-- 04_pf_master_dataset.sql  (Databricks)
--
-- OLI-centric master dataset consolidating provisioning state, decommission
-- status, and PF booking signals into a single flat table.
-- Must run after olis_provisions_bronze.sql and 03_builder_decom_analysis.sql.
--
-- Output → dev_dm.revops_analytics.pf_master_dataset
--
-- Source tables:
--   dev_dm.revops_analytics.olis_provisions_bronze        ✅ output of olis_provisions_bronze.sql
--   dev_dm.revops_analytics.builder_decom_analysis        ✅ output of step 03
--   dev_dm.revops_analytics.pf_reverse_rebooks
--   prod_dm.alteryx.alteryx_salesforce_sbqq_quote_c     (not in salesforce_bronze; using prod)
-- =============================================================================

CREATE OR REPLACE TABLE dev_dm.revops_analytics.pf_master_dataset AS

WITH

-- ── builder_decomms ────────────────────────────────────────────────────────
builder_decomms AS (
  SELECT
    provision_sys_id,
    MIN(DecommissionedAt) AS builder_decom_date,
    COUNT(*)              AS builder_decom_count
  FROM dev_dm.revops_analytics.builder_decom_analysis
  WHERE builder_decom_acct_product_match = 1
    AND provision_sys_id IS NOT NULL
  GROUP BY provision_sys_id
),

-- ── rr_reversals ───────────────────────────────────────────────────────────
-- Match OLIs to reverse rebook incidents (Reversal booking type only).
-- Join path: OLI → Opportunity (direct) OR OLI → Quote → Opportunity (via quote table).
rr_reversals AS (
  SELECT
    o.oli_id,
    COUNT(DISTINCT rr.incident_number) AS rr_incident_count,
    MAX(rr.incident_number)            AS rr_incident_number,
    MAX(rr.processed_as)               AS rr_processed_as
  FROM dev_dm.revops_analytics.olis_provisions_bronze o
  INNER JOIN dev_dm.revops_analytics.pf_reverse_rebooks rr
    ON  rr.booking_type = 'Reversal'
    AND (
          (rr.link_type = 'Opportunity' AND rr.sfdc_id = o.opp_id)
       OR (rr.link_type = 'Quote' AND EXISTS (
              SELECT 1
              FROM prod_dm.alteryx.alteryx_salesforce_sbqq_quote_c q
              WHERE q.salesforce_sbqq_quote_id = rr.sfdc_id
                AND q.SBQQ__Opportunity2__c    = o.opp_id
          ))
       )
  GROUP BY o.oli_id
)

-- =============================================================================
-- Final SELECT — one row per OLI
-- =============================================================================
SELECT

  -- ── IDENTIFIERS ────────────────────────────────────────────────────────────
  o.oli_id,
  o.qli_id,
  o.opp_id,
  o.sfdc_account_id,
  o.product_code,
  o.base_product_code,
  o.product_family,
  o.provision_sys_id,
  o.sfdc_prov_id,
  o.u_resource_key,

  -- ── BOOKING CONTEXT ────────────────────────────────────────────────────────
  o.close_date,
  o.oli_net_acv,
  o.oli_quantity,
  o.sfdc_sub_status           AS subscription_status,
  o.subscription_type,
  o.sfdc_contract_id          AS oli_contract_id,
  o.sfdc_subscription_id      AS oli_subscription_id,
  o.transaction_type_c        AS transaction_type,
  o.join_path                 AS match_type,

  -- ── PF BOOKING FLAGS ───────────────────────────────────────────────────────
  o.oli_pathfinder_enabled    AS oli_pathfinderenabled,
  o.qli_pathfinder_enabled    AS qli_pathfinderenabled,

  -- ── SNOW PROVISION STATE ───────────────────────────────────────────────────
  o.sn_pathfinder_enabled,
  o.sn_used_for,
  o.sn_install_status,
  o.sn_sys_class_name,
  o.sn_updated_date           AS sn_last_updated,

  -- ── PF PROVISIONING STATUS BUCKET ──────────────────────────────────────────
  CASE
    -- Negative ACV / quantity: credit or deprovisioning line
    WHEN o.oli_net_acv < 0 OR o.oli_quantity < 0
      THEN 'negative_oli'

    -- PF BOOKED + SFDC PROVISION
    WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = TRUE AND o.sfdc_prov_id IS NOT NULL
         AND o.sn_install_status = '1' AND lower(o.sn_used_for) = 'production'
         AND o.sn_pathfinder_enabled = TRUE
      THEN 'pf_booked_sfdc_snow_active_production'

    WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = TRUE AND o.sfdc_prov_id IS NOT NULL
         AND o.sn_install_status = '1' AND lower(o.sn_used_for) = 'production'
         AND COALESCE(o.sn_pathfinder_enabled, FALSE) <> TRUE
      THEN 'pf_booked_sfdc_snow_active_production_not_pf_enabled'

    WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = TRUE AND o.sfdc_prov_id IS NOT NULL
         AND o.sn_install_status = '1' AND lower(o.sn_used_for) = 'staging'
      THEN 'pf_booked_sfdc_snow_active_staging'

    WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = TRUE AND o.sfdc_prov_id IS NOT NULL
         AND o.sn_install_status = '1'
      THEN 'pf_booked_sfdc_snow_active_other'

    WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = TRUE AND o.sfdc_prov_id IS NOT NULL
         AND o.sn_install_status = '4'
      THEN 'pf_booked_sfdc_snow_deprovisioned'

    WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = TRUE AND o.sfdc_prov_id IS NOT NULL
         AND o.provision_sys_id IS NOT NULL AND o.sn_install_status IS NULL
      THEN 'pf_booked_sfdc_no_snow_record'

    -- PF BOOKED + SNOW ONLY
    WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = TRUE AND o.sfdc_prov_id IS NULL
         AND o.sn_install_status = '1' AND lower(o.sn_used_for) = 'production'
         AND o.sn_pathfinder_enabled = TRUE
      THEN 'pf_booked_snow_only_active_production'

    WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = TRUE AND o.sfdc_prov_id IS NULL
         AND o.sn_install_status = '1' AND lower(o.sn_used_for) = 'production'
      THEN 'pf_booked_snow_only_active_production_not_pf_enabled'

    WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = TRUE AND o.sfdc_prov_id IS NULL
         AND o.sn_install_status = '1'
      THEN 'pf_booked_snow_only_active_other'

    WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = TRUE AND o.sfdc_prov_id IS NULL
         AND o.sn_install_status = '4'
      THEN 'pf_booked_snow_only_deprovisioned'

    -- PF BOOKED + NO PROVISION
    WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = TRUE AND o.provision_sys_id IS NULL
      THEN 'pf_booked_unmatched'

    -- NOT PF BOOKED + HAS PROVISION
    WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = FALSE AND o.sfdc_prov_id IS NOT NULL
         AND o.sn_install_status = '1' AND lower(o.sn_used_for) = 'production'
         AND o.sn_pathfinder_enabled = TRUE
      THEN 'not_pf_booked_sfdc_snow_active_production'

    WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = FALSE AND o.sfdc_prov_id IS NULL
         AND o.sn_install_status = '1' AND lower(o.sn_used_for) = 'production'
         AND o.sn_pathfinder_enabled = TRUE
      THEN 'not_pf_booked_snow_only_active_production'

    WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = FALSE AND o.provision_sys_id IS NOT NULL
      THEN 'not_pf_booked_has_provision'

    -- NOT PF BOOKED + NO PROVISION
    WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = FALSE AND o.provision_sys_id IS NULL
      THEN 'not_pf_booked_unmatched'

    ELSE 'unknown'
  END AS pf_provision_status,

  -- ── DECOMMISSION FLAGS ─────────────────────────────────────────────────────
  CASE WHEN bd.provision_sys_id IS NOT NULL THEN 1 ELSE 0 END AS is_builder_decommed,
  bd.builder_decom_date,
  bd.builder_decom_count,

  CASE WHEN rr.oli_id IS NOT NULL THEN 1 ELSE 0 END AS is_rr_reversed,
  rr.rr_incident_number,
  rr.rr_incident_count,
  rr.rr_processed_as,

  CASE
    WHEN bd.provision_sys_id IS NOT NULL AND rr.oli_id IS NOT NULL THEN 'both'
    WHEN bd.provision_sys_id IS NOT NULL                           THEN 'builder'
    WHEN rr.oli_id IS NOT NULL                                     THEN 'reverse_rebook'
    ELSE 'none'
  END AS decomm_source,

  -- Snow still active after a decommission (either source)
  CASE
    WHEN (bd.provision_sys_id IS NOT NULL OR rr.oli_id IS NOT NULL)
         AND o.sn_install_status = '1'
         AND lower(o.sn_used_for) IN ('production', 'staging')
    THEN 1 ELSE 0
  END AS snow_stale_after_decom,

  -- Days since builder flagged decom but Snow is still active
  CASE
    WHEN bd.provision_sys_id IS NOT NULL
         AND o.sn_install_status = '1'
         AND bd.builder_decom_date IS NOT NULL
    THEN DATEDIFF(CURRENT_DATE(), CAST(bd.builder_decom_date AS DATE))
  END AS days_stale_since_builder_decom,

  -- ── ANOMALY FLAGS (0/1) ────────────────────────────────────────────────────
  CASE WHEN o.oli_net_acv < 0 AND o.sn_install_status = '1'
       THEN 1 ELSE 0 END AS flag_negative_acv_snow_active,

  CASE WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = TRUE
            AND o.provision_sys_id IS NULL
       THEN 1 ELSE 0 END AS flag_pf_booked_no_provision,

  CASE WHEN COALESCE(o.oli_pathfinder_enabled, FALSE) = TRUE
            AND o.sn_install_status = '4'
       THEN 1 ELSE 0 END AS flag_pf_booked_snow_deprovisioned,

  CASE WHEN o.oli_pathfinder_enabled IS NOT NULL
            AND o.qli_pathfinder_enabled IS NOT NULL
            AND o.oli_pathfinder_enabled <> o.qli_pathfinder_enabled
       THEN 1 ELSE 0 END AS flag_oli_qli_pf_mismatch,

  CASE WHEN o.sfdc_sub_status = 'Cancelled'
            AND o.sn_install_status = '1'
            AND bd.provision_sys_id IS NULL
            AND rr.oli_id IS NULL
       THEN 1 ELSE 0 END AS flag_cancelled_sub_snow_active_undecommed,

  -- Note: in bronze, sfdc_account_id is already SN-derived; mismatch would indicate
  -- a cross-account provision. Kept for structural parity with prior pipeline.
  0 AS flag_snow_acct_mismatch,

  CASE WHEN (bd.provision_sys_id IS NOT NULL OR rr.oli_id IS NOT NULL)
            AND COALESCE(o.oli_pathfinder_enabled, FALSE) = TRUE
       THEN 1 ELSE 0 END AS flag_decommed_still_pf_enabled

FROM dev_dm.revops_analytics.olis_provisions_bronze o
LEFT JOIN builder_decomms bd ON bd.provision_sys_id = o.provision_sys_id
LEFT JOIN rr_reversals rr    ON rr.oli_id           = o.oli_id;
