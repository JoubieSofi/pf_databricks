-- =============================================================================
-- 03_pf_oli_classification.sql  (Databricks)
--
-- Classifies every OLI in provisions_temp into one of three categories:
--   'Decom/RR'       — OLI is a reversal or tied to a decommissioned provision
--   'PF Provisioned' — OLI has a confirmed active PF provision (with overrides)
--   'No PF Provision'— OLI has no qualifying provision
--
-- Logic applied per-row, then de-duplicated to one row per OLI (MAX across
-- all candidate provision rows for that OLI).
--
-- Column definitions:
--   OLI_ID         — the OLI identifier
--   Decom_RR       — 1 when the OLI is a reversal or decommissioned, NULL otherwise
--   PF_Provisioned — 1 when the OLI has a confirmed PF provision, NULL otherwise
--
-- Only rows classified as 'Decom/RR' or 'PF Provisioned' are included;
-- 'No PF Provision' rows are excluded from the output.
--
-- Output → dev_dm.revops_analytics.pf_oli_classification
-- =============================================================================

CREATE OR REPLACE TABLE dev_dm.revops_analytics.pf_oli_classification
 AS

WITH

-- ── Step 1: Row-level scoring ─────────────────────────────────────────────────
-- Apply each flag to every row in provisions_temp (still many rows per OLI).
row_scoring AS (
  SELECT
    oli_id,
    product_family,
    close_date,
    match_type,
    sn_pathfinder_enabled,
    sn_used_for,
    sn_install_status,
    reversal_opp,
    decom_resource,

    -- PF Provisioned: active PF-enabled provision OR explicit deployment-linked match
    CASE
      WHEN (sn_pathfinder_enabled = TRUE AND sn_install_status = '1' AND sn_used_for IN ('Staging', 'Production'))
        OR match_type LIKE '%deployment%'
      THEN oli_id
      ELSE NULL
    END AS pf_provisioned_oli,

    -- Decom/RR: OLI is a reversal booking OR its provision appears in decom tracking
    CASE
      WHEN reversal_opp = 'reversal' OR decom_resource IS NOT NULL
      THEN 1 ELSE 0
    END AS is_decom_rr

  FROM dev_dm.revops_analytics.provisions_temp_4726
  WHERE opp_type != 'Renewal'
    AND (quantity > 0.1 OR quantity < 0.1)
),

-- ── Step 2: Apply overrides ───────────────────────────────────────────────────
-- Identity Security Insights: always treated as PF Provisioned (product does
--   not follow standard SN provisioning flow).
-- Entitle Platform: treated as PF Provisioned for deals closed after 2025-01-01
--   (new provisioning workflow adopted at that date).
-- All other OLIs: carry forward pf_provisioned_oli from Step 1.
override_scoring AS (
  SELECT
    *,
    CASE
      WHEN product_family LIKE '%Insights%'
        OR (product_family LIKE '%Entitle%' AND close_date > '2025-01-01')
      THEN oli_id
      ELSE pf_provisioned_oli
    END AS pf_provisioned_override
  FROM row_scoring
),

-- ── Step 3: De-duplicate to one row per OLI ──────────────────────────────────
-- An OLI may have many candidate provision rows. Take the most favorable signal:
--   - if ANY row is Decom/RR → the OLI is Decom/RR
--   - if ANY row has a qualifying override → the OLI is PF Provisioned
deduped AS (
  SELECT
    oli_id,
    MAX(is_decom_rr)            AS is_decom_rr,
    MAX(pf_provisioned_override) AS pf_provisioned_override
  FROM override_scoring
  GROUP BY oli_id
),

-- ── Step 4: Apply final category label ───────────────────────────────────────
categorized AS (
  SELECT
    oli_id,
    is_decom_rr,
    pf_provisioned_override,
    CASE
      WHEN is_decom_rr = 1                     THEN 'Decom/RR'
      WHEN pf_provisioned_override IS NOT NULL  THEN 'PF Provisioned'
      ELSE                                           'No PF Provision'
    END AS oli_output
  FROM deduped
)

-- ── Final output: one row per OLI ────────────────────────────────────────────
SELECT
  oli_id                                      AS OLI_ID,
  CASE WHEN oli_output = 'Decom/RR'
       THEN 1 END                             AS Decom_RR,
  CASE WHEN oli_output = 'PF Provisioned'
       THEN 1 END                             AS PF_Provisioned
FROM categorized
WHERE oli_output IN ('Decom/RR', 'PF Provisioned')
ORDER BY oli_id;
