-- =============================================================================
-- 05_arr_qoq_comparison.sql  (Databricks)
--
-- Compares ARR by account and product family between Q1 2026 and Q2 2026
-- to identify accounts where ARR is increasing or decreasing quarter-over-quarter.
--
-- Key business rule:
--   Deals closed in 2026 that appear in Q1 ARR are excluded from the Q1
--   baseline.  These represent new bookings whose ARR won't materialise
--   until a future period (e.g. Q1 2027), so subtracting them would create
--   a false "decrease" signal.
--
-- Provisioning status is sourced from pf_oli_classification (step 03)
-- rather than re-derived inline.
--
-- Source tables:
--   dev_dm.revops_analytics.arr_by_product_w_oli8s  (ARR by quarter)
--   dev_dm.revops_analytics.provisions_temp         (OLI → provision matches)
--   dev_dm.revops_analytics.pf_oli_classification   (OLI provisioning class)
-- =============================================================================

WITH

-- ── Lookup: long product-family names → short display names ─────────────────
pf_family_lookup AS (
  SELECT product_family, pf_short
  FROM (VALUES
    ('Privileged Remote Access',      'PRA'),
    ('Password Safe',                 'Password Safe'),
    ('Password Safe with PRA',        'PS with PRA'),
    ('PM for Desktops',               'PM for Desktops'),
    ('PM for Windows Servers',        'PM for Windows Servers'),
    ('PM for Unix and Linux Servers', 'PM for UL Servers'),
    ('Remote Support',                'Remote Support'),
    ('Entitle Platform',              'Entitle'),
    ('Identity Security Insights',    'ISI'),
    ('Workforce Passwords',           'Workforce Passwords')
  ) AS t(product_family, pf_short)
),

-- ── Provisioned accounts: one row per account + product family ──────────────
-- Joins provisions_temp to pf_oli_classification to pick up the classification
-- without re-deriving the scoring logic.  Collects OLI-id lists for reference.
provisioned_accounts AS (
  SELECT
    pt.account_id,
    COALESCE(lk.pf_short, pt.product_family) AS product_family,
    ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(
      CASE WHEN cl.oli_output = 'Decom/RR'
           THEN pt.oli_id END
    )), ', ')                                 AS decom_rr_olis,
    ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(
      CASE WHEN cl.oli_output = 'PF Provisioned'
           THEN pt.oli_id END
    )), ', ')                                 AS provision_pf_olis,
    ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(
      CAST(pt.close_date AS STRING)
    )), ', ')                                 AS close_dates
  FROM dev_dm.revops_analytics.provisions_temp            pt
  INNER JOIN dev_dm.revops_analytics.pf_oli_classification cl
    ON cl.oli_id = pt.oli_id
  LEFT JOIN pf_family_lookup lk
    ON lk.product_family = pt.product_family
  WHERE pt.opp_type != 'Renewal'
    AND cl.oli_output IN ('PF Provisioned', 'Decom/RR')
  GROUP BY pt.account_id,
           COALESCE(lk.pf_short, pt.product_family)
),

-- ── ARR base: Q1 2026 and Q2 2026 rows from the ARR dataset ────────────────
arr_base AS (
  SELECT
    `Account Name`,
    regexp_extract(`Account Link`, '[^/]+$')  AS account_id,
    `Opportunity ID`,
    `Opportunity Product ID`,
    `Product Family Short`                    AS product_family,
    `Close Date`,
    CAST(`ARR: End of Period Adj (USD)` AS DOUBLE)
                                              AS arr,
    CASE
      WHEN `Year-Quarter` = '2026 | Q1' THEN 1
      WHEN `Year-Quarter` = '2026 | Q2' THEN 2
    END                                       AS quarter_ord
  FROM `dev_dm`.`revops_analytics`.`arr_by_product_w_oli8s`
  WHERE `Year-Quarter` IN ('2026 | Q1', '2026 | Q2')
),

-- ── ARR signed: apply QoQ sign logic with new-deal exclusion ────────────────
-- Q2 ARR counts as positive (the "to" quarter).
-- Q1 ARR counts as negative (the "from" quarter) UNLESS the deal closed in
-- 2026 — those are new bookings whose ARR hasn't started yet, so subtracting
-- them would overstate a decrease.
arr_signed AS (
  SELECT
    a.`Account Name`,
    a.account_id,
    a.`Opportunity ID`,
    a.`Opportunity Product ID`,
    a.product_family,
    a.`Close Date`,
    a.quarter_ord,
    CASE
      WHEN a.quarter_ord = 1 AND YEAR(a.`Close Date`) != 2026
        THEN -1 * a.arr                       -- Q1: subtract (normal baseline)
      WHEN a.quarter_ord = 2
        THEN  1 * a.arr                       -- Q2: add
      ELSE  a.arr                             -- Q1 new-2026 deal: keep as-is
    END                                       AS arr_amt,
    p.provision_pf_olis,
    p.decom_rr_olis
  FROM arr_base a
  LEFT JOIN provisioned_accounts p
    ON  p.account_id    = a.account_id
    AND p.product_family = a.product_family
)

-- ── Final output: net ARR change per account / product / OLI ────────────────
SELECT
  `Account Name`,
  account_id,
  `Opportunity ID`,
  `Opportunity Product ID`,
  product_family,
  `Close Date`,
  YEAR(`Close Date`)                          AS close_year,
  decom_rr_olis,
  provision_pf_olis,
  MAX(quarter_ord)                            AS max_q,
  SUM(arr_amt)                                AS arr_diff_qoq
FROM arr_signed
GROUP BY
  `Account Name`,
  account_id,
  `Opportunity ID`,
  `Opportunity Product ID`,
  product_family,
  `Close Date`,
  YEAR(`Close Date`),
  decom_rr_olis,
  provision_pf_olis
ORDER BY arr_diff_qoq ASC, `Opportunity Product ID` ASC;
