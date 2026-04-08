-- =============================================================================
-- 02_oli_comparison.sql  (Databricks)
--
-- Compares unique OLI counts between two provisions_temp snapshots:
--   A (old) → dev_dm.revops_analytics.provisions_temp
--   B (new) → dev_dm.revops_analytics.provisions_temp4726
--
-- Segments:
--   old_only  — in A but not B  (OLIs dropped by new run)
--   new_only  — in B but not A  (OLIs added by new run)
--   in_both   — in both A and B
--   total_old — all unique OLIs in A
--   total_new — all unique OLIs in B
--
-- Sanity checks:
--   old_only + in_both  = total_old
--   new_only + in_both  = total_new
--   in_both            ≤ min(total_old, total_new)
-- =============================================================================

WITH

old_olis AS (
  SELECT DISTINCT oli_id, product_family
  FROM dev_dm.revops_analytics.provisions_temp
),

new_olis AS (
  SELECT DISTINCT oli_id, product_family
  FROM dev_dm.revops_analytics.provisions_temp4726
),

-- OLIs that appear in only one dataset, with product_family sourced from
-- whichever side has it (COALESCE handles any family-change edge cases)
all_olis AS (
  SELECT
    COALESCE(o.oli_id,       n.oli_id)       AS oli_id,
    COALESCE(o.product_family, n.product_family) AS product_family,
    CASE
      WHEN o.oli_id IS NOT NULL AND n.oli_id IS NOT NULL THEN 'in_both'
      WHEN o.oli_id IS NOT NULL                          THEN 'old_only'
      ELSE                                                    'new_only'
    END AS segment
  FROM old_olis o
  FULL OUTER JOIN new_olis n ON o.oli_id = n.oli_id
)

-- =============================================================================
-- PART 1: Overall summary
-- =============================================================================
SELECT
  'overall'        AS product_family,
  segment,
  COUNT(*)         AS unique_oli_count
FROM all_olis
GROUP BY segment

UNION ALL

SELECT
  'overall'        AS product_family,
  'total_old'      AS segment,
  COUNT(DISTINCT oli_id) AS unique_oli_count
FROM old_olis

UNION ALL

SELECT
  'overall'        AS product_family,
  'total_new'      AS segment,
  COUNT(DISTINCT oli_id) AS unique_oli_count
FROM new_olis

-- =============================================================================
-- PART 2: Grouped by product_family
-- =============================================================================
UNION ALL

SELECT
  product_family,
  segment,
  COUNT(*)         AS unique_oli_count
FROM all_olis
GROUP BY product_family, segment

UNION ALL

SELECT
  product_family,
  'total_old'      AS segment,
  COUNT(DISTINCT oli_id) AS unique_oli_count
FROM old_olis
GROUP BY product_family

UNION ALL

SELECT
  product_family,
  'total_new'      AS segment,
  COUNT(DISTINCT oli_id) AS unique_oli_count
FROM new_olis
GROUP BY product_family

ORDER BY
  product_family,
  CASE segment
    WHEN 'total_old' THEN 1
    WHEN 'total_new' THEN 2
    WHEN 'in_both'   THEN 3
    WHEN 'old_only'  THEN 4
    WHEN 'new_only'  THEN 5
  END;
