-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ad hoc: Where are my analyst coverage gaps?
-- MAGIC
-- MAGIC For each holding: count of analysts covering and the spread of their estimates. Surfaces thin coverage (<5 analysts) or high disagreement (>30% spread) - positions where the asset manager has less information edge. Asset manager use: prioritize independent research where the Street isn't paying attention.

-- COMMAND ----------

-- Federated holdings + FactSet consensus estimates.
-- Identifies positions with thin or noisy analyst coverage.
WITH latest_estimates AS (
  SELECT
    a.ticker_region,
    c.FE_MEAN                                                     AS forward_eps,
    c.FE_NUM_EST                                                  AS analyst_count,
    c.FE_HIGH                                                     AS forward_eps_high,
    c.FE_LOW                                                      AS forward_eps_low,
    ROUND((c.FE_HIGH - c.FE_LOW) / NULLIF(c.FE_MEAN, 0) * 100, 2) AS disagreement_pct
  FROM mp_factset_data.sym_v1.sym_ticker_region a
  JOIN mp_factset_data.fe_v4.fe_sec_map         b ON a.fsym_id = b.fsym_id
  JOIN mp_factset_data.fe_v4.fe_basic_conh_af   c ON b.fsym_company_id = c.fsym_id
  WHERE c.FE_ITEM = 'EPS'
    AND c.CONS_END_DATE IS NULL
    AND c.FE_FP_END >= CURRENT_DATE()
    AND ABS(c.FE_MEAN) < 50
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.FE_FP_END) = 1
)
SELECT
  p.symbol,
  p.number_of_shares                                       AS shares_held,
  COALESCE(e.analyst_count, 0)                             AS analysts_covering,
  e.forward_eps,
  e.disagreement_pct,
  CASE
    WHEN e.analyst_count IS NULL OR e.analyst_count = 0      THEN 'NO COVERAGE - Limited Visibility'
    WHEN e.analyst_count < 5                                 THEN 'THIN COVERAGE - Higher Uncertainty'
    WHEN e.disagreement_pct > 30                             THEN 'HIGH DISAGREEMENT - Analysts Split'
    WHEN e.analyst_count >= 10 AND e.disagreement_pct < 10   THEN 'STRONG CONSENSUS'
    ELSE 'NORMAL COVERAGE'
  END AS coverage_status
FROM mp_portfolio_federated.dbo.equity_holdings p
LEFT JOIN latest_estimates e ON UPPER(p.symbol) = e.ticker_region
WHERE p.instrument_type = 'Equity'
ORDER BY
  CASE
    WHEN e.analyst_count IS NULL OR e.analyst_count = 0     THEN 1
    WHEN e.analyst_count < 5                                 THEN 2
    WHEN e.disagreement_pct > 30                             THEN 3
    ELSE 4
  END,
  p.number_of_shares DESC
LIMIT 50;
