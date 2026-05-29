-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ad hoc: Where is my annual earnings coming from?
-- MAGIC
-- MAGIC Federated holdings (on-prem SQL Server) × FactSet annual fundamentals (Marketplace) in one SELECT. Per-holding contribution to total annual earnings, ranked. Demonstrates: one SQL statement spans an on-prem database AND a Databricks Marketplace share with no ETL in between.

-- COMMAND ----------

-- One SELECT, two completely different origins:
--   - mp_portfolio_federated.* lives in on-prem SQL Server
--   - mp_factset_data.* is a Databricks Marketplace share
-- The asset manager writes vanilla SQL; federation makes them act like one warehouse.
WITH latest_fundamentals AS (
  SELECT
    a.ticker_region,
    c.FF_EPS_BASIC                                          AS current_eps,
    c.FF_NET_INCOME                                         AS net_income,
    ROUND(c.FF_NET_INCOME / NULLIF(c.FF_SALES, 0) * 100, 2) AS profit_margin_pct
  FROM mp_factset_data.sym_v1.sym_ticker_region a
  JOIN mp_factset_data.ff_v3.ff_sec_map  b ON a.fsym_id = b.fsym_id
  JOIN mp_factset_data.ff_v3.ff_basic_af c ON b.fsym_company_id = c.fsym_id
  WHERE c.DATE >= '2023-01-01'
    AND c.FF_EPS_BASIC IS NOT NULL
    AND ABS(c.FF_EPS_BASIC) < 50
    AND c.FF_SALES > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.DATE DESC) = 1
)
SELECT
  p.symbol,
  p.number_of_shares                                          AS shares_held,
  f.current_eps,
  ROUND(p.number_of_shares * f.current_eps, 2)                AS my_annual_earnings,
  f.profit_margin_pct,
  RANK() OVER (ORDER BY p.number_of_shares * f.current_eps DESC) AS earnings_rank
FROM mp_portfolio_federated.dbo.equity_holdings p
JOIN latest_fundamentals f ON UPPER(p.symbol) = f.ticker_region
WHERE p.instrument_type = 'Equity'
ORDER BY my_annual_earnings DESC
LIMIT 25;
