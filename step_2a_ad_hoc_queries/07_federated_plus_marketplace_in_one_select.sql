-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ad hoc: Federated + Marketplace in one SELECT
-- MAGIC
-- MAGIC Blog screenshot query. One SELECT joins on-prem SQL Server holdings (mp_portfolio_federated, via Lakehouse Federation) with FactSet fundamentals (mp_factset_data, a Databricks Marketplace share). Output: per-holding annual earnings = shares held * current EPS. The analyst writes vanilla SQL; federation makes both sources act like one warehouse.

-- COMMAND ----------

-- One SELECT, two completely different origins:
--   mp_portfolio_federated.*  ->  on-prem SQL Server (Lakehouse Federation)
--   mp_factset_data.*         ->  Databricks Marketplace share (FactSet)
-- Output: per-holding annual earnings = shares held * current EPS.

SELECT
  p.symbol,
  p.number_of_shares                              AS shares_held,
  c.FF_EPS_BASIC                                  AS current_eps,
  ROUND(p.number_of_shares * c.FF_EPS_BASIC, 2)   AS my_annual_earnings
FROM mp_portfolio_federated.dbo.equity_holdings   p   -- federated -> on-prem
JOIN mp_factset_data.sym_v1.sym_ticker_region     a   ON UPPER(p.symbol) = a.ticker_region
JOIN mp_factset_data.ff_v3.ff_sec_map             b   ON a.fsym_id = b.fsym_id
JOIN mp_factset_data.ff_v3.ff_basic_af            c   ON b.fsym_company_id = c.fsym_id  -- Marketplace
WHERE p.instrument_type = 'Equity'
  AND c.DATE >= '2023-01-01'
QUALIFY ROW_NUMBER() OVER (PARTITION BY p.symbol ORDER BY c.DATE DESC) = 1
ORDER BY my_annual_earnings DESC
LIMIT 10;
