-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ad hoc: What's in my book right now?
-- MAGIC
-- MAGIC Pure federated read against on-prem SQL Server through Lakehouse Federation. The foreign catalog mp_portfolio_federated proxies the query through to the source; the SQL editor treats it exactly like a native Databricks table. Asset manager use: morning glance at top positions and their share of the book.

-- COMMAND ----------

-- Pure federated read against on-prem SQL Server.
-- The foreign catalog mp_portfolio_federated proxies these tables;
-- the SQL editor treats it exactly like a native Databricks table.
SELECT
  symbol,
  number_of_shares                                                   AS shares_held,
  instrument_type,
  ROUND(number_of_shares * 100.0 / SUM(number_of_shares) OVER (), 3) AS pct_of_total_shares
FROM mp_portfolio_federated.dbo.equity_holdings
WHERE instrument_type = 'Equity'
ORDER BY shares_held DESC
LIMIT 25;
