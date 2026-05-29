-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ad hoc: How concentrated is my book?
-- MAGIC
-- MAGIC Pure federated. Cumulative share concentration of the book using window functions. Demonstrates that analytical SQL (window functions, ranking) works seamlessly against the on-prem source via Lakehouse Federation. Asset manager use: surface concentration risk in the top names.

-- COMMAND ----------

-- Pure federated. Window functions over the on-prem source.
-- Identifies what share of the book is concentrated in the top N names.
WITH ranked AS (
  SELECT
    symbol,
    number_of_shares                                                   AS shares_held,
    ROW_NUMBER() OVER (ORDER BY number_of_shares DESC)                 AS position_rank,
    ROUND(number_of_shares * 100.0 / SUM(number_of_shares) OVER (), 3) AS pct_of_book,
    ROUND(SUM(number_of_shares) OVER (ORDER BY number_of_shares DESC
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          * 100.0 / SUM(number_of_shares) OVER (), 3)                  AS cumulative_pct
  FROM mp_portfolio_federated.dbo.equity_holdings
  WHERE instrument_type = 'Equity'
)
SELECT *
FROM ranked
WHERE position_rank <= 50
ORDER BY position_rank;
