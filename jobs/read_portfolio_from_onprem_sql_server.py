# Databricks notebook source
# MAGIC %md
# MAGIC # Read Portfolio from On-Premise SQL Server (via Lakehouse Federation)
# MAGIC
# MAGIC Reads the investor's equity holdings from the on-premise SQL Server using
# MAGIC Lakehouse Federation. The foreign catalog `mp_portfolio_federated` proxies
# MAGIC reads directly to the on-prem database — **no holdings data is copied into
# MAGIC cloud storage**. Every query evaluates live against the source.
# MAGIC
# MAGIC Source: `mp_portfolio_federated.dbo.equity_holdings`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                 AS total_rows,
# MAGIC   COUNT(DISTINCT symbol)                   AS distinct_symbols,
# MAGIC   SUM(CASE WHEN instrument_type = 'Equity' THEN 1 ELSE 0 END) AS equity_rows
# MAGIC FROM mp_portfolio_federated.dbo.equity_holdings
