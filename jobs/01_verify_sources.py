# Databricks notebook source
# MAGIC %md
# MAGIC # Task 01 — Verify federated + marketplace sources
# MAGIC
# MAGIC Smoke-tests that the on-premise SQL Server foreign catalog and the FactSet
# MAGIC marketplace catalog are both reachable before the pipeline kicks off view
# MAGIC creation. Read-only; does not move or persist any data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- On-premise holdings (federated, data stays on-prem)
# MAGIC SELECT COUNT(*) AS federated_holdings_count
# MAGIC FROM mp_portfolio_federated.dbo.equity_holdings
# MAGIC WHERE instrument_type = 'Equity'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FactSet symbology (marketplace)
# MAGIC SELECT COUNT(*) AS factset_ticker_rows
# MAGIC FROM mp_factset_data.sym_v1.sym_ticker_region

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FactSet fundamentals (marketplace)
# MAGIC SELECT COUNT(*) AS factset_fundamentals_rows
# MAGIC FROM mp_factset_data.ff_v3.ff_basic_af
# MAGIC WHERE DATE >= '2023-01-01'
