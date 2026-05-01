# Databricks notebook source
# MAGIC %md
# MAGIC # Read FactSet from Databricks Marketplace
# MAGIC
# MAGIC Reads FactSet financial data shared via Databricks Marketplace (lives in
# MAGIC the cloud). Read-only — confirms the three FactSet tables this pipeline
# MAGIC depends on are reachable and populated.
# MAGIC
# MAGIC - `mp_factset_data.sym_v1.sym_ticker_region` — ticker → FactSet fsym_id symbology
# MAGIC - `mp_factset_data.ff_v3.ff_basic_af` — annual fundamentals
# MAGIC - `mp_factset_data.fe_v4.fe_basic_conh_af` — consensus analyst estimates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'sym_ticker_region'       AS factset_table, COUNT(*) AS row_count FROM mp_factset_data.sym_v1.sym_ticker_region
# MAGIC UNION ALL SELECT 'ff_basic_af (>=2023)',   COUNT(*) FROM mp_factset_data.ff_v3.ff_basic_af WHERE DATE >= '2023-01-01'
# MAGIC UNION ALL SELECT 'fe_basic_conh_af (EPS)', COUNT(*) FROM mp_factset_data.fe_v4.fe_basic_conh_af WHERE FE_ITEM = 'EPS'
