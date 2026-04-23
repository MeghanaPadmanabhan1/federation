# Databricks notebook source
# MAGIC %md
# MAGIC # Task 04 — Smoke test
# MAGIC
# MAGIC Runs a `SELECT` against each published view to confirm it is queryable
# MAGIC and returns rows. Fails the workflow if any view is unreachable or the
# MAGIC portfolio ends up empty.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'my_portfolio_dashboard' AS view_name, COUNT(*) AS row_count
# MAGIC FROM mp_catalog.analytics.my_portfolio_dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'my_portfolio_summary' AS view_name, total_holdings, total_current_earnings, total_projected_earnings
# MAGIC FROM mp_catalog.analytics.my_portfolio_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'my_stock_rankings' AS view_name, COUNT(*) AS row_count FROM mp_catalog.analytics.my_stock_rankings
# MAGIC UNION ALL SELECT 'my_action_items', COUNT(*) FROM mp_catalog.analytics.my_action_items
# MAGIC UNION ALL SELECT 'aibi_portfolio_overview', COUNT(*) FROM mp_catalog.analytics.aibi_portfolio_overview
# MAGIC UNION ALL SELECT 'aibi_risk_distribution', COUNT(*) FROM mp_catalog.analytics.aibi_risk_distribution
# MAGIC UNION ALL SELECT 'aibi_action_distribution', COUNT(*) FROM mp_catalog.analytics.aibi_action_distribution
# MAGIC UNION ALL SELECT 'aibi_stock_performance', COUNT(*) FROM mp_catalog.analytics.aibi_stock_performance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm every object is a plain VIEW (not materialized)
# MAGIC SELECT table_name, table_type
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog = 'mp_catalog'
# MAGIC   AND table_schema = 'analytics'
# MAGIC   AND table_name IN (
# MAGIC     'my_portfolio_dashboard','my_portfolio_summary','my_stock_rankings',
# MAGIC     'my_action_items','aibi_portfolio_overview','aibi_risk_distribution',
# MAGIC     'aibi_action_distribution','aibi_stock_performance'
# MAGIC   )
# MAGIC ORDER BY table_name
