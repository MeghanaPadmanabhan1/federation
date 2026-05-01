# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh Metric View
# MAGIC
# MAGIC Validates `mp_catalog.analytics.portfolio_metrics` (the Unity Catalog
# MAGIC **metric view** that defines governed measures and dimensions over the
# MAGIC portfolio data).
# MAGIC
# MAGIC Metric views in Databricks are compute-on-read — no "refresh" materializes
# MAGIC results. This task runs a `MEASURE()` query to confirm the metric view
# MAGIC resolves against the fresh base view produced by the upstream task.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   MEASURE(`Total Holdings`)                  AS total_holdings,
# MAGIC   ROUND(MEASURE(`Current Annual Earnings`), 2)  AS current_earnings,
# MAGIC   ROUND(MEASURE(`Projected Annual Earnings`), 2) AS projected_earnings,
# MAGIC   ROUND(MEASURE(`Portfolio Growth Rate`), 2) AS growth_rate_pct,
# MAGIC   MEASURE(`High Risk Count`)                 AS high_risk_stocks,
# MAGIC   MEASURE(`Buy Opportunities`)               AS buy_opportunities,
# MAGIC   MEASURE(`Sell Recommendations`)            AS sell_recommendations
# MAGIC FROM mp_catalog.analytics.portfolio_metrics
