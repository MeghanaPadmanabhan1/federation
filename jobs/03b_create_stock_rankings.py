# Databricks notebook source
# MAGIC %md
# MAGIC # Task 03b — Create `my_stock_rankings` view
# MAGIC
# MAGIC Per-stock performance score + risk / action metadata. Non-materialized.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW mp_catalog.analytics.my_stock_rankings AS
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   shares_held,
# MAGIC   my_current_annual_earnings AS annual_earnings_contribution,
# MAGIC   projected_eps_growth_pct AS growth_rate,
# MAGIC   profit_margin_pct,
# MAGIC   debt_to_equity_ratio,
# MAGIC   num_analysts_covering,
# MAGIC   ROUND(
# MAGIC     (CASE WHEN projected_eps_growth_pct > 0 THEN LEAST(projected_eps_growth_pct, 30) ELSE 0 END) +
# MAGIC     (CASE WHEN profit_margin_pct > 0 THEN LEAST(profit_margin_pct, 30) ELSE 0 END) +
# MAGIC     (CASE WHEN debt_to_equity_ratio < 2 THEN 20 ELSE 10 END) +
# MAGIC     (CASE WHEN num_analysts_covering >= 10 THEN 20 ELSE num_analysts_covering * 2 END),
# MAGIC   0) AS performance_score,
# MAGIC   risk_assessment,
# MAGIC   action_recommendation
# MAGIC FROM mp_catalog.analytics.my_portfolio_dashboard
