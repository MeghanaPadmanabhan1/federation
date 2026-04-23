# Databricks notebook source
# MAGIC %md
# MAGIC # Task 03c — Create `my_action_items` view
# MAGIC
# MAGIC Urgency-ordered subset of holdings needing attention (sell/high-risk/high-growth).
# MAGIC Non-materialized.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW mp_catalog.analytics.my_action_items AS
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   shares_held,
# MAGIC   my_current_annual_earnings,
# MAGIC   my_projected_annual_earnings,
# MAGIC   projected_eps_growth_pct,
# MAGIC   profit_margin_pct,
# MAGIC   debt_to_equity_ratio,
# MAGIC   num_analysts_covering,
# MAGIC   risk_assessment,
# MAGIC   action_recommendation,
# MAGIC   CASE
# MAGIC     WHEN action_recommendation LIKE '%SELL%' THEN 'URGENT - Consider Selling'
# MAGIC     WHEN risk_assessment LIKE '%High%' THEN 'REVIEW - High Risk'
# MAGIC     WHEN projected_eps_growth_pct > 25 THEN 'OPPORTUNITY - Strong Growth'
# MAGIC     WHEN action_recommendation LIKE '%STRONG BUY%' THEN 'OPPORTUNITY - Consider Buying More'
# MAGIC     ELSE 'MONITOR - Stable'
# MAGIC   END AS priority_action,
# MAGIC   CURRENT_TIMESTAMP() AS last_updated
# MAGIC FROM mp_catalog.analytics.my_portfolio_dashboard
# MAGIC WHERE action_recommendation LIKE '%SELL%'
# MAGIC    OR action_recommendation LIKE '%STRONG BUY%'
# MAGIC    OR risk_assessment LIKE '%High%'
# MAGIC    OR projected_eps_growth_pct > 20
