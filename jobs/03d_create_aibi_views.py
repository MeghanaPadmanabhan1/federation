# Databricks notebook source
# MAGIC %md
# MAGIC # Task 03d — Create AI/BI dashboard views
# MAGIC
# MAGIC Four views backing the AI/BI dashboard widgets:
# MAGIC - `aibi_portfolio_overview` — top-line KPI counter row
# MAGIC - `aibi_risk_distribution` — risk category distribution
# MAGIC - `aibi_action_distribution` — buy/hold/sell distribution
# MAGIC - `aibi_stock_performance` — per-stock performance tier
# MAGIC
# MAGIC All non-materialized.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW mp_catalog.analytics.aibi_portfolio_overview AS
# MAGIC SELECT
# MAGIC   'Portfolio Health' AS metric_category,
# MAGIC   COUNT(DISTINCT symbol) AS total_stocks,
# MAGIC   ROUND(SUM(my_current_annual_earnings), 2) AS total_annual_earnings,
# MAGIC   ROUND(SUM(my_projected_annual_earnings), 2) AS projected_annual_earnings,
# MAGIC   ROUND(
# MAGIC     (SUM(my_projected_annual_earnings) - SUM(my_current_annual_earnings)) /
# MAGIC     NULLIF(SUM(my_current_annual_earnings), 0) * 100, 2
# MAGIC   ) AS portfolio_growth_pct,
# MAGIC   ROUND(AVG(profit_margin_pct), 2) AS avg_profit_margin,
# MAGIC   ROUND(AVG(debt_to_equity_ratio), 2) AS avg_leverage
# MAGIC FROM mp_catalog.analytics.my_portfolio_dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW mp_catalog.analytics.aibi_risk_distribution AS
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN risk_assessment LIKE '%High%' THEN 'High Risk'
# MAGIC     WHEN risk_assessment LIKE '%Medium%' THEN 'Medium Risk'
# MAGIC     ELSE 'Low Risk'
# MAGIC   END AS risk_category,
# MAGIC   COUNT(*) AS num_stocks,
# MAGIC   ROUND(SUM(my_current_annual_earnings), 2) AS total_earnings,
# MAGIC   ROUND(AVG(projected_eps_growth_pct), 2) AS avg_growth_rate
# MAGIC FROM mp_catalog.analytics.my_portfolio_dashboard
# MAGIC GROUP BY risk_category
# MAGIC ORDER BY
# MAGIC   CASE risk_category
# MAGIC     WHEN 'High Risk' THEN 1
# MAGIC     WHEN 'Medium Risk' THEN 2
# MAGIC     ELSE 3
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW mp_catalog.analytics.aibi_action_distribution AS
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN action_recommendation LIKE '%STRONG BUY%' THEN 'Strong Buy'
# MAGIC     WHEN action_recommendation LIKE '%BUY%' THEN 'Buy'
# MAGIC     WHEN action_recommendation LIKE '%SELL%' THEN 'Sell'
# MAGIC     ELSE 'Hold'
# MAGIC   END AS recommendation,
# MAGIC   COUNT(*) AS num_stocks,
# MAGIC   ROUND(SUM(my_current_annual_earnings), 2) AS current_earnings_impact,
# MAGIC   ROUND(AVG(projected_eps_growth_pct), 2) AS avg_expected_growth
# MAGIC FROM mp_catalog.analytics.my_portfolio_dashboard
# MAGIC GROUP BY recommendation
# MAGIC ORDER BY
# MAGIC   CASE recommendation
# MAGIC     WHEN 'Sell' THEN 1
# MAGIC     WHEN 'Hold' THEN 2
# MAGIC     WHEN 'Buy' THEN 3
# MAGIC     WHEN 'Strong Buy' THEN 4
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW mp_catalog.analytics.aibi_stock_performance AS
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   shares_held,
# MAGIC   my_current_annual_earnings,
# MAGIC   my_projected_annual_earnings,
# MAGIC   projected_eps_growth_pct,
# MAGIC   profit_margin_pct,
# MAGIC   CASE
# MAGIC     WHEN projected_eps_growth_pct > 15 THEN 'Top Performer'
# MAGIC     WHEN projected_eps_growth_pct > 5 THEN 'Above Average'
# MAGIC     WHEN projected_eps_growth_pct > -5 THEN 'Stable'
# MAGIC     ELSE 'Underperformer'
# MAGIC   END AS performance_tier
# MAGIC FROM mp_catalog.analytics.my_portfolio_dashboard
