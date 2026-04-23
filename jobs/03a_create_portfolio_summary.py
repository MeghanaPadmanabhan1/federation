# Databricks notebook source
# MAGIC %md
# MAGIC # Task 03a — Create `my_portfolio_summary` view
# MAGIC
# MAGIC Portfolio-level aggregate (counts, totals, averages). Non-materialized.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW mp_catalog.analytics.my_portfolio_summary AS
# MAGIC SELECT
# MAGIC   'My Portfolio' AS portfolio_name,
# MAGIC   COUNT(DISTINCT symbol) AS total_holdings,
# MAGIC   ROUND(SUM(my_current_annual_earnings), 2) AS total_current_earnings,
# MAGIC   ROUND(SUM(my_projected_annual_earnings), 2) AS total_projected_earnings,
# MAGIC   ROUND(AVG(projected_eps_growth_pct), 2) AS avg_growth_rate,
# MAGIC   ROUND(AVG(profit_margin_pct), 2) AS avg_profit_margin,
# MAGIC   ROUND(AVG(debt_to_equity_ratio), 2) AS avg_debt_to_equity,
# MAGIC   SUM(CASE WHEN risk_assessment LIKE '%High%' THEN 1 ELSE 0 END) AS high_risk_stocks,
# MAGIC   SUM(CASE WHEN action_recommendation LIKE '%SELL%' THEN 1 ELSE 0 END) AS stocks_to_sell,
# MAGIC   SUM(CASE WHEN action_recommendation LIKE '%BUY%' THEN 1 ELSE 0 END) AS buying_opportunities,
# MAGIC   SUM(CASE WHEN projected_eps_growth_pct > 20 THEN 1 ELSE 0 END) AS high_growth_stocks,
# MAGIC   CURRENT_TIMESTAMP() AS last_updated
# MAGIC FROM mp_catalog.analytics.my_portfolio_dashboard
