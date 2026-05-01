# Databricks notebook source
# MAGIC %md
# MAGIC # Build Dashboard Views
# MAGIC
# MAGIC Creates the 7 derived views that back the AI/BI dashboard. All
# MAGIC `CREATE OR REPLACE VIEW` — no data materialized. Depend on the base
# MAGIC `my_portfolio_dashboard` view produced by the prior task.
# MAGIC
# MAGIC | View | Purpose |
# MAGIC |---|---|
# MAGIC | `my_portfolio_summary` | 1-row portfolio-level KPIs (counters) |
# MAGIC | `my_stock_rankings` | Per-stock performance score + risk/action |
# MAGIC | `my_action_items` | Urgency-ordered subset of holdings needing attention |
# MAGIC | `aibi_portfolio_overview` | AI/BI top-line KPI row |
# MAGIC | `aibi_risk_distribution` | Risk category breakdown for pie chart |
# MAGIC | `aibi_action_distribution` | Buy/Hold/Sell breakdown for pie chart |
# MAGIC | `aibi_stock_performance` | Per-stock performance tier for table |

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
# MAGIC   CASE risk_category WHEN 'High Risk' THEN 1 WHEN 'Medium Risk' THEN 2 ELSE 3 END

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
# MAGIC   CASE recommendation WHEN 'Sell' THEN 1 WHEN 'Hold' THEN 2 WHEN 'Buy' THEN 3 WHEN 'Strong Buy' THEN 4 END

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
