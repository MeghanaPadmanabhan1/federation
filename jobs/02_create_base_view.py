# Databricks notebook source
# MAGIC %md
# MAGIC # Task 02 — Create `my_portfolio_dashboard` base view
# MAGIC
# MAGIC The core join: federated on-prem holdings + FactSet fundamentals + FactSet
# MAGIC estimates. Defined as a regular `CREATE OR REPLACE VIEW` so no data is
# MAGIC materialized — each query re-evaluates against the federated source.
# MAGIC
# MAGIC EPS sanity filters (`ABS(eps) < 50`, `FF_SALES > 0`) exclude FactSet rows
# MAGIC with malformed values for certain micro-cap tickers that otherwise skew
# MAGIC the earnings totals.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS mp_catalog.analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW mp_catalog.analytics.my_portfolio_dashboard AS
# MAGIC WITH portfolio AS (
# MAGIC   SELECT
# MAGIC     symbol,
# MAGIC     UPPER(symbol) AS ticker_region,
# MAGIC     number_of_shares AS shares_held,
# MAGIC     instrument_type
# MAGIC   FROM mp_portfolio_federated.dbo.equity_holdings
# MAGIC   WHERE instrument_type = 'Equity'
# MAGIC ),
# MAGIC fundamentals AS (
# MAGIC   SELECT
# MAGIC     a.ticker_region,
# MAGIC     c.DATE AS fiscal_date,
# MAGIC     c.FF_SALES AS revenue,
# MAGIC     c.FF_NET_INCOME AS net_income,
# MAGIC     c.FF_EPS_BASIC AS current_eps,
# MAGIC     c.FF_ASSETS AS total_assets,
# MAGIC     c.FF_COM_EQ AS shareholders_equity,
# MAGIC     c.FF_FUNDS_OPER_GROSS AS operating_cash_flow,
# MAGIC     (c.FF_DEBT_ST + c.FF_DEBT_LT) AS total_debt,
# MAGIC     ROUND(c.FF_NET_INCOME / NULLIF(c.FF_SALES, 0) * 100, 2) AS profit_margin_pct,
# MAGIC     ROUND(c.FF_FUNDS_OPER_GROSS / NULLIF(c.FF_SALES, 0) * 100, 2) AS cash_flow_margin_pct,
# MAGIC     ROUND((c.FF_DEBT_ST + c.FF_DEBT_LT) / NULLIF(c.FF_COM_EQ, 0), 2) AS debt_to_equity_ratio,
# MAGIC     ROUND(c.FF_COM_EQ / NULLIF(c.FF_ASSETS, 0) * 100, 2) AS equity_ratio_pct
# MAGIC   FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   JOIN mp_factset_data.ff_v3.ff_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN mp_factset_data.ff_v3.ff_basic_af c ON b.fsym_company_id = c.fsym_id
# MAGIC   WHERE c.DATE >= '2023-01-01'
# MAGIC     AND c.FF_EPS_BASIC IS NOT NULL
# MAGIC     AND ABS(c.FF_EPS_BASIC) < 50
# MAGIC     AND c.FF_SALES > 0
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.DATE DESC) = 1
# MAGIC ),
# MAGIC estimates AS (
# MAGIC   SELECT
# MAGIC     a.ticker_region,
# MAGIC     c.FE_FP_END AS next_fiscal_period,
# MAGIC     c.FE_MEAN AS forward_eps,
# MAGIC     c.FE_MEDIAN AS forward_eps_median,
# MAGIC     c.FE_HIGH AS forward_eps_high,
# MAGIC     c.FE_LOW AS forward_eps_low,
# MAGIC     c.FE_NUM_EST AS analyst_count,
# MAGIC     ROUND((c.FE_HIGH - c.FE_LOW) / NULLIF(c.FE_MEAN, 0) * 100, 2) AS estimate_spread_pct
# MAGIC   FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   JOIN mp_factset_data.fe_v4.fe_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN mp_factset_data.fe_v4.fe_basic_conh_af c ON b.fsym_company_id = c.fsym_id
# MAGIC   WHERE c.FE_ITEM = 'EPS'
# MAGIC     AND c.CONS_END_DATE IS NULL
# MAGIC     AND c.FE_FP_END >= CURRENT_DATE()
# MAGIC     AND ABS(c.FE_MEAN) < 50
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.FE_FP_END) = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   p.symbol,
# MAGIC   p.shares_held,
# MAGIC   f.fiscal_date AS latest_fiscal_date,
# MAGIC   ROUND(f.revenue / 1000000, 2) AS revenue_mm,
# MAGIC   ROUND(f.net_income / 1000000, 2) AS net_income_mm,
# MAGIC   ROUND(f.operating_cash_flow / 1000000, 2) AS operating_cf_mm,
# MAGIC   f.profit_margin_pct,
# MAGIC   f.cash_flow_margin_pct,
# MAGIC   f.debt_to_equity_ratio,
# MAGIC   f.equity_ratio_pct,
# MAGIC   f.current_eps,
# MAGIC   e.forward_eps,
# MAGIC   e.next_fiscal_period AS next_estimate_period,
# MAGIC   e.analyst_count AS num_analysts_covering,
# MAGIC   e.estimate_spread_pct AS analyst_disagreement_pct,
# MAGIC   ROUND(p.shares_held * f.current_eps, 2) AS my_current_annual_earnings,
# MAGIC   ROUND(p.shares_held * e.forward_eps, 2) AS my_projected_annual_earnings,
# MAGIC   ROUND(p.shares_held * (e.forward_eps - f.current_eps), 2) AS my_expected_earnings_increase,
# MAGIC   ROUND(((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) * 100, 2) AS projected_eps_growth_pct,
# MAGIC   CASE
# MAGIC     WHEN f.net_income < 0 THEN 'High Risk - Unprofitable'
# MAGIC     WHEN f.debt_to_equity_ratio > 2.5 THEN 'High Risk - Excessive Debt'
# MAGIC     WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) < -0.15 THEN 'Medium Risk - Declining Earnings'
# MAGIC     WHEN f.profit_margin_pct < 3 THEN 'Medium Risk - Low Margins'
# MAGIC     WHEN f.profit_margin_pct > 15 AND f.debt_to_equity_ratio < 1.5 THEN 'Low Risk - Strong Fundamentals'
# MAGIC     ELSE 'Low Risk'
# MAGIC   END AS risk_assessment,
# MAGIC   CASE
# MAGIC     WHEN f.net_income < 0 THEN 'SELL - Company Losing Money'
# MAGIC     WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) < -0.15 THEN 'SELL - Earnings Declining'
# MAGIC     WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) > 0.20 THEN 'STRONG BUY - High Growth Expected'
# MAGIC     WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) > 0.05 THEN 'BUY - Positive Growth'
# MAGIC     WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) > -0.10 THEN 'HOLD - Stable'
# MAGIC     ELSE 'HOLD - Monitor'
# MAGIC   END AS action_recommendation
# MAGIC FROM portfolio p
# MAGIC JOIN fundamentals f ON p.ticker_region = f.ticker_region
# MAGIC LEFT JOIN estimates e ON p.ticker_region = e.ticker_region

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm view type (not materialized)
# MAGIC SELECT table_type FROM system.information_schema.tables
# MAGIC WHERE table_catalog = 'mp_catalog' AND table_schema = 'analytics'
# MAGIC   AND table_name = 'my_portfolio_dashboard'
