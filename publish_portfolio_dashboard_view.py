# Databricks notebook source
# MAGIC %md
# MAGIC # Publish Portfolio Dashboard View
# MAGIC
# MAGIC Runs after `Portfolio Federation Pipeline` in the workflow. Creates a
# MAGIC **non-materialized** Unity Catalog view at
# MAGIC `mp_catalog.analytics_dlt.portfolio_dashboard`.
# MAGIC
# MAGIC Why this lives outside the SDP pipeline: SDP pipelines cannot natively
# MAGIC publish a non-materialized view to the catalog. `@dlt.view()` outputs
# MAGIC are pipeline-internal, `@dlt.table()` materializes, and SDP Python
# MAGIC restricts `spark.sql()` to read-only commands so in-pipeline DDL is
# MAGIC blocked. Running `CREATE OR REPLACE VIEW` as a companion SQL task is
# MAGIC the only way to produce a queryable, non-materialized output.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS mp_catalog.analytics_dlt;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW mp_catalog.analytics_dlt.portfolio_dashboard AS
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
# MAGIC     c.FF_COM_EQ AS shareholders_equity,
# MAGIC     c.FF_FUNDS_OPER_GROSS AS operating_cash_flow,
# MAGIC     c.FF_DEBT_LT AS long_term_debt,
# MAGIC     ROUND(c.FF_NET_INCOME / NULLIF(c.FF_SALES, 0) * 100, 2) AS profit_margin_pct,
# MAGIC     ROUND(c.FF_COM_EQ / NULLIF(c.FF_ASSETS, 0) * 100, 2) AS equity_ratio_pct,
# MAGIC     ROUND(c.FF_DEBT_LT / NULLIF(c.FF_COM_EQ, 0), 2) AS debt_to_equity_ratio
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
# MAGIC     c.FE_FP_END AS next_estimate_period,
# MAGIC     c.FE_MEAN AS forward_eps,
# MAGIC     c.FE_NUM_EST AS num_analysts,
# MAGIC     c.FE_STD_DEV AS analyst_disagreement
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
# MAGIC   f.fiscal_date,
# MAGIC   ROUND(f.revenue / 1000000, 2) AS revenue_mm,
# MAGIC   ROUND(f.net_income / 1000000, 2) AS net_income_mm,
# MAGIC   ROUND(f.operating_cash_flow / 1000000, 2) AS operating_cf_mm,
# MAGIC   f.profit_margin_pct,
# MAGIC   f.equity_ratio_pct,
# MAGIC   f.debt_to_equity_ratio,
# MAGIC   f.current_eps,
# MAGIC   e.forward_eps,
# MAGIC   e.next_estimate_period,
# MAGIC   e.num_analysts AS num_analysts_covering,
# MAGIC   ROUND(p.shares_held * f.current_eps, 2) AS my_current_annual_earnings,
# MAGIC   ROUND(p.shares_held * e.forward_eps, 2) AS my_projected_annual_earnings,
# MAGIC   ROUND(p.shares_held * (e.forward_eps - f.current_eps), 2) AS my_expected_earnings_increase,
# MAGIC   ROUND(((e.forward_eps - f.current_eps) / NULLIF(ABS(f.current_eps), 0)) * 100, 2) AS projected_eps_growth_pct
# MAGIC FROM portfolio p
# MAGIC JOIN fundamentals f ON p.ticker_region = f.ticker_region
# MAGIC LEFT JOIN estimates e ON p.ticker_region = e.ticker_region;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm view type is VIEW (not MATERIALIZED_VIEW)
# MAGIC SELECT table_schema, table_name, table_type
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog = 'mp_catalog'
# MAGIC   AND table_schema = 'analytics_dlt'
# MAGIC   AND table_name = 'portfolio_dashboard';
