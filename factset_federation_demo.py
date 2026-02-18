# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse Federation with FactSet Data
# MAGIC
# MAGIC ## Real-World Use Case: Portfolio Analysis Without Data Migration
# MAGIC
# MAGIC **The Challenge:**
# MAGIC - Customer has a portfolio stored in an **on-premise SQL database** (ticker symbols, shares held)
# MAGIC - Wants to analyze portfolio using **FactSet financial data** from Databricks Marketplace
# MAGIC - Cannot move sensitive portfolio data to the cloud due to security/compliance requirements
# MAGIC
# MAGIC **The Solution:**
# MAGIC Use Lakehouse Federation to query on-premise data **in place** and join with FactSet data for unified analytics.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Understanding FactSet Data Structure
# MAGIC
# MAGIC FactSet data uses **fsym_id** (FactSet Symbol ID) and requires mapping through symbology tables.
# MAGIC
# MAGIC **Key FactSet Schemas in mp_factset_data:**
# MAGIC - `ff_v3` - FactSet Fundamentals (financial statements, ratios)
# MAGIC - `fe_v4` - FactSet Estimates (analyst estimates, consensus data)
# MAGIC - `sym_v1` - Symbology tables (maps ticker-region to fsym_id)
# MAGIC
# MAGIC **Important Tables:**
# MAGIC - `sym_v1.sym_ticker_region` - Maps ticker-region (e.g., 'MSFT-US') to fsym_id
# MAGIC - `ff_v3.ff_sec_map` - Security mapping for fundamentals
# MAGIC - `ff_v3.ff_basic_af` - Annual fundamentals data
# MAGIC - `fe_v4.fe_sec_map` - Security mapping for estimates
# MAGIC - `fe_v4.fe_basic_conh_af` - Consensus estimates data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explore available FactSet tables
# MAGIC SHOW TABLES IN mp_factset_data.ff_v3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: View symbology table structure
# MAGIC SELECT *
# MAGIC FROM mp_factset_data.sym_v1.sym_ticker_region
# MAGIC WHERE ticker_region IN ('MSFT-US', 'AAPL-US', 'GOOGL-US')
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Understanding the Mapping Tables
# MAGIC
# MAGIC FactSet uses mapping tables to connect ticker-regions to the proper IDs for fundamentals and estimates.
# MAGIC
# MAGIC **Fundamentals Mapping:** sym_ticker_region → ff_sec_map → ff_basic_af
# MAGIC **Estimates Mapping:** sym_ticker_region → fe_sec_map → fe_basic_conh_af

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Fundamentals mapping chain
# MAGIC SELECT
# MAGIC   a.ticker_region,
# MAGIC   a.fsym_id,
# MAGIC   b.fsym_company_id
# MAGIC FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC JOIN mp_factset_data.ff_v3.ff_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC WHERE a.ticker_region = 'MSFT-US'
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Set Up Federation to On-Premise Portfolio Database

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create connection to on-premise SQL database
# MAGIC CREATE CONNECTION IF NOT EXISTS onprem_sql_connection
# MAGIC TYPE sqlserver
# MAGIC OPTIONS (
# MAGIC   host '<your-onprem-server>.database.windows.net',
# MAGIC   port '1433',
# MAGIC   user '<username>',
# MAGIC   password secret('onprem-secrets', 'sql-password')
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create foreign catalog for on-premise portfolio database
# MAGIC CREATE CATALOG IF NOT EXISTS portfolio_federated
# MAGIC USING CONNECTION onprem_sql_connection
# MAGIC OPTIONS (
# MAGIC   database 'PortfolioDB'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify we can see the schemas
# MAGIC SHOW SCHEMAS IN portfolio_federated;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Query Federated Portfolio Data (Stays On-Premise!)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer's portfolio data (queried from on-premise database)
# MAGIC -- This data NEVER leaves the on-premise SQL server
# MAGIC
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   ticker_symbol,
# MAGIC   shares_held,
# MAGIC   cost_basis,
# MAGIC   purchase_date,
# MAGIC   account_type
# MAGIC FROM portfolio_federated.dbo.customer_holdings
# MAGIC ORDER BY shares_held * cost_basis DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Portfolio + FactSet Fundamentals
# MAGIC
# MAGIC To combine portfolio with FactSet fundamentals, we use this join pattern:
# MAGIC
# MAGIC ```
# MAGIC Portfolio (ticker) → Add '-US' suffix → sym_ticker_region (fsym_id) → ff_sec_map (fsym_company_id) → ff_basic_af (financials)
# MAGIC   [On-Premise]                              [Databricks Symbology]           [Mapping]                    [Fundamentals]
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Complete query: Portfolio + FactSet Fundamentals
# MAGIC -- Shows how to enrich portfolio holdings with financial data
# MAGIC
# MAGIC WITH portfolio AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     ticker_symbol,
# MAGIC     CONCAT(ticker_symbol, '-US') AS ticker_region,  -- Convert to FactSet format
# MAGIC     shares_held,
# MAGIC     cost_basis,
# MAGIC     shares_held * cost_basis AS position_value,
# MAGIC     purchase_date
# MAGIC   FROM portfolio_federated.dbo.customer_holdings
# MAGIC ),
# MAGIC fundamentals_data AS (
# MAGIC   SELECT
# MAGIC     a.ticker_region,
# MAGIC     c.fsym_id,
# MAGIC     c.ff_date AS fiscal_date,
# MAGIC     c.ff_sales AS revenue,
# MAGIC     c.ff_net_inc AS net_income,
# MAGIC     c.ff_eps_basic AS eps,
# MAGIC     c.ff_assets AS total_assets,
# MAGIC     c.ff_com_eq AS shareholders_equity,
# MAGIC     c.ff_oper_cf AS operating_cash_flow,
# MAGIC     c.ff_debt_st AS short_term_debt,
# MAGIC     c.ff_debt_lt AS long_term_debt,
# MAGIC     ROUND(c.ff_net_inc / NULLIF(c.ff_sales, 0) * 100, 2) AS profit_margin_pct,
# MAGIC     ROUND(c.ff_com_eq / NULLIF(c.ff_assets, 0) * 100, 2) AS equity_ratio_pct,
# MAGIC     ROUND((c.ff_debt_st + c.ff_debt_lt) / NULLIF(c.ff_com_eq, 0), 2) AS debt_to_equity
# MAGIC   FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   JOIN mp_factset_data.ff_v3.ff_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN mp_factset_data.ff_v3.ff_basic_af c ON b.fsym_company_id = c.fsym_id
# MAGIC   WHERE c.ff_date >= '2023-01-01'  -- Recent fiscal year data
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.ff_date DESC) = 1  -- Most recent fiscal data
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   p.customer_id,
# MAGIC   p.ticker_symbol,
# MAGIC   p.shares_held,
# MAGIC   ROUND(p.position_value, 2) AS position_value_usd,
# MAGIC   f.fiscal_date AS latest_fiscal_date,
# MAGIC   ROUND(f.revenue / 1000000, 2) AS revenue_millions,
# MAGIC   ROUND(f.net_income / 1000000, 2) AS net_income_millions,
# MAGIC   f.eps AS earnings_per_share,
# MAGIC   f.profit_margin_pct,
# MAGIC   f.equity_ratio_pct,
# MAGIC   f.debt_to_equity,
# MAGIC   ROUND(p.shares_held * f.eps, 2) AS position_earnings_value,
# MAGIC   CASE
# MAGIC     WHEN f.net_income < 0 THEN 'Unprofitable'
# MAGIC     WHEN f.debt_to_equity > 2 THEN 'High Leverage'
# MAGIC     WHEN f.profit_margin_pct < 5 THEN 'Low Margin'
# MAGIC     ELSE 'Healthy'
# MAGIC   END AS financial_health
# MAGIC FROM portfolio p
# MAGIC JOIN fundamentals_data f ON p.ticker_region = f.ticker_region
# MAGIC ORDER BY p.position_value DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Portfolio + FactSet Estimates
# MAGIC
# MAGIC Combine portfolio with **FactSet Estimates** to see analyst projections and consensus data.
# MAGIC
# MAGIC **Join Pattern:**
# MAGIC ```
# MAGIC Portfolio (ticker) → sym_ticker_region → fe_sec_map → fe_basic_conh_af (estimates)
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Portfolio enriched with analyst consensus estimates
# MAGIC -- Shows forward-looking earnings projections
# MAGIC
# MAGIC WITH portfolio AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     ticker_symbol,
# MAGIC     CONCAT(ticker_symbol, '-US') AS ticker_region,
# MAGIC     shares_held,
# MAGIC     cost_basis,
# MAGIC     shares_held * cost_basis AS position_value
# MAGIC   FROM portfolio_federated.dbo.customer_holdings
# MAGIC ),
# MAGIC estimates_data AS (
# MAGIC   SELECT
# MAGIC     a.ticker_region,
# MAGIC     c.fe_item AS estimate_item,
# MAGIC     c.fe_fp_end AS fiscal_period_end,
# MAGIC     c.fe_cons_mean AS consensus_mean,
# MAGIC     c.fe_cons_median AS consensus_median,
# MAGIC     c.fe_cons_high AS consensus_high,
# MAGIC     c.fe_cons_low AS consensus_low,
# MAGIC     c.fe_cons_stdev AS consensus_std_dev,
# MAGIC     c.fe_cons_est_cnt AS analyst_count,
# MAGIC     c.cons_end_date
# MAGIC   FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   JOIN mp_factset_data.fe_v4.fe_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN mp_factset_data.fe_v4.fe_basic_conh_af c ON b.fsym_company_id = c.fsym_id
# MAGIC   WHERE c.fe_item = 'EPS'  -- Focus on EPS estimates
# MAGIC     AND c.fe_fp_end >= CURRENT_DATE()  -- Future estimates only
# MAGIC     AND c.cons_end_date IS NULL  -- Latest consensus for each period
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   p.customer_id,
# MAGIC   p.ticker_symbol,
# MAGIC   p.shares_held,
# MAGIC   ROUND(p.position_value, 2) AS position_value_usd,
# MAGIC   e.fiscal_period_end,
# MAGIC   e.consensus_mean AS eps_consensus_mean,
# MAGIC   e.consensus_median AS eps_consensus_median,
# MAGIC   e.consensus_high AS eps_high,
# MAGIC   e.consensus_low AS eps_low,
# MAGIC   ROUND(e.consensus_std_dev, 2) AS eps_std_dev,
# MAGIC   e.analyst_count,
# MAGIC   ROUND(p.shares_held * e.consensus_mean, 2) AS projected_earnings_value,
# MAGIC   ROUND((e.consensus_high - e.consensus_low) / NULLIF(e.consensus_mean, 0) * 100, 2) AS estimate_spread_pct,
# MAGIC   CASE
# MAGIC     WHEN e.analyst_count < 3 THEN 'Low Coverage'
# MAGIC     WHEN (e.consensus_high - e.consensus_low) / NULLIF(e.consensus_mean, 0) > 0.5 THEN 'High Uncertainty'
# MAGIC     WHEN e.consensus_mean > 0 THEN 'Positive Outlook'
# MAGIC     ELSE 'Negative Outlook'
# MAGIC   END AS analyst_signal
# MAGIC FROM portfolio p
# MAGIC JOIN estimates_data e ON p.ticker_region = e.ticker_region
# MAGIC WHERE e.fiscal_period_end <= DATE_ADD(CURRENT_DATE(), 365)  -- Next 12 months
# MAGIC ORDER BY p.position_value DESC, e.fiscal_period_end;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Comprehensive Investment Analysis
# MAGIC
# MAGIC Combine historical fundamentals + forward estimates for complete portfolio insights.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Complete investment analysis: Fundamentals + Estimates + Risk Metrics
# MAGIC
# MAGIC WITH portfolio AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     ticker_symbol,
# MAGIC     CONCAT(ticker_symbol, '-US') AS ticker_region,
# MAGIC     shares_held,
# MAGIC     cost_basis,
# MAGIC     shares_held * cost_basis AS position_value,
# MAGIC     purchase_date,
# MAGIC     account_type
# MAGIC   FROM portfolio_federated.dbo.customer_holdings
# MAGIC ),
# MAGIC fundamentals AS (
# MAGIC   SELECT
# MAGIC     a.ticker_region,
# MAGIC     c.ff_date AS fiscal_date,
# MAGIC     c.ff_sales AS revenue,
# MAGIC     c.ff_net_inc AS net_income,
# MAGIC     c.ff_eps_basic AS historical_eps,
# MAGIC     c.ff_oper_cf AS operating_cash_flow,
# MAGIC     c.ff_com_eq AS shareholders_equity,
# MAGIC     c.ff_assets AS total_assets,
# MAGIC     (c.ff_debt_st + c.ff_debt_lt) AS total_debt,
# MAGIC     ROUND(c.ff_net_inc / NULLIF(c.ff_sales, 0) * 100, 2) AS profit_margin_pct,
# MAGIC     ROUND((c.ff_debt_st + c.ff_debt_lt) / NULLIF(c.ff_com_eq, 0), 2) AS debt_to_equity
# MAGIC   FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   JOIN mp_factset_data.ff_v3.ff_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN mp_factset_data.ff_v3.ff_basic_af c ON b.fsym_company_id = c.fsym_id
# MAGIC   WHERE c.ff_date >= '2023-01-01'
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.ff_date DESC) = 1
# MAGIC ),
# MAGIC estimates AS (
# MAGIC   SELECT
# MAGIC     a.ticker_region,
# MAGIC     c.fe_fp_end AS next_fiscal_period,
# MAGIC     c.fe_cons_mean AS eps_estimate,
# MAGIC     c.fe_cons_high AS eps_high,
# MAGIC     c.fe_cons_low AS eps_low,
# MAGIC     c.fe_cons_est_cnt AS analyst_count
# MAGIC   FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   JOIN mp_factset_data.fe_v4.fe_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN mp_factset_data.fe_v4.fe_basic_conh_af c ON b.fsym_company_id = c.fsym_id
# MAGIC   WHERE c.fe_item = 'EPS'
# MAGIC     AND c.cons_end_date IS NULL
# MAGIC     AND c.fe_fp_end >= CURRENT_DATE()
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.fe_fp_end) = 1
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   p.customer_id,
# MAGIC   p.ticker_symbol,
# MAGIC   p.account_type,
# MAGIC   p.shares_held,
# MAGIC   ROUND(p.position_value, 2) AS position_value_usd,
# MAGIC
# MAGIC   -- Historical Fundamentals
# MAGIC   f.fiscal_date AS latest_fiscal_date,
# MAGIC   ROUND(f.revenue / 1000000, 2) AS revenue_mm,
# MAGIC   ROUND(f.net_income / 1000000, 2) AS net_income_mm,
# MAGIC   f.historical_eps,
# MAGIC   f.profit_margin_pct,
# MAGIC   f.debt_to_equity,
# MAGIC
# MAGIC   -- Forward Estimates
# MAGIC   e.next_fiscal_period,
# MAGIC   e.eps_estimate AS forward_eps,
# MAGIC   e.analyst_count,
# MAGIC
# MAGIC   -- Calculated Metrics
# MAGIC   ROUND(p.shares_held * f.historical_eps, 2) AS historical_position_earnings,
# MAGIC   ROUND(p.shares_held * e.eps_estimate, 2) AS estimated_position_earnings,
# MAGIC   ROUND(((e.eps_estimate - f.historical_eps) / NULLIF(f.historical_eps, 0)) * 100, 2) AS eps_growth_pct,
# MAGIC
# MAGIC   -- Investment Signals
# MAGIC   CASE
# MAGIC     WHEN f.net_income < 0 THEN 'Sell - Unprofitable'
# MAGIC     WHEN f.debt_to_equity > 2 THEN 'Hold - High Leverage'
# MAGIC     WHEN ((e.eps_estimate - f.historical_eps) / NULLIF(f.historical_eps, 0)) > 0.15 THEN 'Strong Buy - High Growth'
# MAGIC     WHEN ((e.eps_estimate - f.historical_eps) / NULLIF(f.historical_eps, 0)) > 0 THEN 'Buy - Positive Growth'
# MAGIC     WHEN ((e.eps_estimate - f.historical_eps) / NULLIF(f.historical_eps, 0)) > -0.10 THEN 'Hold - Flat Growth'
# MAGIC     ELSE 'Sell - Declining Earnings'
# MAGIC   END AS investment_recommendation
# MAGIC
# MAGIC FROM portfolio p
# MAGIC JOIN fundamentals f ON p.ticker_region = f.ticker_region
# MAGIC LEFT JOIN estimates e ON p.ticker_region = e.ticker_region
# MAGIC ORDER BY p.position_value DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Customer Portfolio Risk Assessment
# MAGIC
# MAGIC Aggregate portfolio-level metrics to identify risk concentration and health.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer-level portfolio risk summary
# MAGIC
# MAGIC WITH portfolio_analysis AS (
# MAGIC   SELECT
# MAGIC     p.customer_id,
# MAGIC     p.ticker_symbol,
# MAGIC     CONCAT(p.ticker_symbol, '-US') AS ticker_region,
# MAGIC     p.shares_held * p.cost_basis AS position_value,
# MAGIC     f.ff_net_inc AS net_income,
# MAGIC     f.ff_eps_basic AS historical_eps,
# MAGIC     e.fe_cons_mean AS eps_estimate,
# MAGIC     ROUND(((e.fe_cons_mean - f.ff_eps_basic) / NULLIF(f.ff_eps_basic, 0)) * 100, 2) AS eps_growth_pct,
# MAGIC     (f.ff_debt_st + f.ff_debt_lt) / NULLIF(f.ff_com_eq, 0) AS debt_to_equity
# MAGIC   FROM portfolio_federated.dbo.customer_holdings p
# MAGIC   JOIN mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC     ON CONCAT(p.ticker_symbol, '-US') = a.ticker_region
# MAGIC   JOIN mp_factset_data.ff_v3.ff_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY fsym_id ORDER BY ff_date DESC) AS rn
# MAGIC     FROM mp_factset_data.ff_v3.ff_basic_af
# MAGIC     WHERE ff_date >= '2023-01-01'
# MAGIC   ) f ON b.fsym_company_id = f.fsym_id AND f.rn = 1
# MAGIC   LEFT JOIN (
# MAGIC     SELECT a.ticker_region, c.fe_cons_mean
# MAGIC     FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC     JOIN mp_factset_data.fe_v4.fe_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC     JOIN mp_factset_data.fe_v4.fe_basic_conh_af c ON b.fsym_company_id = c.fsym_id
# MAGIC     WHERE c.fe_item = 'EPS'
# MAGIC       AND c.cons_end_date IS NULL
# MAGIC       AND c.fe_fp_end >= CURRENT_DATE()
# MAGIC     QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.fe_fp_end) = 1
# MAGIC   ) e ON a.ticker_region = e.ticker_region
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   COUNT(DISTINCT ticker_symbol) AS num_positions,
# MAGIC   ROUND(SUM(position_value), 2) AS total_portfolio_value_usd,
# MAGIC   ROUND(AVG(eps_growth_pct), 2) AS avg_eps_growth_pct,
# MAGIC   SUM(CASE WHEN net_income < 0 THEN 1 ELSE 0 END) AS unprofitable_holdings,
# MAGIC   SUM(CASE WHEN debt_to_equity > 2 THEN 1 ELSE 0 END) AS high_leverage_holdings,
# MAGIC   SUM(CASE WHEN eps_growth_pct < -10 THEN 1 ELSE 0 END) AS declining_earnings_holdings,
# MAGIC   ROUND(SUM(CASE WHEN net_income < 0 OR eps_growth_pct < -10 THEN position_value ELSE 0 END), 2) AS at_risk_value_usd,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN net_income < 0 OR eps_growth_pct < -10 THEN position_value ELSE 0 END) /
# MAGIC     NULLIF(SUM(position_value), 0) * 100, 2
# MAGIC   ) AS risk_exposure_pct,
# MAGIC   CASE
# MAGIC     WHEN SUM(CASE WHEN net_income < 0 OR eps_growth_pct < -10 THEN position_value ELSE 0 END) /
# MAGIC          NULLIF(SUM(position_value), 0) > 0.30 THEN 'High Risk'
# MAGIC     WHEN SUM(CASE WHEN net_income < 0 OR eps_growth_pct < -10 THEN position_value ELSE 0 END) /
# MAGIC          NULLIF(SUM(position_value), 0) > 0.15 THEN 'Medium Risk'
# MAGIC     ELSE 'Healthy'
# MAGIC   END AS portfolio_health_rating
# MAGIC FROM portfolio_analysis
# MAGIC GROUP BY customer_id
# MAGIC ORDER BY total_portfolio_value_usd DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Deep Dive - Microsoft Portfolio Analysis
# MAGIC
# MAGIC Detailed analysis for a specific holding with historical trends and forward estimates.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Detailed multi-year analysis for Microsoft
# MAGIC
# MAGIC WITH portfolio AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     ticker_symbol,
# MAGIC     shares_held,
# MAGIC     cost_basis,
# MAGIC     purchase_date,
# MAGIC     shares_held * cost_basis AS position_value
# MAGIC   FROM portfolio_federated.dbo.customer_holdings
# MAGIC   WHERE ticker_symbol = 'MSFT'
# MAGIC ),
# MAGIC historical_fundamentals AS (
# MAGIC   SELECT
# MAGIC     c.ff_date AS fiscal_date,
# MAGIC     YEAR(c.ff_date) AS fiscal_year,
# MAGIC     ROUND(c.ff_sales / 1000000, 2) AS revenue_mm,
# MAGIC     ROUND(c.ff_net_inc / 1000000, 2) AS net_income_mm,
# MAGIC     c.ff_eps_basic AS eps,
# MAGIC     ROUND(c.ff_oper_cf / 1000000, 2) AS operating_cf_mm,
# MAGIC     ROUND(c.ff_net_inc / NULLIF(c.ff_sales, 0) * 100, 2) AS profit_margin_pct,
# MAGIC     ROUND(c.ff_com_eq / NULLIF(c.ff_assets, 0) * 100, 2) AS equity_ratio_pct
# MAGIC   FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   JOIN mp_factset_data.ff_v3.ff_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN mp_factset_data.ff_v3.ff_basic_af c ON b.fsym_company_id = c.fsym_id
# MAGIC   WHERE a.ticker_region = 'MSFT-US'
# MAGIC     AND c.ff_date >= '2021-01-01'
# MAGIC ),
# MAGIC forward_estimates AS (
# MAGIC   SELECT
# MAGIC     c.fe_fp_end AS fiscal_period_end,
# MAGIC     c.fe_item AS item,
# MAGIC     c.fe_cons_mean AS consensus_estimate,
# MAGIC     c.fe_cons_high AS high_estimate,
# MAGIC     c.fe_cons_low AS low_estimate,
# MAGIC     c.fe_cons_est_cnt AS analyst_count
# MAGIC   FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   JOIN mp_factset_data.fe_v4.fe_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN mp_factset_data.fe_v4.fe_basic_conh_af c ON b.fsym_company_id = c.fsym_id
# MAGIC   WHERE a.ticker_region = 'MSFT-US'
# MAGIC     AND c.fe_item = 'EPS'
# MAGIC     AND c.cons_end_date IS NULL
# MAGIC     AND c.fe_fp_end >= CURRENT_DATE()
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   p.customer_id,
# MAGIC   p.ticker_symbol AS ticker,
# MAGIC   p.shares_held,
# MAGIC   ROUND(p.position_value, 2) AS position_value_usd,
# MAGIC   p.purchase_date,
# MAGIC
# MAGIC   -- Historical Performance
# MAGIC   h.fiscal_date,
# MAGIC   h.fiscal_year,
# MAGIC   h.revenue_mm,
# MAGIC   h.net_income_mm,
# MAGIC   h.eps AS historical_eps,
# MAGIC   h.operating_cf_mm,
# MAGIC   h.profit_margin_pct,
# MAGIC
# MAGIC   -- Forward Estimates
# MAGIC   f.fiscal_period_end AS estimate_period,
# MAGIC   f.consensus_estimate AS forward_eps,
# MAGIC   f.high_estimate AS forward_eps_high,
# MAGIC   f.low_estimate AS forward_eps_low,
# MAGIC   f.analyst_count,
# MAGIC
# MAGIC   -- Position Impact
# MAGIC   ROUND(p.shares_held * h.eps, 2) AS historical_position_earnings,
# MAGIC   ROUND(p.shares_held * f.consensus_estimate, 2) AS estimated_position_earnings,
# MAGIC
# MAGIC   -- Growth Metrics
# MAGIC   ROUND(((f.consensus_estimate - h.eps) / NULLIF(h.eps, 0)) * 100, 2) AS eps_growth_pct
# MAGIC
# MAGIC FROM portfolio p
# MAGIC CROSS JOIN historical_fundamentals h
# MAGIC LEFT JOIN forward_estimates f ON f.fiscal_period_end IS NOT NULL
# MAGIC ORDER BY h.fiscal_year DESC, f.fiscal_period_end;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Create Production Dashboard View
# MAGIC
# MAGIC Create a unified view combining on-premise portfolio data with FactSet fundamentals and estimates.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create production-ready dashboard view
# MAGIC CREATE OR REPLACE VIEW main.analytics.portfolio_factset_dashboard AS
# MAGIC
# MAGIC WITH portfolio AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     ticker_symbol,
# MAGIC     CONCAT(ticker_symbol, '-US') AS ticker_region,
# MAGIC     shares_held,
# MAGIC     cost_basis,
# MAGIC     shares_held * cost_basis AS position_value,
# MAGIC     purchase_date,
# MAGIC     account_type
# MAGIC   FROM portfolio_federated.dbo.customer_holdings
# MAGIC ),
# MAGIC fundamentals AS (
# MAGIC   SELECT
# MAGIC     a.ticker_region,
# MAGIC     c.ff_date AS fiscal_date,
# MAGIC     c.ff_sales AS revenue,
# MAGIC     c.ff_net_inc AS net_income,
# MAGIC     c.ff_eps_basic AS current_eps,
# MAGIC     c.ff_assets AS total_assets,
# MAGIC     c.ff_com_eq AS shareholders_equity,
# MAGIC     c.ff_oper_cf AS operating_cash_flow,
# MAGIC     (c.ff_debt_st + c.ff_debt_lt) AS total_debt,
# MAGIC     ROUND(c.ff_net_inc / NULLIF(c.ff_sales, 0) * 100, 2) AS profit_margin_pct,
# MAGIC     ROUND(c.ff_oper_cf / NULLIF(c.ff_sales, 0) * 100, 2) AS cash_flow_margin_pct,
# MAGIC     ROUND((c.ff_debt_st + c.ff_debt_lt) / NULLIF(c.ff_com_eq, 0), 2) AS debt_to_equity_ratio,
# MAGIC     ROUND(c.ff_com_eq / NULLIF(c.ff_assets, 0) * 100, 2) AS equity_ratio_pct
# MAGIC   FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   JOIN mp_factset_data.ff_v3.ff_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN mp_factset_data.ff_v3.ff_basic_af c ON b.fsym_company_id = c.fsym_id
# MAGIC   WHERE c.ff_date >= '2023-01-01'
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.ff_date DESC) = 1
# MAGIC ),
# MAGIC estimates AS (
# MAGIC   SELECT
# MAGIC     a.ticker_region,
# MAGIC     c.fe_fp_end AS next_fiscal_period,
# MAGIC     c.fe_cons_mean AS forward_eps,
# MAGIC     c.fe_cons_median AS forward_eps_median,
# MAGIC     c.fe_cons_high AS forward_eps_high,
# MAGIC     c.fe_cons_low AS forward_eps_low,
# MAGIC     c.fe_cons_est_cnt AS analyst_count,
# MAGIC     ROUND((c.fe_cons_high - c.fe_cons_low) / NULLIF(c.fe_cons_mean, 0) * 100, 2) AS estimate_spread_pct
# MAGIC   FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   JOIN mp_factset_data.fe_v4.fe_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN mp_factset_data.fe_v4.fe_basic_conh_af c ON b.fsym_company_id = c.fsym_id
# MAGIC   WHERE c.fe_item = 'EPS'
# MAGIC     AND c.cons_end_date IS NULL
# MAGIC     AND c.fe_fp_end >= CURRENT_DATE()
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.fe_fp_end) = 1
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   -- Portfolio Info
# MAGIC   p.customer_id,
# MAGIC   p.ticker_symbol,
# MAGIC   p.account_type,
# MAGIC   p.shares_held,
# MAGIC   ROUND(p.position_value, 2) AS position_value_usd,
# MAGIC   p.purchase_date,
# MAGIC   DATEDIFF(CURRENT_DATE(), p.purchase_date) AS holding_period_days,
# MAGIC
# MAGIC   -- Financial Health Metrics
# MAGIC   f.fiscal_date AS latest_fiscal_date,
# MAGIC   ROUND(f.revenue / 1000000, 2) AS revenue_mm,
# MAGIC   ROUND(f.net_income / 1000000, 2) AS net_income_mm,
# MAGIC   ROUND(f.operating_cash_flow / 1000000, 2) AS operating_cf_mm,
# MAGIC   f.profit_margin_pct,
# MAGIC   f.cash_flow_margin_pct,
# MAGIC   f.debt_to_equity_ratio,
# MAGIC   f.equity_ratio_pct,
# MAGIC
# MAGIC   -- Current & Forward Performance
# MAGIC   f.current_eps AS eps_current,
# MAGIC   e.forward_eps AS eps_next_period,
# MAGIC   e.next_fiscal_period AS estimate_fiscal_period,
# MAGIC   e.analyst_count,
# MAGIC   e.estimate_spread_pct,
# MAGIC
# MAGIC   -- Position Earnings
# MAGIC   ROUND(p.shares_held * f.current_eps, 2) AS current_position_earnings,
# MAGIC   ROUND(p.shares_held * e.forward_eps, 2) AS estimated_position_earnings,
# MAGIC
# MAGIC   -- Growth & Risk Indicators
# MAGIC   ROUND(((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) * 100, 2) AS eps_growth_pct,
# MAGIC   CASE
# MAGIC     WHEN f.net_income < 0 THEN 'High Risk - Unprofitable'
# MAGIC     WHEN f.debt_to_equity_ratio > 2.5 THEN 'High Risk - Excessive Leverage'
# MAGIC     WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) < -0.15 THEN 'Medium Risk - Declining Earnings'
# MAGIC     WHEN f.profit_margin_pct < 3 THEN 'Medium Risk - Low Margins'
# MAGIC     ELSE 'Low Risk'
# MAGIC   END AS risk_level,
# MAGIC   CASE
# MAGIC     WHEN f.net_income < 0 THEN 'Sell'
# MAGIC     WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) > 0.20 THEN 'Strong Buy'
# MAGIC     WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) > 0.05 THEN 'Buy'
# MAGIC     WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) > -0.10 THEN 'Hold'
# MAGIC     ELSE 'Sell'
# MAGIC   END AS investment_recommendation
# MAGIC
# MAGIC FROM portfolio p
# MAGIC JOIN fundamentals f ON p.ticker_region = f.ticker_region
# MAGIC LEFT JOIN estimates e ON p.ticker_region = e.ticker_region;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the dashboard view
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   ticker_symbol,
# MAGIC   account_type,
# MAGIC   shares_held,
# MAGIC   position_value_usd,
# MAGIC   revenue_mm,
# MAGIC   net_income_mm,
# MAGIC   profit_margin_pct,
# MAGIC   debt_to_equity_ratio,
# MAGIC   eps_current,
# MAGIC   eps_next_period,
# MAGIC   eps_growth_pct,
# MAGIC   analyst_count,
# MAGIC   risk_level,
# MAGIC   investment_recommendation
# MAGIC FROM main.analytics.portfolio_factset_dashboard
# MAGIC ORDER BY position_value_usd DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Portfolio Analytics Dashboard
# MAGIC
# MAGIC Create aggregated metrics and visualizations for executive dashboards.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load the dashboard data
dashboard_df = spark.sql("SELECT * FROM main.analytics.portfolio_factset_dashboard")

# Customer Portfolio Summary
customer_summary = dashboard_df.groupBy("customer_id").agg(
    F.count("ticker_symbol").alias("num_positions"),
    F.round(F.sum("position_value_usd"), 2).alias("total_portfolio_value"),
    F.round(F.avg("eps_growth_pct"), 2).alias("avg_eps_growth"),
    F.round(F.avg("profit_margin_pct"), 2).alias("avg_profit_margin"),
    F.sum(F.when(F.col("risk_level").contains("High"), 1).otherwise(0)).alias("high_risk_positions"),
    F.sum(F.when(F.col("investment_recommendation") == "Sell", 1).otherwise(0)).alias("sell_recommendations"),
    F.round(F.sum(F.when(F.col("risk_level").contains("High"), F.col("position_value_usd")).otherwise(0)), 2).alias("at_risk_value")
).withColumn(
    "risk_exposure_pct",
    F.round((F.col("at_risk_value") / F.col("total_portfolio_value")) * 100, 2)
).withColumn(
    "portfolio_health",
    F.when(F.col("risk_exposure_pct") > 25, "Needs Attention")
     .when(F.col("risk_exposure_pct") > 10, "Monitor Closely")
     .otherwise("Healthy")
)

display(customer_summary)

# COMMAND ----------

# Sector/Ticker Concentration Analysis
ticker_analysis = dashboard_df.groupBy("ticker_symbol").agg(
    F.count("customer_id").alias("num_customers_holding"),
    F.sum("shares_held").alias("total_shares_held"),
    F.round(F.sum("position_value_usd"), 2).alias("total_position_value"),
    F.round(F.avg("eps_growth_pct"), 2).alias("avg_eps_growth"),
    F.avg("analyst_count").alias("avg_analyst_coverage"),
    F.first("investment_recommendation").alias("recommendation")
).orderBy(F.col("total_position_value").desc())

display(ticker_analysis)

# COMMAND ----------

# Risk Matrix: Position Value vs. EPS Growth
risk_matrix = dashboard_df.select(
    "customer_id",
    "ticker_symbol",
    "position_value_usd",
    "eps_growth_pct",
    "profit_margin_pct",
    "debt_to_equity_ratio",
    "risk_level",
    "investment_recommendation"
).orderBy(F.col("position_value_usd").desc())

display(risk_matrix)

# COMMAND ----------

# Performance Metrics by Account Type
account_analysis = dashboard_df.groupBy("account_type").agg(
    F.count("ticker_symbol").alias("num_positions"),
    F.round(F.sum("position_value_usd"), 2).alias("total_value"),
    F.round(F.avg("eps_growth_pct"), 2).alias("avg_growth"),
    F.round(F.avg("profit_margin_pct"), 2).alias("avg_margin"),
    F.sum(F.when(F.col("investment_recommendation") == "Strong Buy", 1).otherwise(0)).alias("strong_buy_count"),
    F.sum(F.when(F.col("investment_recommendation") == "Buy", 1).otherwise(0)).alias("buy_count"),
    F.sum(F.when(F.col("investment_recommendation") == "Hold", 1).otherwise(0)).alias("hold_count"),
    F.sum(F.when(F.col("investment_recommendation") == "Sell", 1).otherwise(0)).alias("sell_count")
)

display(account_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Export for BI Tools (Tableau, Power BI, Looker)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create aggregated summary tables for BI consumption
# MAGIC
# MAGIC -- Customer-level metrics
# MAGIC CREATE OR REPLACE TABLE main.analytics.customer_portfolio_summary AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   COUNT(DISTINCT ticker_symbol) AS num_positions,
# MAGIC   ROUND(SUM(position_value_usd), 2) AS total_portfolio_value,
# MAGIC   ROUND(AVG(eps_growth_pct), 2) AS avg_eps_growth_pct,
# MAGIC   ROUND(AVG(profit_margin_pct), 2) AS avg_profit_margin_pct,
# MAGIC   ROUND(AVG(debt_to_equity_ratio), 2) AS avg_debt_to_equity,
# MAGIC   SUM(CASE WHEN risk_level LIKE '%High%' THEN 1 ELSE 0 END) AS high_risk_count,
# MAGIC   SUM(CASE WHEN investment_recommendation = 'Sell' THEN 1 ELSE 0 END) AS sell_recommendation_count,
# MAGIC   ROUND(SUM(CASE WHEN risk_level LIKE '%High%' THEN position_value_usd ELSE 0 END), 2) AS at_risk_value,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN risk_level LIKE '%High%' THEN position_value_usd ELSE 0 END) /
# MAGIC     NULLIF(SUM(position_value_usd), 0) * 100, 2
# MAGIC   ) AS risk_exposure_pct,
# MAGIC   CURRENT_TIMESTAMP() AS last_updated
# MAGIC FROM main.analytics.portfolio_factset_dashboard
# MAGIC GROUP BY customer_id;
# MAGIC
# MAGIC SELECT * FROM main.analytics.customer_portfolio_summary
# MAGIC ORDER BY total_portfolio_value DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Holdings with action recommendations
# MAGIC CREATE OR REPLACE TABLE main.analytics.holdings_action_items AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   ticker_symbol,
# MAGIC   account_type,
# MAGIC   position_value_usd,
# MAGIC   eps_current,
# MAGIC   eps_next_period,
# MAGIC   eps_growth_pct,
# MAGIC   profit_margin_pct,
# MAGIC   debt_to_equity_ratio,
# MAGIC   analyst_count,
# MAGIC   risk_level,
# MAGIC   investment_recommendation,
# MAGIC   CASE
# MAGIC     WHEN investment_recommendation = 'Sell' THEN 'Action Required'
# MAGIC     WHEN risk_level LIKE '%High%' THEN 'Review Recommended'
# MAGIC     WHEN eps_growth_pct > 20 THEN 'Consider Increasing Position'
# MAGIC     ELSE 'Monitor'
# MAGIC   END AS action_priority,
# MAGIC   CURRENT_TIMESTAMP() AS last_updated
# MAGIC FROM main.analytics.portfolio_factset_dashboard
# MAGIC WHERE investment_recommendation IN ('Sell', 'Strong Buy')
# MAGIC    OR risk_level LIKE '%High%'
# MAGIC ORDER BY
# MAGIC   CASE
# MAGIC     WHEN investment_recommendation = 'Sell' THEN 1
# MAGIC     WHEN risk_level LIKE '%High%' THEN 2
# MAGIC     ELSE 3
# MAGIC   END,
# MAGIC   position_value_usd DESC;
# MAGIC
# MAGIC SELECT * FROM main.analytics.holdings_action_items;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Key Accomplishments
# MAGIC
# MAGIC ### What We Demonstrated:
# MAGIC
# MAGIC 1. ✅ **Portfolio data stayed on-premise** - Zero data migration required
# MAGIC 2. ✅ **FactSet symbology mapping** - Proper joins using ticker_region → fsym_id → fsym_company_id
# MAGIC 3. ✅ **Complete fundamentals integration** - Revenue, earnings, cash flow, debt ratios from ff_v3.ff_basic_af
# MAGIC 4. ✅ **Forward-looking estimates** - Analyst consensus EPS projections from fe_v4.fe_basic_conh_af
# MAGIC 5. ✅ **Automated risk scoring** - Profitability, leverage, and growth trend analysis
# MAGIC 6. ✅ **Investment recommendations** - Buy/Hold/Sell signals based on comprehensive metrics
# MAGIC 7. ✅ **Production dashboards** - Multi-level views for portfolio managers and executives
# MAGIC 8. ✅ **BI tool integration** - Summary tables ready for Tableau, Power BI, Looker
# MAGIC
# MAGIC ### The Value Proposition:
# MAGIC
# MAGIC **Traditional Approach:**
# MAGIC - Copy portfolio data to cloud ❌
# MAGIC - Build complex ETL pipelines ❌
# MAGIC - Manage data duplication ❌
# MAGIC - Deal with compliance headaches ❌
# MAGIC
# MAGIC **Lakehouse Federation:**
# MAGIC - Query data in place ✅
# MAGIC - Zero data movement ✅
# MAGIC - Real-time portfolio analysis ✅
# MAGIC - Maintain security posture ✅
# MAGIC
# MAGIC ## 🏦 Why This Matters for Financial Services
# MAGIC
# MAGIC ### Security & Compliance
# MAGIC - Customer PII stays in approved, compliant databases
# MAGIC - Maintain existing audit trails and access controls
# MAGIC - No additional data governance burden
# MAGIC
# MAGIC ### Operational Efficiency
# MAGIC - No ETL to build and maintain
# MAGIC - Always querying current data
# MAGIC - Faster time to insights
# MAGIC
# MAGIC ### Cost Savings
# MAGIC - No data transfer costs
# MAGIC - No storage duplication
# MAGIC - Reduced infrastructure complexity
# MAGIC
# MAGIC ### Business Agility
# MAGIC - Support hybrid architectures (on-prem + cloud)
# MAGIC - Leverage existing investments
# MAGIC - Enable upstream system integrations without migration

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 FactSet Tables & Fields Used
# MAGIC
# MAGIC ### Symbology (sym_v1)
# MAGIC - **sym_ticker_region**: Maps ticker-region (e.g., 'MSFT-US') to fsym_id
# MAGIC   - `ticker_region`: Standard ticker with region suffix
# MAGIC   - `fsym_id`: FactSet Symbol ID
# MAGIC
# MAGIC ### Fundamentals (ff_v3)
# MAGIC - **ff_sec_map**: Security mapping table
# MAGIC   - `fsym_id` → `fsym_company_id` mapping
# MAGIC - **ff_basic_af**: Annual fundamentals
# MAGIC   - `ff_date`: Fiscal period date
# MAGIC   - `ff_sales`: Revenue
# MAGIC   - `ff_net_inc`: Net income
# MAGIC   - `ff_eps_basic`: Basic EPS
# MAGIC   - `ff_oper_cf`: Operating cash flow
# MAGIC   - `ff_assets`, `ff_com_eq`: Balance sheet items
# MAGIC   - `ff_debt_st`, `ff_debt_lt`: Short/long-term debt
# MAGIC
# MAGIC ### Estimates (fe_v4)
# MAGIC - **fe_sec_map**: Estimates security mapping
# MAGIC   - `fsym_id` → `fsym_company_id` mapping
# MAGIC - **fe_basic_conh_af**: Consensus estimates (annual/historical)
# MAGIC   - `fe_item`: Estimate item (e.g., 'EPS')
# MAGIC   - `fe_fp_end`: Fiscal period end date
# MAGIC   - `fe_cons_mean`, `fe_cons_median`: Consensus estimates
# MAGIC   - `fe_cons_high`, `fe_cons_low`: Estimate range
# MAGIC   - `fe_cons_est_cnt`: Number of analysts
# MAGIC   - `cons_end_date`: NULL for latest consensus
# MAGIC
# MAGIC ## 📈 Example Use Cases
# MAGIC
# MAGIC ### 1. Portfolio Management
# MAGIC - Combine client holdings with FactSet fundamentals (revenue, earnings, margins)
# MAGIC - Layer in analyst consensus estimates for forward-looking view
# MAGIC - Generate buy/sell recommendations based on growth trends and financial health
# MAGIC
# MAGIC ### 2. Risk Management
# MAGIC - Identify unprofitable positions (negative net income)
# MAGIC - Flag high-leverage companies (debt-to-equity > 2.0)
# MAGIC - Alert on declining earnings estimates (EPS growth < -10%)
# MAGIC - Monitor analyst coverage and estimate dispersion
# MAGIC
# MAGIC ### 3. Client Reporting
# MAGIC - Generate personalized portfolio reports with institutional-grade data
# MAGIC - Show position-level earnings contribution
# MAGIC - Display profit margins, cash flow, and balance sheet strength
# MAGIC - Provide forward estimates with analyst consensus
# MAGIC
# MAGIC ### 4. Compliance & Auditing
# MAGIC - Keep sensitive portfolio data in regulated on-premise systems
# MAGIC - Query for compliance checks without data export
# MAGIC - Maintain complete audit trail of data access
# MAGIC - Meet data residency and sovereignty requirements

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Understanding the Join Patterns
# MAGIC
# MAGIC ### Key Insight: ticker_region Format
# MAGIC - FactSet uses `ticker_region` format: ticker + region suffix (e.g., 'MSFT-US', 'AAPL-US')
# MAGIC - On-premise portfolios typically store just the ticker symbol
# MAGIC - Solution: Use `CONCAT(ticker_symbol, '-US')` to create the ticker_region
# MAGIC
# MAGIC ### Fundamentals Join Pattern
# MAGIC ```
# MAGIC portfolio.ticker_symbol + '-US'
# MAGIC   ↓
# MAGIC sym_ticker_region.ticker_region → sym_ticker_region.fsym_id
# MAGIC   ↓
# MAGIC ff_sec_map.fsym_id → ff_sec_map.fsym_company_id
# MAGIC   ↓
# MAGIC ff_basic_af.fsym_id [financial data]
# MAGIC ```
# MAGIC
# MAGIC ### Estimates Join Pattern
# MAGIC ```
# MAGIC portfolio.ticker_symbol + '-US'
# MAGIC   ↓
# MAGIC sym_ticker_region.ticker_region → sym_ticker_region.fsym_id
# MAGIC   ↓
# MAGIC fe_sec_map.fsym_id → fe_sec_map.fsym_company_id
# MAGIC   ↓
# MAGIC fe_basic_conh_af.fsym_id [consensus estimates]
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Complete join pattern for a single ticker
# MAGIC SELECT
# MAGIC   'MSFT' AS portfolio_ticker,
# MAGIC   'MSFT-US' AS ticker_region_format,
# MAGIC   a.fsym_id AS symbology_fsym_id,
# MAGIC   b.fsym_company_id AS mapped_company_id,
# MAGIC   c.ff_date AS fiscal_date,
# MAGIC   c.ff_sales AS revenue,
# MAGIC   c.ff_eps_basic AS eps
# MAGIC FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC JOIN mp_factset_data.ff_v3.ff_sec_map b
# MAGIC   ON a.fsym_id = b.fsym_id
# MAGIC JOIN mp_factset_data.ff_v3.ff_basic_af c
# MAGIC   ON b.fsym_company_id = c.fsym_id
# MAGIC WHERE a.ticker_region = 'MSFT-US'
# MAGIC   AND c.ff_date >= '2023-01-01'
# MAGIC ORDER BY c.ff_date DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Behind the Scenes: Query Optimization

# COMMAND ----------

# MAGIC %sql
# MAGIC -- See how Databricks optimizes federated queries
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   ticker_symbol,
# MAGIC   shares_held
# MAGIC FROM portfolio_federated.dbo.customer_holdings
# MAGIC WHERE customer_id = 1001;

# COMMAND ----------

# MAGIC %md
# MAGIC **Notice in the query plan:**
# MAGIC - **Predicate Pushdown**: `WHERE customer_id = 1001` executes in SQL Server
# MAGIC - **Projection Pushdown**: Only selected columns are transferred
# MAGIC - **JDBC Scan**: Shows data is read directly from external source
# MAGIC
# MAGIC This means minimal data transfer and maximum performance!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Implementation Checklist
# MAGIC
# MAGIC ### 1. Set Up Federation Connection
# MAGIC ```sql
# MAGIC CREATE CONNECTION onprem_sql_connection
# MAGIC TYPE sqlserver
# MAGIC OPTIONS (
# MAGIC   host '<your-server>.database.windows.net',
# MAGIC   port '1433',
# MAGIC   user '<username>',
# MAGIC   password secret('<scope>', '<key>')
# MAGIC );
# MAGIC
# MAGIC CREATE CATALOG portfolio_federated
# MAGIC USING CONNECTION onprem_sql_connection
# MAGIC OPTIONS (database 'PortfolioDB');
# MAGIC ```
# MAGIC
# MAGIC ### 2. Access FactSet Data (mp_factset_data catalog)
# MAGIC - Verify access to: `ff_v3.ff_basic_af`, `ff_v3.ff_sec_map`
# MAGIC - Verify access to: `fe_v4.fe_basic_conh_af`, `fe_v4.fe_sec_map`
# MAGIC - Verify access to: `sym_v1.sym_ticker_region`
# MAGIC
# MAGIC ### 3. Understand Your Portfolio Schema
# MAGIC - Identify ticker column (convert to ticker_region format with '-US' suffix)
# MAGIC - Identify position size, cost basis, customer ID fields
# MAGIC - Determine account type and purchase date if available
# MAGIC
# MAGIC ### 4. Build Core Queries
# MAGIC - Start with fundamentals: sym_ticker_region → ff_sec_map → ff_basic_af
# MAGIC - Add estimates: sym_ticker_region → fe_sec_map → fe_basic_conh_af
# MAGIC - Filter estimates: `fe_item = 'EPS'`, `cons_end_date IS NULL` for latest
# MAGIC
# MAGIC ### 5. Create Production Views
# MAGIC - Use the dashboard view pattern from Step 10
# MAGIC - Add calculated metrics: growth rates, risk scores, recommendations
# MAGIC - Create summary tables for different user personas
# MAGIC
# MAGIC ### 6. Connect BI Tools
# MAGIC - Point Tableau/Power BI to `main.analytics.portfolio_factset_dashboard`
# MAGIC - Use `customer_portfolio_summary` for executive dashboards
# MAGIC - Use `holdings_action_items` for portfolio manager workflows
# MAGIC
# MAGIC ## 📚 Resources
# MAGIC
# MAGIC - [FactSet on Databricks Marketplace](https://marketplace.databricks.com/)
# MAGIC - [Lakehouse Federation Documentation](https://docs.databricks.com/query-federation/)
# MAGIC - [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/)
# MAGIC - [FactSet Data Feeds Documentation](https://www.factset.com/data-feeds)
