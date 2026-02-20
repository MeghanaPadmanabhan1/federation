# Databricks notebook source
# MAGIC %md
# MAGIC # Personal Investment Portfolio Dashboard
# MAGIC ## Powered by Lakehouse Federation + FactSet Financial Data
# MAGIC
# MAGIC **The Challenge:**
# MAGIC - Individual investor maintains equity portfolio in an **on-premise SQL database**
# MAGIC - Wants comprehensive financial analysis using **FactSet institutional-grade data**
# MAGIC - Cannot move sensitive portfolio data to the cloud due to security/privacy requirements
# MAGIC
# MAGIC **The Solution:**
# MAGIC - Use Lakehouse Federation to query on-premise holdings **in place**
# MAGIC - Enrich with FactSet fundamentals, estimates, and analyst consensus
# MAGIC - Generate actionable investment insights and risk assessments

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
# MAGIC ## Step 3: Federation Connection
# MAGIC
# MAGIC The on-premise portfolio is already federated as **mp_portfolio_federated** catalog.

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
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS mp_portfolio_federated
# MAGIC USING CONNECTION azure_sql_federation
# MAGIC OPTIONS (
# MAGIC   database 'oneenvsqldb'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify connection to federated portfolio
# MAGIC SHOW SCHEMAS IN mp_portfolio_federated;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: View Your Portfolio Holdings
# MAGIC
# MAGIC **Schema:** `mp_portfolio_federated.dbo.equity_holdings`
# MAGIC - `instrument_type`: Type of security (equity, etc.)
# MAGIC - `symbol`: Ticker symbol
# MAGIC - `number_of_shares`: Shares held

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Your personal equity holdings (queried from on-premise database)
# MAGIC -- This data NEVER leaves your on-premise SQL server
# MAGIC
# MAGIC SELECT
# MAGIC   instrument_type,
# MAGIC   symbol,
# MAGIC   number_of_shares
# MAGIC FROM mp_portfolio_federated.dbo.equity_holdings
# MAGIC WHERE instrument_type = 'Equity'
# MAGIC ORDER BY number_of_shares DESC;

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
# MAGIC -- Quick test: See if portfolio tickers now match FactSet
# MAGIC WITH my_portfolio AS (
# MAGIC   SELECT 
# MAGIC     symbol,
# MAGIC     UPPER(CONCAT(symbol, '-US')) AS ticker_region
# MAGIC   FROM mp_portfolio_federated.dbo.equity_holdings
# MAGIC   WHERE instrument_type = 'Equity'
# MAGIC   LIMIT 5
# MAGIC )
# MAGIC SELECT 
# MAGIC   p.symbol,
# MAGIC   p.ticker_region,
# MAGIC   s.fsym_id,
# MAGIC   CASE 
# MAGIC     WHEN s.fsym_id IS NOT NULL THEN '✅ FOUND' 
# MAGIC     ELSE '❌ NOT FOUND' 
# MAGIC   END AS status
# MAGIC FROM my_portfolio p
# MAGIC LEFT JOIN mp_factset_data.sym_v1.sym_ticker_region s
# MAGIC   ON p.ticker_region = s.ticker_region;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 12
# MAGIC %sql
# MAGIC -- Portfolio enriched with FactSet Fundamentals
# MAGIC -- Combines your holdings with institutional-grade financial data
# MAGIC
# MAGIC WITH portfolio AS (
# MAGIC   SELECT
# MAGIC     symbol,
# MAGIC     UPPER(symbol) AS ticker_region ,  -- Convert to FactSet format with UPPER case
# MAGIC     number_of_shares AS shares_held,
# MAGIC     instrument_type
# MAGIC   FROM mp_portfolio_federated.dbo.equity_holdings
# MAGIC   WHERE instrument_type = 'Equity'
# MAGIC ),
# MAGIC fundamentals_data AS (
# MAGIC   SELECT
# MAGIC     a.ticker_region,
# MAGIC     c.fsym_id,
# MAGIC     c.DATE AS fiscal_date,
# MAGIC     c.FF_SALES AS revenue,
# MAGIC     c.FF_NET_INCOME AS net_income,
# MAGIC     c.FF_EPS_BASIC AS eps,
# MAGIC     c.FF_ASSETS AS total_assets,
# MAGIC     c.FF_COM_EQ AS shareholders_equity,
# MAGIC     c.FF_FUNDS_OPER_GROSS AS operating_cash_flow,
# MAGIC     c.FF_DEBT_ST AS short_term_debt,
# MAGIC     c.FF_DEBT_LT AS long_term_debt,
# MAGIC     ROUND(c.FF_NET_INCOME / NULLIF(c.FF_SALES, 0) * 100, 2) AS profit_margin_pct,
# MAGIC     ROUND(c.FF_COM_EQ / NULLIF(c.FF_ASSETS, 0) * 100, 2) AS equity_ratio_pct,
# MAGIC     ROUND((c.FF_DEBT_ST + c.FF_DEBT_LT) / NULLIF(c.FF_COM_EQ, 0), 2) AS debt_to_equity
# MAGIC   FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   JOIN mp_factset_data.ff_v3.ff_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN mp_factset_data.ff_v3.ff_basic_af c ON b.fsym_company_id = c.fsym_id
# MAGIC   WHERE c.DATE >= '2023-01-01'  -- Recent fiscal year data
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.DATE DESC) = 1  -- Most recent fiscal data
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   p.symbol,
# MAGIC   p.shares_held,
# MAGIC   f.fiscal_date AS latest_fiscal_date,
# MAGIC
# MAGIC   -- Financial Performance
# MAGIC   ROUND(f.revenue / 1000000, 2) AS revenue_millions,
# MAGIC   ROUND(f.net_income / 1000000, 2) AS net_income_millions,
# MAGIC   f.eps AS earnings_per_share,
# MAGIC   ROUND(p.shares_held * f.eps, 2) AS my_share_of_earnings,
# MAGIC
# MAGIC   -- Profitability Metrics
# MAGIC   f.profit_margin_pct,
# MAGIC
# MAGIC   -- Financial Health
# MAGIC   f.equity_ratio_pct,
# MAGIC   f.debt_to_equity,
# MAGIC   ROUND(f.operating_cash_flow / 1000000, 2) AS operating_cf_millions,
# MAGIC
# MAGIC   -- Overall Assessment
# MAGIC   CASE
# MAGIC     WHEN f.net_income < 0 THEN 'Unprofitable'
# MAGIC     WHEN f.debt_to_equity > 2.5 THEN 'High Leverage Risk'
# MAGIC     WHEN f.profit_margin_pct < 5 THEN 'Low Margin'
# MAGIC     WHEN f.profit_margin_pct > 15 AND f.debt_to_equity < 1.5 THEN 'Strong'
# MAGIC     ELSE 'Healthy'
# MAGIC   END AS financial_health
# MAGIC FROM portfolio p
# MAGIC JOIN fundamentals_data f ON p.ticker_region = f.ticker_region
# MAGIC ORDER BY p.shares_held DESC;

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

# DBTITLE 1,Cell 14
# MAGIC %sql
# MAGIC -- Portfolio enriched with analyst consensus estimates
# MAGIC -- Shows forward-looking earnings projections for your holdings
# MAGIC
# MAGIC WITH portfolio AS (
# MAGIC   SELECT
# MAGIC     symbol,
# MAGIC     UPPER(symbol) AS ticker_region ,
# MAGIC     number_of_shares AS shares_held
# MAGIC   FROM mp_portfolio_federated.dbo.equity_holdings
# MAGIC   WHERE instrument_type = 'Equity'
# MAGIC ),
# MAGIC estimates_data AS (
# MAGIC   SELECT
# MAGIC     a.ticker_region,
# MAGIC     c.FE_ITEM AS estimate_item,
# MAGIC     c.FE_FP_END AS fiscal_period_end,
# MAGIC     c.FE_MEAN AS consensus_mean,
# MAGIC     c.FE_MEDIAN AS consensus_median,
# MAGIC     c.FE_HIGH AS consensus_high,
# MAGIC     c.FE_LOW AS consensus_low,
# MAGIC     c.FE_STD_DEV AS consensus_std_dev,
# MAGIC     c.FE_NUM_EST AS analyst_count,
# MAGIC     c.CONS_END_DATE
# MAGIC   FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   JOIN mp_factset_data.fe_v4.fe_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN mp_factset_data.fe_v4.fe_basic_conh_af c ON b.fsym_company_id = c.fsym_id
# MAGIC   WHERE c.FE_ITEM = 'EPS'  -- Focus on EPS estimates
# MAGIC     AND c.FE_FP_END >= CURRENT_DATE()  -- Future estimates only
# MAGIC     AND c.CONS_END_DATE IS NULL  -- Latest consensus for each period
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   p.symbol,
# MAGIC   p.shares_held,
# MAGIC   e.fiscal_period_end,
# MAGIC
# MAGIC   -- Analyst Consensus
# MAGIC   e.consensus_mean AS eps_consensus,
# MAGIC   e.consensus_median AS eps_median,
# MAGIC   e.consensus_high AS eps_high_estimate,
# MAGIC   e.consensus_low AS eps_low_estimate,
# MAGIC   e.analyst_count AS num_analysts,
# MAGIC
# MAGIC   -- Your Position Impact
# MAGIC   ROUND(p.shares_held * e.consensus_mean, 2) AS my_projected_earnings,
# MAGIC   ROUND(p.shares_held * e.consensus_high, 2) AS my_best_case_earnings,
# MAGIC   ROUND(p.shares_held * e.consensus_low, 2) AS my_worst_case_earnings,
# MAGIC
# MAGIC   -- Analyst Agreement
# MAGIC   ROUND(e.consensus_std_dev, 2) AS eps_std_deviation,
# MAGIC   ROUND((e.consensus_high - e.consensus_low) / NULLIF(e.consensus_mean, 0) * 100, 2) AS estimate_spread_pct,
# MAGIC
# MAGIC   -- Outlook Signal
# MAGIC   CASE
# MAGIC     WHEN e.analyst_count < 3 THEN 'Low Coverage - Limited Data'
# MAGIC     WHEN (e.consensus_high - e.consensus_low) / NULLIF(e.consensus_mean, 0) > 0.5 THEN 'High Uncertainty - Divergent Views'
# MAGIC     WHEN e.consensus_mean > 0 THEN 'Positive Outlook - Profitable Expected'
# MAGIC     ELSE 'Negative Outlook - Losses Expected'
# MAGIC   END AS analyst_signal
# MAGIC FROM portfolio p
# MAGIC JOIN estimates_data e ON p.ticker_region = e.ticker_region
# MAGIC WHERE e.fiscal_period_end <= DATE_ADD(CURRENT_DATE(), 365)  -- Next 12 months
# MAGIC ORDER BY p.shares_held DESC, e.fiscal_period_end;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Complete Portfolio Investment Analysis
# MAGIC
# MAGIC Combine historical fundamentals + forward estimates for comprehensive investment decisions.

# COMMAND ----------

# DBTITLE 1,Cell 17
# MAGIC %sql
# MAGIC -- Complete investment analysis: Fundamentals + Estimates + Risk Metrics
# MAGIC
# MAGIC WITH portfolio AS (
# MAGIC   SELECT
# MAGIC     symbol,
# MAGIC     UPPER(symbol) AS ticker_region ,
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
# MAGIC     c.FF_EPS_BASIC AS historical_eps,
# MAGIC     c.FF_FUNDS_OPER_GROSS AS operating_cash_flow,
# MAGIC     c.FF_COM_EQ AS shareholders_equity,
# MAGIC     c.FF_ASSETS AS total_assets,
# MAGIC     (c.FF_DEBT_ST + c.FF_DEBT_LT) AS total_debt,
# MAGIC     ROUND(c.FF_NET_INCOME / NULLIF(c.FF_SALES, 0) * 100, 2) AS profit_margin_pct,
# MAGIC     ROUND((c.FF_DEBT_ST + c.FF_DEBT_LT) / NULLIF(c.FF_COM_EQ, 0), 2) AS debt_to_equity
# MAGIC   FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   JOIN mp_factset_data.ff_v3.ff_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN mp_factset_data.ff_v3.ff_basic_af c ON b.fsym_company_id = c.fsym_id
# MAGIC   WHERE c.DATE >= '2023-01-01'
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.DATE DESC) = 1
# MAGIC ),
# MAGIC estimates AS (
# MAGIC   SELECT
# MAGIC     a.ticker_region,
# MAGIC     c.FE_FP_END AS next_fiscal_period,
# MAGIC     c.FE_MEAN AS eps_estimate,
# MAGIC     c.FE_HIGH AS eps_high,
# MAGIC     c.FE_LOW AS eps_low,
# MAGIC     c.FE_NUM_EST AS analyst_count
# MAGIC   FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   JOIN mp_factset_data.fe_v4.fe_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN mp_factset_data.fe_v4.fe_basic_conh_af c ON b.fsym_company_id = c.fsym_id
# MAGIC   WHERE c.FE_ITEM = 'EPS'
# MAGIC     AND c.CONS_END_DATE IS NULL
# MAGIC     AND c.FE_FP_END >= CURRENT_DATE()
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.FE_FP_END) = 1
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   p.symbol,
# MAGIC   p.shares_held,
# MAGIC
# MAGIC   -- Historical Fundamentals
# MAGIC   f.fiscal_date AS latest_fiscal_date,
# MAGIC   ROUND(f.revenue / 1000000, 2) AS revenue_mm,
# MAGIC   ROUND(f.net_income / 1000000, 2) AS net_income_mm,
# MAGIC   f.historical_eps AS current_eps,
# MAGIC   f.profit_margin_pct,
# MAGIC   f.debt_to_equity,
# MAGIC   ROUND(f.operating_cash_flow / 1000000, 2) AS operating_cf_mm,
# MAGIC
# MAGIC   -- Forward Estimates
# MAGIC   e.next_fiscal_period AS estimate_period,
# MAGIC   e.eps_estimate AS forward_eps,
# MAGIC   e.analyst_count AS num_analysts,
# MAGIC
# MAGIC   -- Your Position Impact
# MAGIC   ROUND(p.shares_held * f.historical_eps, 2) AS my_current_earnings_share,
# MAGIC   ROUND(p.shares_held * e.eps_estimate, 2) AS my_projected_earnings_share,
# MAGIC   ROUND(p.shares_held * (e.eps_estimate - f.historical_eps), 2) AS my_expected_earnings_growth,
# MAGIC
# MAGIC   -- Growth Metrics
# MAGIC   ROUND(((e.eps_estimate - f.historical_eps) / NULLIF(f.historical_eps, 0)) * 100, 2) AS eps_growth_pct,
# MAGIC
# MAGIC   -- Investment Signals & Recommendations
# MAGIC   CASE
# MAGIC     WHEN f.net_income < 0 THEN 'SELL - Company Unprofitable'
# MAGIC     WHEN f.debt_to_equity > 2.5 THEN 'SELL - Excessive Leverage Risk'
# MAGIC     WHEN ((e.eps_estimate - f.historical_eps) / NULLIF(f.historical_eps, 0)) < -0.15 THEN 'SELL - Declining Earnings Expected'
# MAGIC     WHEN ((e.eps_estimate - f.historical_eps) / NULLIF(f.historical_eps, 0)) > 0.20 THEN 'STRONG BUY - High Growth Expected'
# MAGIC     WHEN ((e.eps_estimate - f.historical_eps) / NULLIF(f.historical_eps, 0)) > 0.05 THEN 'BUY - Positive Growth Expected'
# MAGIC     WHEN ((e.eps_estimate - f.historical_eps) / NULLIF(f.historical_eps, 0)) > -0.10 THEN 'HOLD - Flat Growth'
# MAGIC     ELSE 'HOLD - Monitor Closely'
# MAGIC   END AS investment_recommendation,
# MAGIC
# MAGIC   -- Risk Assessment
# MAGIC   CASE
# MAGIC     WHEN f.net_income < 0 THEN 'High Risk'
# MAGIC     WHEN f.debt_to_equity > 2.5 THEN 'High Risk'
# MAGIC     WHEN f.profit_margin_pct < 3 THEN 'Medium Risk'
# MAGIC     WHEN f.debt_to_equity > 1.5 THEN 'Medium Risk'
# MAGIC     ELSE 'Low Risk'
# MAGIC   END AS risk_level
# MAGIC
# MAGIC FROM portfolio p
# MAGIC JOIN fundamentals f ON p.ticker_region = f.ticker_region
# MAGIC LEFT JOIN estimates e ON p.ticker_region = e.ticker_region
# MAGIC ORDER BY p.shares_held DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Portfolio Risk Assessment & Summary
# MAGIC
# MAGIC Aggregate portfolio-level metrics to understand your overall investment health.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Trace the full join chain with correct join (no CONCAT)
# MAGIC WITH step1 AS (
# MAGIC   SELECT symbol, UPPER(symbol) AS ticker_region
# MAGIC   FROM mp_portfolio_federated.dbo.equity_holdings
# MAGIC   WHERE instrument_type = 'Equity'
# MAGIC ),
# MAGIC step2 AS (
# MAGIC   SELECT s1.symbol, s1.ticker_region, a.fsym_id AS sym_fsym_id
# MAGIC   FROM step1 s1
# MAGIC   LEFT JOIN mp_factset_data.sym_v1.sym_ticker_region a ON s1.ticker_region = a.ticker_region
# MAGIC ),
# MAGIC step3 AS (
# MAGIC   SELECT s2.*, b.fsym_company_id
# MAGIC   FROM step2 s2
# MAGIC   LEFT JOIN mp_factset_data.ff_v3.ff_sec_map b ON s2.sym_fsym_id = b.fsym_id
# MAGIC ),
# MAGIC step4 AS (
# MAGIC   SELECT s3.*, f.fsym_id AS fund_fsym_id
# MAGIC   FROM step3 s3
# MAGIC   LEFT JOIN mp_factset_data.ff_v3.ff_basic_af f 
# MAGIC     ON s3.fsym_company_id = f.fsym_id AND f.DATE >= '2020-01-01'
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   'Portfolio Equities' as step, COUNT(*) as row_count FROM step1
# MAGIC UNION ALL
# MAGIC SELECT 'After sym_ticker_region join', COUNT(*) FROM step2 WHERE sym_fsym_id IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT 'After ff_sec_map join', COUNT(*) FROM step3 WHERE fsym_company_id IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT 'After ff_basic_af join', COUNT(*) FROM step4 WHERE fund_fsym_id IS NOT NULL
# MAGIC ORDER BY step;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if ANY portfolio symbols exist in FactSet
# MAGIC SELECT 
# MAGIC   p.symbol,
# MAGIC   a.ticker_region,
# MAGIC   a.fsym_id
# MAGIC FROM mp_portfolio_federated.dbo.equity_holdings p
# MAGIC JOIN mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   ON UPPER(p.symbol) = a.ticker_region
# MAGIC WHERE p.instrument_type = 'Equity'
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Portfolio risk and diversification summary (FINAL CORRECTED VERSION)
# MAGIC
# MAGIC WITH portfolio_analysis AS (
# MAGIC   SELECT
# MAGIC     p.symbol,
# MAGIC     UPPER(p.symbol) AS ticker_region,
# MAGIC     p.number_of_shares AS shares_held,
# MAGIC     f.FF_NET_INCOME AS net_income,
# MAGIC     f.FF_EPS_BASIC AS historical_eps,
# MAGIC     f.FF_SALES AS revenue,
# MAGIC     f.FF_FUNDS_OPER_GROSS AS operating_cash_flow,
# MAGIC     e.FE_MEAN AS eps_estimate,
# MAGIC     ROUND(((e.FE_MEAN - f.FF_EPS_BASIC) / NULLIF(f.FF_EPS_BASIC, 0)) * 100, 2) AS eps_growth_pct,
# MAGIC     (f.FF_DEBT_ST + f.FF_DEBT_LT) / NULLIF(f.FF_COM_EQ, 0) AS debt_to_equity,
# MAGIC     ROUND(f.FF_NET_INCOME / NULLIF(f.FF_SALES, 0) * 100, 2) AS profit_margin_pct,
# MAGIC     p.number_of_shares * f.FF_EPS_BASIC AS my_current_earnings,
# MAGIC     p.number_of_shares * e.FE_MEAN AS my_projected_earnings
# MAGIC   FROM mp_portfolio_federated.dbo.equity_holdings p
# MAGIC   JOIN mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC     ON UPPER(p.symbol) = a.ticker_region
# MAGIC   JOIN mp_factset_data.ff_v3.ff_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY fsym_id ORDER BY DATE DESC) AS rn
# MAGIC     FROM mp_factset_data.ff_v3.ff_basic_af
# MAGIC     WHERE DATE >= '2020-01-01'
# MAGIC   ) f ON b.fsym_company_id = f.fsym_id AND f.rn = 1
# MAGIC   LEFT JOIN (
# MAGIC     SELECT a.ticker_region, c.FE_MEAN
# MAGIC     FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC     JOIN mp_factset_data.fe_v4.fe_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC     JOIN mp_factset_data.fe_v4.fe_basic_conh_af c ON b.fsym_company_id = c.fsym_id
# MAGIC     WHERE c.FE_ITEM = 'EPS'
# MAGIC       AND c.CONS_END_DATE IS NULL
# MAGIC       AND c.FE_FP_END >= CURRENT_DATE()
# MAGIC     QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.FE_FP_END) = 1
# MAGIC   ) e ON UPPER(p.symbol) = e.ticker_region
# MAGIC   WHERE p.instrument_type = 'Equity'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   'My Portfolio' AS portfolio_name,
# MAGIC   COUNT(DISTINCT symbol) AS total_holdings,
# MAGIC   ROUND(SUM(my_current_earnings), 2) AS total_current_annual_earnings,
# MAGIC   ROUND(SUM(my_projected_earnings), 2) AS total_projected_annual_earnings,
# MAGIC   ROUND((SUM(my_projected_earnings) - SUM(my_current_earnings)) / NULLIF(SUM(my_current_earnings), 0) * 100, 2) AS portfolio_growth_rate_pct,
# MAGIC   ROUND(AVG(eps_growth_pct), 2) AS avg_eps_growth_pct,
# MAGIC   ROUND(AVG(profit_margin_pct), 2) AS avg_profit_margin_pct,
# MAGIC   ROUND(AVG(debt_to_equity), 2) AS avg_debt_to_equity,
# MAGIC   SUM(CASE WHEN net_income < 0 THEN 1 ELSE 0 END) AS unprofitable_companies,
# MAGIC   SUM(CASE WHEN debt_to_equity > 2.5 THEN 1 ELSE 0 END) AS high_leverage_companies,
# MAGIC   SUM(CASE WHEN eps_growth_pct < -10 THEN 1 ELSE 0 END) AS declining_earnings_companies,
# MAGIC   SUM(CASE WHEN operating_cash_flow < 0 THEN 1 ELSE 0 END) AS negative_cash_flow_companies,
# MAGIC   SUM(CASE WHEN eps_growth_pct > 20 THEN 1 ELSE 0 END) AS high_growth_stocks,
# MAGIC   SUM(CASE WHEN profit_margin_pct > 15 AND debt_to_equity < 1.5 THEN 1 ELSE 0 END) AS quality_stocks,
# MAGIC   CASE
# MAGIC     WHEN SUM(CASE WHEN net_income < 0 OR eps_growth_pct < -10 THEN shares_held ELSE 0 END) /
# MAGIC          NULLIF(SUM(shares_held), 0) > 0.30 THEN '⚠️ HIGH RISK - Consider Rebalancing'
# MAGIC     WHEN SUM(CASE WHEN net_income < 0 OR eps_growth_pct < -10 THEN shares_held ELSE 0 END) /
# MAGIC          NULLIF(SUM(shares_held), 0) > 0.15 THEN '⚠️ MEDIUM RISK - Monitor Closely'
# MAGIC     WHEN AVG(eps_growth_pct) > 10 AND AVG(profit_margin_pct) > 10 THEN '✅ EXCELLENT - Strong Growth Portfolio'
# MAGIC     ELSE '✅ HEALTHY - Well Positioned'
# MAGIC   END AS portfolio_health_rating
# MAGIC FROM portfolio_analysis;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Individual Stock Deep Dive
# MAGIC
# MAGIC Multi-year trend analysis for any stock in your portfolio (change symbol as needed).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Detailed multi-year analysis for a specific holding
# MAGIC -- Change 'COLB-US' to any symbol in your portfolio
# MAGIC
# MAGIC WITH my_holding AS (
# MAGIC   SELECT
# MAGIC     symbol,
# MAGIC     number_of_shares AS shares_held
# MAGIC   FROM mp_portfolio_federated.dbo.equity_holdings
# MAGIC   WHERE symbol = 'COLB-US'  -- Use full symbol WITH -US suffix
# MAGIC     AND instrument_type = 'Equity'
# MAGIC ),
# MAGIC historical_fundamentals AS (
# MAGIC   SELECT
# MAGIC     c.DATE AS fiscal_date,
# MAGIC     YEAR(c.DATE) AS fiscal_year,
# MAGIC     ROUND(c.FF_SALES / 1000000, 2) AS revenue_mm,
# MAGIC     ROUND(c.FF_NET_INCOME / 1000000, 2) AS net_income_mm,
# MAGIC     c.FF_EPS_BASIC AS eps,
# MAGIC     ROUND(c.FF_FUNDS_OPER_GROSS / 1000000, 2) AS operating_cf_mm,
# MAGIC     ROUND(c.FF_NET_INCOME / NULLIF(c.FF_SALES, 0) * 100, 2) AS profit_margin_pct,
# MAGIC     ROUND((c.FF_DEBT_ST + c.FF_DEBT_LT) / NULLIF(c.FF_COM_EQ, 0), 2) AS debt_to_equity
# MAGIC   FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   JOIN mp_factset_data.ff_v3.ff_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN mp_factset_data.ff_v3.ff_basic_af c ON b.fsym_company_id = c.fsym_id
# MAGIC   WHERE a.ticker_region = 'COLB-US'  -- Must match symbol above
# MAGIC     AND c.DATE >= '2020-01-01'
# MAGIC ),
# MAGIC forward_estimates AS (
# MAGIC   SELECT
# MAGIC     c.FE_FP_END AS fiscal_period_end,
# MAGIC     c.FE_ITEM AS item,
# MAGIC     c.FE_MEAN AS consensus_estimate,
# MAGIC     c.FE_HIGH AS high_estimate,
# MAGIC     c.FE_LOW AS low_estimate,
# MAGIC     c.FE_NUM_EST AS analyst_count
# MAGIC   FROM mp_factset_data.sym_v1.sym_ticker_region a
# MAGIC   JOIN mp_factset_data.fe_v4.fe_sec_map b ON a.fsym_id = b.fsym_id
# MAGIC   JOIN mp_factset_data.fe_v4.fe_basic_conh_af c ON b.fsym_company_id = c.fsym_id
# MAGIC   WHERE a.ticker_region = 'COLB-US'  -- Must match symbol above
# MAGIC     AND c.FE_ITEM = 'EPS'
# MAGIC     AND c.CONS_END_DATE IS NULL
# MAGIC     AND c.FE_FP_END >= CURRENT_DATE()
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   m.symbol,
# MAGIC   m.shares_held,
# MAGIC   h.fiscal_date,
# MAGIC   h.fiscal_year,
# MAGIC   h.revenue_mm,
# MAGIC   h.net_income_mm,
# MAGIC   h.eps AS historical_eps,
# MAGIC   h.operating_cf_mm,
# MAGIC   h.profit_margin_pct,
# MAGIC   h.debt_to_equity,
# MAGIC   ROUND(m.shares_held * h.eps, 2) AS my_annual_earnings_this_year,
# MAGIC   f.fiscal_period_end AS estimate_period,
# MAGIC   f.consensus_estimate AS forward_eps,
# MAGIC   f.high_estimate AS forward_eps_high,
# MAGIC   f.low_estimate AS forward_eps_low,
# MAGIC   f.analyst_count,
# MAGIC   ROUND(m.shares_held * f.consensus_estimate, 2) AS my_projected_annual_earnings,
# MAGIC   ROUND(((f.consensus_estimate - h.eps) / NULLIF(h.eps, 0)) * 100, 2) AS eps_growth_pct
# MAGIC FROM my_holding m
# MAGIC CROSS JOIN historical_fundamentals h
# MAGIC LEFT JOIN forward_estimates f ON f.fiscal_period_end IS NOT NULL
# MAGIC ORDER BY h.fiscal_year DESC, f.fiscal_period_end;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Create Personal Investment Dashboard View
# MAGIC
# MAGIC Create a unified view combining your on-premise holdings with FactSet financials and estimates.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create production-ready personal portfolio dashboard
# MAGIC CREATE OR REPLACE VIEW mp_catalog.analytics.my_portfolio_dashboard AS
# MAGIC
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
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.FE_FP_END) = 1
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   -- My Holdings
# MAGIC   p.symbol,
# MAGIC   p.shares_held,
# MAGIC
# MAGIC   -- Company Financial Health
# MAGIC   f.fiscal_date AS latest_fiscal_date,
# MAGIC   ROUND(f.revenue / 1000000, 2) AS revenue_mm,
# MAGIC   ROUND(f.net_income / 1000000, 2) AS net_income_mm,
# MAGIC   ROUND(f.operating_cash_flow / 1000000, 2) AS operating_cf_mm,
# MAGIC   f.profit_margin_pct,
# MAGIC   f.cash_flow_margin_pct,
# MAGIC   f.debt_to_equity_ratio,
# MAGIC   f.equity_ratio_pct,
# MAGIC
# MAGIC   -- EPS Performance
# MAGIC   f.current_eps,
# MAGIC   e.forward_eps AS forward_eps,
# MAGIC   e.next_fiscal_period AS next_estimate_period,
# MAGIC   e.analyst_count AS num_analysts_covering,
# MAGIC   e.estimate_spread_pct AS analyst_disagreement_pct,
# MAGIC
# MAGIC   -- My Share of Company Earnings
# MAGIC   ROUND(p.shares_held * f.current_eps, 2) AS my_current_annual_earnings,
# MAGIC   ROUND(p.shares_held * e.forward_eps, 2) AS my_projected_annual_earnings,
# MAGIC   ROUND(p.shares_held * (e.forward_eps - f.current_eps), 2) AS my_expected_earnings_increase,
# MAGIC
# MAGIC   -- Growth Potential
# MAGIC   ROUND(((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) * 100, 2) AS projected_eps_growth_pct,
# MAGIC
# MAGIC   -- Risk Assessment
# MAGIC   CASE
# MAGIC     WHEN f.net_income < 0 THEN '🔴 High Risk - Unprofitable'
# MAGIC     WHEN f.debt_to_equity_ratio > 2.5 THEN '🔴 High Risk - Excessive Debt'
# MAGIC     WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) < -0.15 THEN '🟡 Medium Risk - Declining Earnings'
# MAGIC     WHEN f.profit_margin_pct < 3 THEN '🟡 Medium Risk - Low Margins'
# MAGIC     WHEN f.profit_margin_pct > 15 AND f.debt_to_equity_ratio < 1.5 THEN '🟢 Low Risk - Strong Fundamentals'
# MAGIC     ELSE '🟢 Low Risk'
# MAGIC   END AS risk_assessment,
# MAGIC
# MAGIC   -- Investment Recommendation
# MAGIC   CASE
# MAGIC     WHEN f.net_income < 0 THEN '❌ SELL - Company Losing Money'
# MAGIC     WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) < -0.15 THEN '❌ SELL - Earnings Declining'
# MAGIC     WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) > 0.20 THEN '💰 STRONG BUY - High Growth Expected'
# MAGIC     WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) > 0.05 THEN '✅ BUY - Positive Growth'
# MAGIC     WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) > -0.10 THEN '⏸️ HOLD - Stable'
# MAGIC     ELSE '⚠️ HOLD - Monitor'
# MAGIC   END AS action_recommendation
# MAGIC
# MAGIC FROM portfolio p
# MAGIC JOIN fundamentals f ON p.ticker_region = f.ticker_region
# MAGIC LEFT JOIN estimates e ON p.ticker_region = e.ticker_region;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query your personal dashboard
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
# MAGIC   action_recommendation
# MAGIC FROM mp_catalog.analytics.my_portfolio_dashboard
# MAGIC ORDER BY my_current_annual_earnings DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Portfolio Analytics & Visualizations
# MAGIC
# MAGIC Create insightful analyses and charts for your investment portfolio.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load your dashboard data
dashboard_df = spark.sql("SELECT * FROM mp_catalog.analytics.my_portfolio_dashboard")

# Portfolio Summary Metrics
print("📊 MY PORTFOLIO SUMMARY")
print("=" * 80)

summary = dashboard_df.agg(
    F.count("symbol").alias("total_positions"),
    F.round(F.sum("my_current_annual_earnings"), 2).alias("total_current_annual_earnings"),
    F.round(F.sum("my_projected_annual_earnings"), 2).alias("total_projected_annual_earnings"),
    F.round(F.avg("projected_eps_growth_pct"), 2).alias("avg_growth_rate"),
    F.round(F.avg("profit_margin_pct"), 2).alias("avg_profit_margin"),
    F.round(F.avg("debt_to_equity_ratio"), 2).alias("avg_debt_to_equity"),
    F.sum(F.when(F.col("risk_assessment").contains("High"), 1).otherwise(0)).alias("high_risk_stocks"),
    F.sum(F.when(F.col("action_recommendation").contains("SELL"), 1).otherwise(0)).alias("sell_recommendations"),
    F.sum(F.when(F.col("action_recommendation").contains("BUY"), 1).otherwise(0)).alias("buy_opportunities")
)

display(summary)

# COMMAND ----------

# Individual Stock Performance Analysis
print("📈 STOCK-BY-STOCK ANALYSIS")
print("=" * 80)

stock_analysis = dashboard_df.select(
    "symbol",
    "shares_held",
    "my_current_annual_earnings",
    "my_projected_annual_earnings",
    "projected_eps_growth_pct",
    "profit_margin_pct",
    "debt_to_equity_ratio",
    "num_analysts_covering",
    "risk_assessment",
    "action_recommendation"
).orderBy(F.col("my_current_annual_earnings").desc())

display(stock_analysis)

# COMMAND ----------

# Top Growth Opportunities
print("🚀 TOP GROWTH OPPORTUNITIES IN YOUR PORTFOLIO")
print("=" * 80)

growth_stocks = dashboard_df.filter(
    F.col("projected_eps_growth_pct") > 10
).select(
    "symbol",
    "shares_held",
    "projected_eps_growth_pct",
    "my_expected_earnings_increase",
    "profit_margin_pct",
    "risk_assessment",
    "action_recommendation"
).orderBy(F.col("projected_eps_growth_pct").desc())

display(growth_stocks)

# COMMAND ----------

# Risk Alert: Positions Needing Attention
print("⚠️ POSITIONS REQUIRING ATTENTION")
print("=" * 80)

risk_alerts = dashboard_df.filter(
    F.col("risk_assessment").contains("High") |
    F.col("action_recommendation").contains("SELL")
).select(
    "symbol",
    "shares_held",
    "my_current_annual_earnings",
    "projected_eps_growth_pct",
    "profit_margin_pct",
    "debt_to_equity_ratio",
    "risk_assessment",
    "action_recommendation"
).orderBy(F.col("my_current_annual_earnings").desc())

display(risk_alerts)

# COMMAND ----------

# Quality Stocks: Strong Fundamentals
print("💎 QUALITY STOCKS - STRONG FUNDAMENTALS")
print("=" * 80)

quality_stocks = dashboard_df.filter(
    (F.col("profit_margin_pct") > 15) &
    (F.col("debt_to_equity_ratio") < 1.5)
).select(
    "symbol",
    "profit_margin_pct",
    "debt_to_equity_ratio",
    "projected_eps_growth_pct",
    "my_current_annual_earnings",
    "risk_assessment"
).orderBy(F.col("profit_margin_pct").desc())

display(quality_stocks)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Create Dashboard Tables for Visualization Tools

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Portfolio Summary Table
# MAGIC CREATE OR REPLACE TABLE mp_catalog.analytics.my_portfolio_summary AS
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
# MAGIC FROM mp_catalog.analytics.my_portfolio_dashboard;
# MAGIC
# MAGIC SELECT * FROM mp_catalog.analytics.my_portfolio_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Action Items Table - Prioritized by Urgency
# MAGIC CREATE OR REPLACE TABLE mp_catalog.analytics.my_action_items AS
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
# MAGIC     WHEN action_recommendation LIKE '%SELL%' THEN '🔴 URGENT - Consider Selling'
# MAGIC     WHEN risk_assessment LIKE '%High%' THEN '🟡 REVIEW - High Risk'
# MAGIC     WHEN projected_eps_growth_pct > 25 THEN '🟢 OPPORTUNITY - Strong Growth'
# MAGIC     WHEN action_recommendation LIKE '%STRONG BUY%' THEN '💰 OPPORTUNITY - Consider Buying More'
# MAGIC     ELSE '⚪ MONITOR - Stable'
# MAGIC   END AS priority_action,
# MAGIC   CURRENT_TIMESTAMP() AS last_updated
# MAGIC FROM mp_catalog.analytics.my_portfolio_dashboard
# MAGIC WHERE action_recommendation LIKE '%SELL%'
# MAGIC    OR action_recommendation LIKE '%STRONG BUY%'
# MAGIC    OR risk_assessment LIKE '%High%'
# MAGIC    OR projected_eps_growth_pct > 20
# MAGIC ORDER BY
# MAGIC   CASE
# MAGIC     WHEN action_recommendation LIKE '%SELL%' THEN 1
# MAGIC     WHEN risk_assessment LIKE '%High%' THEN 2
# MAGIC     WHEN projected_eps_growth_pct > 25 THEN 3
# MAGIC     ELSE 4
# MAGIC   END,
# MAGIC   my_current_annual_earnings DESC;
# MAGIC
# MAGIC SELECT * FROM mp_catalog.analytics.my_action_items;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Stock Performance Rankings
# MAGIC CREATE OR REPLACE TABLE mp_catalog.analytics.my_stock_rankings AS
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   shares_held,
# MAGIC   my_current_annual_earnings AS annual_earnings_contribution,
# MAGIC   projected_eps_growth_pct AS growth_rate,
# MAGIC   profit_margin_pct,
# MAGIC   debt_to_equity_ratio,
# MAGIC   num_analysts_covering,
# MAGIC
# MAGIC   -- Performance Score (100 point scale)
# MAGIC   ROUND(
# MAGIC     (CASE WHEN projected_eps_growth_pct > 0 THEN LEAST(projected_eps_growth_pct, 30) ELSE 0 END) +  -- Growth (30 pts)
# MAGIC     (CASE WHEN profit_margin_pct > 0 THEN LEAST(profit_margin_pct, 30) ELSE 0 END) +  -- Profitability (30 pts)
# MAGIC     (CASE WHEN debt_to_equity_ratio < 2 THEN 20 ELSE 10 END) +  -- Financial Health (20 pts)
# MAGIC     (CASE WHEN num_analysts_covering >= 10 THEN 20 ELSE num_analysts_covering * 2 END), -- Coverage (20 pts)
# MAGIC   0) AS performance_score,
# MAGIC
# MAGIC   risk_assessment,
# MAGIC   action_recommendation
# MAGIC FROM mp_catalog.analytics.my_portfolio_dashboard
# MAGIC ORDER BY performance_score DESC;
# MAGIC
# MAGIC SELECT * FROM mp_catalog.analytics.my_stock_rankings;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13: Create AI/BI Dashboard
# MAGIC
# MAGIC Generate a comprehensive Databricks AI/BI Dashboard with key insights and visualizations.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- AI/BI Dashboard: Portfolio Overview
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
# MAGIC FROM mp_catalog.analytics.my_portfolio_dashboard;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- AI/BI Dashboard: Risk Distribution
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
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- AI/BI Dashboard: Investment Actions Distribution
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
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- AI/BI Dashboard: Top Performers and Laggards
# MAGIC CREATE OR REPLACE VIEW mp_catalog.analytics.aibi_stock_performance AS
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   shares_held,
# MAGIC   my_current_annual_earnings,
# MAGIC   my_projected_annual_earnings,
# MAGIC   projected_eps_growth_pct,
# MAGIC   profit_margin_pct,
# MAGIC   CASE
# MAGIC     WHEN projected_eps_growth_pct > 15 THEN '🚀 Top Performer'
# MAGIC     WHEN projected_eps_growth_pct > 5 THEN '📈 Above Average'
# MAGIC     WHEN projected_eps_growth_pct > -5 THEN '➡️ Stable'
# MAGIC     ELSE '📉 Underperformer'
# MAGIC   END AS performance_tier
# MAGIC FROM mp_catalog.analytics.my_portfolio_dashboard
# MAGIC ORDER BY projected_eps_growth_pct DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Create Databricks AI/BI Dashboard
# MAGIC
# MAGIC Use these queries to create interactive visualizations in Databricks AI/BI Dashboards:
# MAGIC
# MAGIC **Dashboard Components:**
# MAGIC
# MAGIC 1. **Portfolio Health KPIs** (Counter Visualization)
# MAGIC    - Total Holdings
# MAGIC    - Current Annual Earnings
# MAGIC    - Projected Growth %
# MAGIC    - Query: `SELECT * FROM main.analytics.aibi_portfolio_overview`
# MAGIC
# MAGIC 2. **Risk Distribution** (Pie Chart)
# MAGIC    - Shows portfolio allocation across risk levels
# MAGIC    - Query: `SELECT * FROM main.analytics.aibi_risk_distribution`
# MAGIC
# MAGIC 3. **Investment Actions** (Bar Chart)
# MAGIC    - Distribution of Buy/Hold/Sell recommendations
# MAGIC    - Query: `SELECT * FROM main.analytics.aibi_action_distribution`
# MAGIC
# MAGIC 4. **Stock Performance** (Table with Color Coding)
# MAGIC    - Ranked list of holdings by growth potential
# MAGIC    - Query: `SELECT * FROM main.analytics.aibi_stock_performance`
# MAGIC
# MAGIC 5. **Earnings Waterfall** (Combo Chart)
# MAGIC    - Current vs Projected earnings by stock
# MAGIC    - Query: `SELECT symbol, my_current_annual_earnings, my_projected_annual_earnings FROM main.analytics.my_portfolio_dashboard`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample Dashboard Query: Portfolio Performance Summary
# MAGIC SELECT
# MAGIC   p.symbol,
# MAGIC   p.my_current_annual_earnings AS current_earnings,
# MAGIC   p.my_projected_annual_earnings AS projected_earnings,
# MAGIC   p.projected_eps_growth_pct AS growth_rate,
# MAGIC   p.risk_assessment,
# MAGIC   p.action_recommendation,
# MAGIC   r.performance_score
# MAGIC FROM mp_catalog.analytics.my_portfolio_dashboard p
# MAGIC JOIN mp_catalog.analytics.my_stock_rankings r ON p.symbol = r.symbol
# MAGIC ORDER BY r.performance_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 What You Now Have
# MAGIC
# MAGIC ### Your Personal Investment Intelligence Platform:
# MAGIC
# MAGIC 1. ✅ **Privacy-Preserving Architecture** - Your holdings stay on-premise, zero data movement to cloud
# MAGIC 2. ✅ **Institutional-Grade Data** - FactSet financials, estimates, and analyst consensus for all your stocks
# MAGIC 3. ✅ **Comprehensive Financial Analysis** - Revenue, earnings, cash flow, debt ratios, profit margins
# MAGIC 4. ✅ **Forward-Looking Insights** - Analyst EPS projections showing expected earnings growth
# MAGIC 5. ✅ **Personalized Earnings Impact** - See YOUR share of company earnings, not just per-share metrics
# MAGIC 6. ✅ **Automated Risk Assessment** - Identify unprofitable, overleveraged, or declining positions
# MAGIC 7. ✅ **Actionable Recommendations** - Clear Buy/Hold/Sell signals based on growth and financial health
# MAGIC 8. ✅ **Interactive Dashboards** - AI/BI visualizations showing portfolio health, risk distribution, opportunities
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
# MAGIC ## 💡 Key Insights You Can Now Access
# MAGIC
# MAGIC ### 📊 Portfolio Health Monitoring
# MAGIC - **Total Earnings Power**: See aggregate annual earnings across all holdings
# MAGIC - **Growth Trajectory**: Portfolio-wide projected growth rate
# MAGIC - **Quality Metrics**: Average profit margins and leverage across your portfolio
# MAGIC - **Diversification**: Holdings count and concentration analysis
# MAGIC
# MAGIC ### ⚠️ Risk Intelligence
# MAGIC - **Unprofitable Companies**: Identify money-losing investments immediately
# MAGIC - **Leverage Concerns**: Flag overleveraged companies (debt-to-equity > 2.5)
# MAGIC - **Declining Earnings**: Spot companies with negative growth projections
# MAGIC - **Analyst Coverage**: Know which stocks have limited analyst attention
# MAGIC
# MAGIC ### 💰 Investment Opportunities
# MAGIC - **High Growth Stocks**: Find positions with >20% projected earnings growth
# MAGIC - **Quality Plays**: Identify high-margin, low-debt companies
# MAGIC - **Undervalued Gems**: Stocks with strong fundamentals but low coverage
# MAGIC - **Analyst Consensus**: Understand where analysts are bullish or bearish
# MAGIC
# MAGIC ### 🎯 Actionable Decisions
# MAGIC - **Clear Signals**: Automatic Buy/Hold/Sell recommendations
# MAGIC - **Prioritized Actions**: Know which positions need immediate attention
# MAGIC - **Performance Scoring**: Rank your holdings objectively
# MAGIC - **Expected Impact**: See how each decision affects YOUR earnings share

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
# MAGIC ## 🚀 Quick Start Guide
# MAGIC
# MAGIC ### ✅ What's Already Set Up
# MAGIC - **Federated Catalog**: `mp_portfolio_federated` connected to your on-premise database
# MAGIC - **FactSet Data**: `mp_factset_data` catalog with fundamentals and estimates
# MAGIC - **Dashboard View**: `main.analytics.my_portfolio_dashboard` with all metrics
# MAGIC - **Summary Tables**: Pre-built tables for visualizations
# MAGIC
# MAGIC ### 📊 Access Your Dashboards
# MAGIC
# MAGIC **Main Dashboard:**
# MAGIC ```sql
# MAGIC SELECT * FROM main.analytics.my_portfolio_dashboard
# MAGIC ORDER BY my_current_annual_earnings DESC;
# MAGIC ```
# MAGIC
# MAGIC **Portfolio Summary:**
# MAGIC ```sql
# MAGIC SELECT * FROM main.analytics.my_portfolio_summary;
# MAGIC ```
# MAGIC
# MAGIC **Action Items (What to do now):**
# MAGIC ```sql
# MAGIC SELECT * FROM main.analytics.my_action_items;
# MAGIC ```
# MAGIC
# MAGIC **Stock Rankings:**
# MAGIC ```sql
# MAGIC SELECT * FROM main.analytics.my_stock_rankings;
# MAGIC ```
# MAGIC
# MAGIC ### 🎨 Create AI/BI Dashboard
# MAGIC
# MAGIC 1. Navigate to **Databricks AI/BI Dashboards**
# MAGIC 2. Click **Create Dashboard**
# MAGIC 3. Add visualizations using these queries:
# MAGIC    - Portfolio Overview: `main.analytics.aibi_portfolio_overview`
# MAGIC    - Risk Distribution: `main.analytics.aibi_risk_distribution`
# MAGIC    - Action Items: `main.analytics.aibi_action_distribution`
# MAGIC    - Stock Performance: `main.analytics.aibi_stock_performance`
# MAGIC
# MAGIC ### 🔄 Keep Data Fresh
# MAGIC
# MAGIC **Daily Refresh:**
# MAGIC ```sql
# MAGIC -- Create a scheduled job to refresh your views
# MAGIC REFRESH TABLE main.analytics.my_portfolio_dashboard;
# MAGIC REFRESH TABLE main.analytics.my_portfolio_summary;
# MAGIC REFRESH TABLE main.analytics.my_action_items;
# MAGIC REFRESH TABLE main.analytics.my_stock_rankings;
# MAGIC ```
# MAGIC
# MAGIC ### 🎯 Analyze Specific Stocks
# MAGIC
# MAGIC **Deep Dive Template** (Change symbol in Step 9):
# MAGIC - See multi-year financial trends
# MAGIC - Compare historical vs. projected earnings
# MAGIC - Understand YOUR specific earnings share
# MAGIC
# MAGIC ## 📈 Your Investment Intelligence Toolkit
# MAGIC
# MAGIC ### 📊 Views & Tables Created
# MAGIC
# MAGIC | Object | Purpose | Query |
# MAGIC |--------|---------|-------|
# MAGIC | `my_portfolio_dashboard` | Complete holding-level analysis | All financial metrics + recommendations |
# MAGIC | `my_portfolio_summary` | Portfolio-level KPIs | Total earnings, growth rate, risk counts |
# MAGIC | `my_action_items` | Prioritized to-do list | Stocks needing immediate attention |
# MAGIC | `my_stock_rankings` | Performance scoring | 100-point scale ranking system |
# MAGIC | `aibi_portfolio_overview` | Dashboard KPI metrics | For counter visualizations |
# MAGIC | `aibi_risk_distribution` | Risk breakdown | For pie charts |
# MAGIC | `aibi_action_distribution` | Recommendation split | For bar charts |
# MAGIC | `aibi_stock_performance` | Performance tiers | For categorization |
# MAGIC
# MAGIC ### 🎯 Key Metrics You Can Track
# MAGIC
# MAGIC **Earnings Metrics:**
# MAGIC - `my_current_annual_earnings`: Your share of company's annual profits (current)
# MAGIC - `my_projected_annual_earnings`: Your share based on analyst estimates (forward)
# MAGIC - `my_expected_earnings_increase`: Dollar increase you can expect
# MAGIC
# MAGIC **Growth Indicators:**
# MAGIC - `projected_eps_growth_pct`: Expected earnings growth rate
# MAGIC - `portfolio_growth_rate_pct`: Your entire portfolio's growth trajectory
# MAGIC
# MAGIC **Quality Metrics:**
# MAGIC - `profit_margin_pct`: Company profitability efficiency
# MAGIC - `debt_to_equity_ratio`: Financial leverage and risk
# MAGIC - `cash_flow_margin_pct`: Operating cash generation
# MAGIC - `performance_score`: Composite 0-100 ranking
# MAGIC
# MAGIC **Risk Signals:**
# MAGIC - `risk_assessment`: High/Medium/Low risk classification
# MAGIC - `action_recommendation`: Buy/Hold/Sell signals
# MAGIC - `num_analysts_covering`: Analyst attention level
# MAGIC
# MAGIC ### 🔗 Next Steps
# MAGIC
# MAGIC 1. **Review Your Dashboard**: Run queries in Step 11 to see your portfolio health
# MAGIC 2. **Check Action Items**: See which stocks need attention (Step 12)
# MAGIC 3. **Create AI/BI Dashboard**: Build visual dashboards (Step 13)
# MAGIC 4. **Deep Dive Analysis**: Analyze individual stocks (Step 9)
# MAGIC 5. **Set Up Alerts**: Create scheduled jobs to monitor changes
# MAGIC
# MAGIC ### 📚 Resources
# MAGIC
# MAGIC - [FactSet Data Documentation](https://www.factset.com/data-feeds)
# MAGIC - [Lakehouse Federation Guide](https://docs.databricks.com/query-federation/)
# MAGIC - [Databricks AI/BI Dashboards](https://docs.databricks.com/dashboards/)
# MAGIC - [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/)
# MAGIC
# MAGIC
