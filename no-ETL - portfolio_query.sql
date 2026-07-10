-- Databricks notebook source
-- MAGIC %md
-- MAGIC # no-ETL - Portfolio Query
-- MAGIC
-- MAGIC The whole demo, without a workflow. One SQL query joins:
-- MAGIC
-- MAGIC - **`mp_portfolio_federated.dbo.equity_holdings`** — on-prem SQL Server, via Lakehouse Federation (data stays on-prem)
-- MAGIC - **`mp_factset_data.sym_v1.sym_ticker_region`** — FactSet symbology, via Databricks Marketplace
-- MAGIC - **`mp_factset_data.ff_v3.ff_basic_af`** — FactSet fundamentals, via Databricks Marketplace
-- MAGIC - **`mp_factset_data.fe_v4.fe_basic_conh_af`** — FactSet analyst estimates, via Databricks Marketplace
-- MAGIC
-- MAGIC Nothing is materialized. No views. No workflow. Every query below evaluates live against the foreign catalog + marketplace catalog. This is the *simplest* way to consume the demo — vanilla SQL against Unity Catalog, with federation making both sources look like the same warehouse.
-- MAGIC
-- MAGIC The companion dashboard `no-ETL - portfolio_dashboard` uses the same joins inline in each widget's dataset.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The full per-holding view — federated holdings × FactSet fundamentals × FactSet estimates

-- COMMAND ----------

WITH portfolio AS (
  SELECT
    symbol,
    UPPER(symbol) AS ticker_region,
    number_of_shares AS shares_held
  FROM mp_portfolio_federated.dbo.equity_holdings   -- <- on-prem, federated
  WHERE instrument_type = 'Equity'
),
fundamentals AS (
  SELECT
    a.ticker_region,
    c.DATE                                          AS fiscal_date,
    c.FF_SALES                                      AS revenue,
    c.FF_NET_INCOME                                 AS net_income,
    c.FF_EPS_BASIC                                  AS current_eps,
    ROUND(c.FF_NET_INCOME / NULLIF(c.FF_SALES, 0) * 100, 2)                                AS profit_margin_pct,
    ROUND((c.FF_DEBT_ST + c.FF_DEBT_LT) / NULLIF(c.FF_COM_EQ, 0), 2)                       AS debt_to_equity_ratio
  FROM mp_factset_data.sym_v1.sym_ticker_region a
  JOIN mp_factset_data.ff_v3.ff_sec_map         b ON a.fsym_id = b.fsym_id
  JOIN mp_factset_data.ff_v3.ff_basic_af        c ON b.fsym_company_id = c.fsym_id
  WHERE c.DATE >= '2023-01-01'
    AND c.FF_EPS_BASIC IS NOT NULL
    AND ABS(c.FF_EPS_BASIC) < 50         -- exclude implausible FactSet EPS rows for a few micro-caps
    AND c.FF_SALES > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.DATE DESC) = 1
),
estimates AS (
  SELECT
    a.ticker_region,
    c.FE_MEAN     AS forward_eps,
    c.FE_NUM_EST  AS analyst_count
  FROM mp_factset_data.sym_v1.sym_ticker_region a
  JOIN mp_factset_data.fe_v4.fe_sec_map         b ON a.fsym_id = b.fsym_id
  JOIN mp_factset_data.fe_v4.fe_basic_conh_af   c ON b.fsym_company_id = c.fsym_id
  WHERE c.FE_ITEM = 'EPS'
    AND c.CONS_END_DATE IS NULL
    AND c.FE_FP_END >= CURRENT_DATE()
    AND ABS(c.FE_MEAN) < 50
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.FE_FP_END) = 1
)
SELECT
  p.symbol,
  p.shares_held,
  f.current_eps,
  e.forward_eps,
  e.analyst_count                                                      AS num_analysts_covering,
  ROUND(p.shares_held * f.current_eps, 2)                              AS my_current_annual_earnings,
  ROUND(p.shares_held * e.forward_eps, 2)                              AS my_projected_annual_earnings,
  ROUND(((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) * 100, 2) AS projected_eps_growth_pct,
  f.profit_margin_pct,
  f.debt_to_equity_ratio,
  CASE
    WHEN f.net_income < 0                                                                THEN 'High Risk - Unprofitable'
    WHEN f.debt_to_equity_ratio > 2.5                                                    THEN 'High Risk - Excessive Debt'
    WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) < -0.15            THEN 'Medium Risk - Declining Earnings'
    WHEN f.profit_margin_pct < 3                                                         THEN 'Medium Risk - Low Margins'
    WHEN f.profit_margin_pct > 15 AND f.debt_to_equity_ratio < 1.5                       THEN 'Low Risk - Strong Fundamentals'
    ELSE 'Low Risk'
  END                                                                  AS risk_assessment,
  CASE
    WHEN f.net_income < 0                                                                THEN 'SELL - Company Losing Money'
    WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) < -0.15            THEN 'SELL - Earnings Declining'
    WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) > 0.20             THEN 'STRONG BUY - High Growth Expected'
    WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) > 0.05             THEN 'BUY - Positive Growth'
    WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) > -0.10            THEN 'HOLD - Stable'
    ELSE 'HOLD - Monitor'
  END                                                                  AS action_recommendation
FROM portfolio p
JOIN fundamentals f  ON p.ticker_region = f.ticker_region
LEFT JOIN estimates e ON p.ticker_region = e.ticker_region
ORDER BY my_current_annual_earnings DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Portfolio-level summary — one row rolled up from the above

-- COMMAND ----------

WITH per_holding AS (
  WITH portfolio AS (
    SELECT symbol, UPPER(symbol) AS ticker_region, number_of_shares AS shares_held
    FROM mp_portfolio_federated.dbo.equity_holdings
    WHERE instrument_type = 'Equity'
  ),
  fundamentals AS (
    SELECT a.ticker_region, c.FF_NET_INCOME AS net_income, c.FF_EPS_BASIC AS current_eps,
           ROUND(c.FF_NET_INCOME / NULLIF(c.FF_SALES, 0) * 100, 2) AS profit_margin_pct,
           ROUND((c.FF_DEBT_ST + c.FF_DEBT_LT) / NULLIF(c.FF_COM_EQ, 0), 2) AS debt_to_equity_ratio
    FROM mp_factset_data.sym_v1.sym_ticker_region a
    JOIN mp_factset_data.ff_v3.ff_sec_map         b ON a.fsym_id = b.fsym_id
    JOIN mp_factset_data.ff_v3.ff_basic_af        c ON b.fsym_company_id = c.fsym_id
    WHERE c.DATE >= '2023-01-01' AND c.FF_EPS_BASIC IS NOT NULL AND ABS(c.FF_EPS_BASIC) < 50 AND c.FF_SALES > 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.DATE DESC) = 1
  ),
  estimates AS (
    SELECT a.ticker_region, c.FE_MEAN AS forward_eps
    FROM mp_factset_data.sym_v1.sym_ticker_region a
    JOIN mp_factset_data.fe_v4.fe_sec_map         b ON a.fsym_id = b.fsym_id
    JOIN mp_factset_data.fe_v4.fe_basic_conh_af   c ON b.fsym_company_id = c.fsym_id
    WHERE c.FE_ITEM = 'EPS' AND c.CONS_END_DATE IS NULL AND c.FE_FP_END >= CURRENT_DATE() AND ABS(c.FE_MEAN) < 50
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.FE_FP_END) = 1
  )
  SELECT p.symbol, p.shares_held * f.current_eps AS current, p.shares_held * e.forward_eps AS projected,
         f.net_income, f.debt_to_equity_ratio, f.profit_margin_pct,
         ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) * 100 AS growth_pct
  FROM portfolio p JOIN fundamentals f ON p.ticker_region = f.ticker_region
  LEFT JOIN estimates e ON p.ticker_region = e.ticker_region
)
SELECT
  COUNT(*)                              AS total_holdings,
  ROUND(SUM(current), 2)                AS total_current_earnings,
  ROUND(SUM(projected), 2)              AS total_projected_earnings,
  ROUND(AVG(growth_pct), 2)             AS avg_growth_rate_pct,
  ROUND(AVG(profit_margin_pct), 2)      AS avg_profit_margin_pct,
  ROUND(AVG(debt_to_equity_ratio), 2)   AS avg_debt_to_equity,
  SUM(CASE WHEN net_income < 0 OR debt_to_equity_ratio > 2.5 THEN 1 ELSE 0 END)  AS high_risk_stocks,
  SUM(CASE WHEN net_income < 0 THEN 1 ELSE 0 END)                                AS unprofitable_stocks,
  SUM(CASE WHEN growth_pct > 20 THEN 1 ELSE 0 END)                               AS high_growth_stocks
FROM per_holding;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Risk distribution — same joins, grouped

-- COMMAND ----------

WITH per_holding AS (
  WITH portfolio AS (
    SELECT symbol, UPPER(symbol) AS ticker_region, number_of_shares AS shares_held
    FROM mp_portfolio_federated.dbo.equity_holdings WHERE instrument_type = 'Equity'
  ),
  fundamentals AS (
    SELECT a.ticker_region, c.FF_NET_INCOME AS net_income,
           ROUND((c.FF_DEBT_ST + c.FF_DEBT_LT) / NULLIF(c.FF_COM_EQ, 0), 2) AS debt_to_equity_ratio,
           ROUND(c.FF_NET_INCOME / NULLIF(c.FF_SALES, 0) * 100, 2) AS profit_margin_pct,
           c.FF_EPS_BASIC AS current_eps
    FROM mp_factset_data.sym_v1.sym_ticker_region a
    JOIN mp_factset_data.ff_v3.ff_sec_map         b ON a.fsym_id = b.fsym_id
    JOIN mp_factset_data.ff_v3.ff_basic_af        c ON b.fsym_company_id = c.fsym_id
    WHERE c.DATE >= '2023-01-01' AND c.FF_EPS_BASIC IS NOT NULL AND ABS(c.FF_EPS_BASIC) < 50 AND c.FF_SALES > 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.DATE DESC) = 1
  ),
  estimates AS (
    SELECT a.ticker_region, c.FE_MEAN AS forward_eps
    FROM mp_factset_data.sym_v1.sym_ticker_region a
    JOIN mp_factset_data.fe_v4.fe_sec_map         b ON a.fsym_id = b.fsym_id
    JOIN mp_factset_data.fe_v4.fe_basic_conh_af   c ON b.fsym_company_id = c.fsym_id
    WHERE c.FE_ITEM = 'EPS' AND c.CONS_END_DATE IS NULL AND c.FE_FP_END >= CURRENT_DATE() AND ABS(c.FE_MEAN) < 50
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.FE_FP_END) = 1
  )
  SELECT
    CASE
      WHEN f.net_income < 0 OR f.debt_to_equity_ratio > 2.5                          THEN 'High Risk'
      WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) < -0.15
           OR f.profit_margin_pct < 3                                                THEN 'Medium Risk'
      ELSE 'Low Risk'
    END AS risk_category,
    p.shares_held * f.current_eps AS earnings
  FROM portfolio p JOIN fundamentals f ON p.ticker_region = f.ticker_region
  LEFT JOIN estimates e ON p.ticker_region = e.ticker_region
)
SELECT risk_category, COUNT(*) AS num_stocks, ROUND(SUM(earnings), 2) AS total_earnings
FROM per_holding
GROUP BY risk_category
ORDER BY CASE risk_category WHEN 'High Risk' THEN 1 WHEN 'Medium Risk' THEN 2 ELSE 3 END;
