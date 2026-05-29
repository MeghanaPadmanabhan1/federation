-- Databricks notebook source
-- Portfolio Federation Pipeline (Lakeflow Declarative Pipeline)
-- Target catalog: mp_catalog
-- Target schema:  analytics_sdp
--
-- Reads federated on-prem holdings + FactSet Marketplace data,
-- joins them, and produces the materialized views that back the
-- AI/BI dashboard. Source views are TEMPORARY (visible in the
-- pipeline DAG, not published to the catalog) so the graph tells
-- the full source -> join -> serve story.

-- ============================================================
-- Source 1: portfolio holdings from on-prem SQL Server (federated)
-- ============================================================
CREATE OR REPLACE TEMPORARY VIEW src_portfolio_onprem AS
SELECT
  symbol,
  UPPER(symbol)      AS ticker_region,
  number_of_shares   AS shares_held,
  instrument_type
FROM mp_portfolio_federated.dbo.equity_holdings
WHERE instrument_type = 'Equity';

-- ============================================================
-- Source 2: FactSet from Databricks Marketplace
-- Combines annual fundamentals with consensus analyst estimates
-- (LEFT JOIN so tickers without estimates still appear).
-- ============================================================
CREATE OR REPLACE TEMPORARY VIEW src_factset_marketplace AS
WITH fundamentals AS (
  SELECT
    a.ticker_region,
    c.DATE                                                                          AS fiscal_date,
    c.FF_SALES                                                                      AS revenue,
    c.FF_NET_INCOME                                                                 AS net_income,
    c.FF_EPS_BASIC                                                                  AS current_eps,
    c.FF_FUNDS_OPER_GROSS                                                           AS operating_cash_flow,
    ROUND(c.FF_NET_INCOME       / NULLIF(c.FF_SALES, 0)   * 100, 2)                 AS profit_margin_pct,
    ROUND(c.FF_FUNDS_OPER_GROSS / NULLIF(c.FF_SALES, 0)   * 100, 2)                 AS cash_flow_margin_pct,
    ROUND((c.FF_DEBT_ST + c.FF_DEBT_LT) / NULLIF(c.FF_COM_EQ, 0), 2)                AS debt_to_equity_ratio,
    ROUND(c.FF_COM_EQ            / NULLIF(c.FF_ASSETS, 0) * 100, 2)                 AS equity_ratio_pct
  FROM mp_factset_data.sym_v1.sym_ticker_region a
  JOIN mp_factset_data.ff_v3.ff_sec_map  b ON a.fsym_id = b.fsym_id
  JOIN mp_factset_data.ff_v3.ff_basic_af c ON b.fsym_company_id = c.fsym_id
  WHERE c.DATE >= '2023-01-01'
    AND c.FF_EPS_BASIC IS NOT NULL
    AND ABS(c.FF_EPS_BASIC) < 50
    AND c.FF_SALES > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.DATE DESC) = 1
),
estimates AS (
  SELECT
    a.ticker_region,
    c.FE_FP_END                                                       AS next_fiscal_period,
    c.FE_MEAN                                                         AS forward_eps,
    c.FE_NUM_EST                                                      AS analyst_count,
    ROUND((c.FE_HIGH - c.FE_LOW) / NULLIF(c.FE_MEAN, 0) * 100, 2)     AS estimate_spread_pct
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
  f.ticker_region,
  f.fiscal_date,
  f.revenue,
  f.net_income,
  f.current_eps,
  f.operating_cash_flow,
  f.profit_margin_pct,
  f.cash_flow_margin_pct,
  f.debt_to_equity_ratio,
  f.equity_ratio_pct,
  e.next_fiscal_period,
  e.forward_eps,
  e.analyst_count,
  e.estimate_spread_pct
FROM fundamentals f
LEFT JOIN estimates e ON f.ticker_region = e.ticker_region;

-- ============================================================
-- Gold: combine holdings with FactSet fundamentals + estimates
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW my_portfolio_dashboard AS
SELECT
  p.symbol,
  p.shares_held,
  m.fiscal_date                                                       AS latest_fiscal_date,
  ROUND(m.revenue   / 1000000, 2)                                     AS revenue_mm,
  ROUND(m.net_income / 1000000, 2)                                    AS net_income_mm,
  ROUND(m.operating_cash_flow / 1000000, 2)                           AS operating_cf_mm,
  m.profit_margin_pct,
  m.cash_flow_margin_pct,
  m.debt_to_equity_ratio,
  m.equity_ratio_pct,
  m.current_eps,
  m.forward_eps,
  m.next_fiscal_period                                                AS next_estimate_period,
  m.analyst_count                                                     AS num_analysts_covering,
  m.estimate_spread_pct                                               AS analyst_disagreement_pct,
  ROUND(p.shares_held * m.current_eps, 2)                             AS my_current_annual_earnings,
  ROUND(p.shares_held * m.forward_eps, 2)                             AS my_projected_annual_earnings,
  ROUND(p.shares_held * (m.forward_eps - m.current_eps), 2)           AS my_expected_earnings_increase,
  ROUND(((m.forward_eps - m.current_eps) / NULLIF(m.current_eps, 0)) * 100, 2) AS projected_eps_growth_pct,
  CASE
    WHEN m.net_income < 0                                                            THEN 'High Risk - Unprofitable'
    WHEN m.debt_to_equity_ratio > 2.5                                                THEN 'High Risk - Excessive Debt'
    WHEN ((m.forward_eps - m.current_eps) / NULLIF(m.current_eps, 0)) < -0.15        THEN 'Medium Risk - Declining Earnings'
    WHEN m.profit_margin_pct < 3                                                     THEN 'Medium Risk - Low Margins'
    WHEN m.profit_margin_pct > 15 AND m.debt_to_equity_ratio < 1.5                   THEN 'Low Risk - Strong Fundamentals'
    ELSE 'Low Risk'
  END AS risk_assessment,
  CASE
    WHEN m.net_income < 0                                                            THEN 'SELL - Company Losing Money'
    WHEN ((m.forward_eps - m.current_eps) / NULLIF(m.current_eps, 0)) < -0.15        THEN 'SELL - Earnings Declining'
    WHEN ((m.forward_eps - m.current_eps) / NULLIF(m.current_eps, 0)) > 0.20         THEN 'STRONG BUY - High Growth Expected'
    WHEN ((m.forward_eps - m.current_eps) / NULLIF(m.current_eps, 0)) > 0.05         THEN 'BUY - Positive Growth'
    WHEN ((m.forward_eps - m.current_eps) / NULLIF(m.current_eps, 0)) > -0.10        THEN 'HOLD - Stable'
    ELSE 'HOLD - Monitor'
  END AS action_recommendation
FROM src_portfolio_onprem     p
JOIN src_factset_marketplace  m ON p.ticker_region = m.ticker_region;

-- ============================================================
-- Downstream: portfolio summary (one-row KPIs)
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW my_portfolio_summary AS
SELECT
  'My Portfolio'                                                            AS portfolio_name,
  COUNT(DISTINCT symbol)                                                    AS total_holdings,
  ROUND(SUM(my_current_annual_earnings), 2)                                 AS total_current_earnings,
  ROUND(SUM(my_projected_annual_earnings), 2)                               AS total_projected_earnings,
  ROUND(AVG(projected_eps_growth_pct), 2)                                   AS avg_growth_rate,
  ROUND(AVG(profit_margin_pct), 2)                                          AS avg_profit_margin,
  ROUND(AVG(debt_to_equity_ratio), 2)                                       AS avg_debt_to_equity,
  SUM(CASE WHEN risk_assessment LIKE '%High%' THEN 1 ELSE 0 END)             AS high_risk_stocks,
  SUM(CASE WHEN action_recommendation LIKE '%SELL%' THEN 1 ELSE 0 END)       AS stocks_to_sell,
  SUM(CASE WHEN action_recommendation LIKE '%BUY%'  THEN 1 ELSE 0 END)       AS buying_opportunities,
  SUM(CASE WHEN projected_eps_growth_pct > 20 THEN 1 ELSE 0 END)             AS high_growth_stocks,
  CURRENT_TIMESTAMP()                                                       AS last_updated
FROM my_portfolio_dashboard;

-- ============================================================
-- Downstream: per-stock rankings
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW my_stock_rankings AS
SELECT
  symbol,
  shares_held,
  my_current_annual_earnings  AS annual_earnings_contribution,
  projected_eps_growth_pct    AS growth_rate,
  profit_margin_pct,
  debt_to_equity_ratio,
  num_analysts_covering,
  ROUND(
    (CASE WHEN projected_eps_growth_pct > 0 THEN LEAST(projected_eps_growth_pct, 30) ELSE 0 END) +
    (CASE WHEN profit_margin_pct        > 0 THEN LEAST(profit_margin_pct, 30)        ELSE 0 END) +
    (CASE WHEN debt_to_equity_ratio     < 2 THEN 20 ELSE 10 END) +
    (CASE WHEN num_analysts_covering   >= 10 THEN 20 ELSE num_analysts_covering * 2 END),
  0) AS performance_score,
  risk_assessment,
  action_recommendation
FROM my_portfolio_dashboard;

-- ============================================================
-- Downstream: action items
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW my_action_items AS
SELECT
  symbol,
  shares_held,
  my_current_annual_earnings,
  my_projected_annual_earnings,
  projected_eps_growth_pct,
  profit_margin_pct,
  debt_to_equity_ratio,
  num_analysts_covering,
  risk_assessment,
  action_recommendation,
  CASE
    WHEN action_recommendation LIKE '%SELL%'        THEN 'URGENT - Consider Selling'
    WHEN risk_assessment LIKE '%High%'              THEN 'REVIEW - High Risk'
    WHEN projected_eps_growth_pct > 25              THEN 'OPPORTUNITY - Strong Growth'
    WHEN action_recommendation LIKE '%STRONG BUY%'  THEN 'OPPORTUNITY - Consider Buying More'
    ELSE 'MONITOR - Stable'
  END AS priority_action,
  CURRENT_TIMESTAMP() AS last_updated
FROM my_portfolio_dashboard
WHERE action_recommendation LIKE '%SELL%'
   OR action_recommendation LIKE '%STRONG BUY%'
   OR risk_assessment       LIKE '%High%'
   OR projected_eps_growth_pct > 20;

-- ============================================================
-- Downstream: AI/BI portfolio overview
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW aibi_portfolio_overview AS
SELECT
  'Portfolio Health'                                                                  AS metric_category,
  COUNT(DISTINCT symbol)                                                              AS total_stocks,
  ROUND(SUM(my_current_annual_earnings), 2)                                           AS total_annual_earnings,
  ROUND(SUM(my_projected_annual_earnings), 2)                                         AS projected_annual_earnings,
  ROUND(
    (SUM(my_projected_annual_earnings) - SUM(my_current_annual_earnings)) /
    NULLIF(SUM(my_current_annual_earnings), 0) * 100, 2
  )                                                                                    AS portfolio_growth_pct,
  ROUND(AVG(profit_margin_pct), 2)                                                    AS avg_profit_margin,
  ROUND(AVG(debt_to_equity_ratio), 2)                                                 AS avg_leverage
FROM my_portfolio_dashboard;

-- ============================================================
-- Downstream: AI/BI risk distribution
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW aibi_risk_distribution AS
SELECT
  CASE
    WHEN risk_assessment LIKE '%High%'   THEN 'High Risk'
    WHEN risk_assessment LIKE '%Medium%' THEN 'Medium Risk'
    ELSE 'Low Risk'
  END                                                AS risk_category,
  COUNT(*)                                           AS num_stocks,
  ROUND(SUM(my_current_annual_earnings), 2)          AS total_earnings,
  ROUND(AVG(projected_eps_growth_pct), 2)            AS avg_growth_rate
FROM my_portfolio_dashboard
GROUP BY
  CASE
    WHEN risk_assessment LIKE '%High%'   THEN 'High Risk'
    WHEN risk_assessment LIKE '%Medium%' THEN 'Medium Risk'
    ELSE 'Low Risk'
  END;

-- ============================================================
-- Downstream: AI/BI action distribution
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW aibi_action_distribution AS
SELECT
  CASE
    WHEN action_recommendation LIKE '%STRONG BUY%' THEN 'Strong Buy'
    WHEN action_recommendation LIKE '%BUY%'        THEN 'Buy'
    WHEN action_recommendation LIKE '%SELL%'       THEN 'Sell'
    ELSE 'Hold'
  END                                                AS recommendation,
  COUNT(*)                                           AS num_stocks,
  ROUND(SUM(my_current_annual_earnings), 2)          AS current_earnings_impact,
  ROUND(AVG(projected_eps_growth_pct), 2)            AS avg_expected_growth
FROM my_portfolio_dashboard
GROUP BY
  CASE
    WHEN action_recommendation LIKE '%STRONG BUY%' THEN 'Strong Buy'
    WHEN action_recommendation LIKE '%BUY%'        THEN 'Buy'
    WHEN action_recommendation LIKE '%SELL%'       THEN 'Sell'
    ELSE 'Hold'
  END;

-- ============================================================
-- Downstream: AI/BI stock performance tier
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW aibi_stock_performance AS
SELECT
  symbol,
  shares_held,
  my_current_annual_earnings,
  my_projected_annual_earnings,
  projected_eps_growth_pct,
  profit_margin_pct,
  CASE
    WHEN projected_eps_growth_pct > 15  THEN 'Top Performer'
    WHEN projected_eps_growth_pct > 5   THEN 'Above Average'
    WHEN projected_eps_growth_pct > -5  THEN 'Stable'
    ELSE 'Underperformer'
  END AS performance_tier
FROM my_portfolio_dashboard;
