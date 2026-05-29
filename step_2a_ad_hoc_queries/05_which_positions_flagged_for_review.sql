-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ad hoc: Which positions are flagged for review today?
-- MAGIC
-- MAGIC Morning triage. Federated holdings × FactSet financials + estimates; surfaces only positions hitting at least one risk criterion (unprofitable, over-leveraged, earnings declining, or low margins). Asset manager use: the punch list of names that need attention before market open.

-- COMMAND ----------

-- Morning triage list. Federated holdings + FactSet fundamentals + estimates.
-- Filters to positions hitting at least one risk criterion.
WITH latest_fundamentals AS (
  SELECT
    a.ticker_region,
    c.FF_NET_INCOME                                                 AS net_income,
    c.FF_EPS_BASIC                                                  AS current_eps,
    ROUND((c.FF_DEBT_ST + c.FF_DEBT_LT) / NULLIF(c.FF_COM_EQ, 0), 2) AS debt_to_equity,
    ROUND(c.FF_NET_INCOME / NULLIF(c.FF_SALES, 0) * 100, 2)         AS profit_margin_pct
  FROM mp_factset_data.sym_v1.sym_ticker_region a
  JOIN mp_factset_data.ff_v3.ff_sec_map  b ON a.fsym_id = b.fsym_id
  JOIN mp_factset_data.ff_v3.ff_basic_af c ON b.fsym_company_id = c.fsym_id
  WHERE c.DATE >= '2023-01-01'
    AND c.FF_EPS_BASIC IS NOT NULL
    AND ABS(c.FF_EPS_BASIC) < 50
    AND c.FF_SALES > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.DATE DESC) = 1
),
latest_estimates AS (
  SELECT
    a.ticker_region,
    c.FE_MEAN AS forward_eps
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
  p.number_of_shares                                                AS shares_held,
  f.current_eps,
  e.forward_eps,
  f.debt_to_equity,
  f.profit_margin_pct,
  ROUND(((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) * 100, 2) AS projected_eps_growth_pct,
  CASE
    WHEN f.net_income < 0                                                                 THEN 'SELL - Unprofitable'
    WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) < -0.15             THEN 'SELL - Earnings Declining'
    WHEN f.debt_to_equity > 2.5                                                            THEN 'REVIEW - High Leverage'
    WHEN f.profit_margin_pct < 3                                                           THEN 'REVIEW - Low Margins'
  END AS flag
FROM mp_portfolio_federated.dbo.equity_holdings p
JOIN latest_fundamentals f      ON UPPER(p.symbol) = f.ticker_region
LEFT JOIN latest_estimates e    ON UPPER(p.symbol) = e.ticker_region
WHERE p.instrument_type = 'Equity'
  AND (f.net_income < 0
       OR f.debt_to_equity > 2.5
       OR ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) < -0.15
       OR f.profit_margin_pct < 3)
ORDER BY
  CASE
    WHEN f.net_income < 0 THEN 1
    WHEN ((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) < -0.15 THEN 2
    WHEN f.debt_to_equity > 2.5 THEN 3
    ELSE 4
  END,
  p.number_of_shares DESC;
