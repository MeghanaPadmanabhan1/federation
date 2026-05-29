-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ad hoc: What's my projected upside next fiscal period?
-- MAGIC
-- MAGIC Federated holdings + FactSet annual fundamentals + FactSet consensus estimates, joined live. For each holding: shares * (forward EPS - current EPS) = expected earnings increase, ranked. Asset manager use: identifies which names will drive the next leg of portfolio growth.

-- COMMAND ----------

-- Federated holdings + FactSet fundamentals + FactSet estimates,
-- all in one query. The forward outlook each position contributes to the book.
WITH latest_fundamentals AS (
  SELECT
    a.ticker_region,
    c.FF_EPS_BASIC AS current_eps
  FROM mp_factset_data.sym_v1.sym_ticker_region a
  JOIN mp_factset_data.ff_v3.ff_sec_map  b ON a.fsym_id = b.fsym_id
  JOIN mp_factset_data.ff_v3.ff_basic_af c ON b.fsym_company_id = c.fsym_id
  WHERE c.DATE >= '2023-01-01'
    AND c.FF_EPS_BASIC IS NOT NULL
    AND ABS(c.FF_EPS_BASIC) < 50
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ticker_region ORDER BY c.DATE DESC) = 1
),
latest_estimates AS (
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
  p.number_of_shares                                              AS shares_held,
  ROUND(p.number_of_shares * f.current_eps, 2)                    AS current_annual_earnings,
  ROUND(p.number_of_shares * e.forward_eps, 2)                    AS projected_annual_earnings,
  ROUND(p.number_of_shares * (e.forward_eps - f.current_eps), 2)  AS expected_earnings_increase,
  ROUND(((e.forward_eps - f.current_eps) / NULLIF(f.current_eps, 0)) * 100, 2) AS projected_eps_growth_pct,
  e.analyst_count
FROM mp_portfolio_federated.dbo.equity_holdings p
JOIN latest_fundamentals f ON UPPER(p.symbol) = f.ticker_region
JOIN latest_estimates    e ON UPPER(p.symbol) = e.ticker_region
WHERE p.instrument_type = 'Equity'
ORDER BY expected_earnings_increase DESC
LIMIT 25;
