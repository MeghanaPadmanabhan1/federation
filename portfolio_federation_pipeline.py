# Databricks notebook source
# MAGIC %md
# MAGIC # Portfolio Analytics Pipeline (Views-only)
# MAGIC ## Lakehouse Federation + FactSet Marketplace → Investment Insights
# MAGIC
# MAGIC This Spark Declarative Pipeline combines **on-premise portfolio holdings** (via Lakehouse
# MAGIC Federation) with **FactSet financial data** (via Databricks Marketplace) to produce an
# MAGIC investment analytics flow — **without moving any sensitive data to the cloud**.
# MAGIC
# MAGIC All steps use `@dlt.view()` (not `@dlt.table()`), so:
# MAGIC - No data is materialized into cloud storage
# MAGIC - Every pipeline run re-queries the federated on-prem source live
# MAGIC - Views are pipeline-internal; downstream reads use `dlt.read(...)`
# MAGIC
# MAGIC The persistent views your dashboard reads from (`mp_catalog.analytics.*`) are created
# MAGIC separately in `factset_federation_demo` via `CREATE OR REPLACE VIEW`.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source: On-Premise Holdings (Federated)

# COMMAND ----------

@dlt.view(
    name="federated_holdings",
    comment="Portfolio holdings queried from on-premise SQL Server via Lakehouse Federation. Data never leaves the source."
)
def federated_holdings():
    return (
        spark.read.table("mp_portfolio_federated.dbo.equity_holdings")
        .filter(F.col("instrument_type") == "Equity")
        .select(
            F.col("symbol"),
            F.upper(F.col("symbol")).alias("ticker_region"),
            F.col("number_of_shares").alias("shares_held")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source: FactSet Fundamentals (Marketplace)

# COMMAND ----------

@dlt.view(
    name="factset_fundamentals",
    comment="Latest annual financial fundamentals from FactSet via Databricks Marketplace, with sanity filters on malformed EPS rows."
)
def factset_fundamentals():
    sym = spark.read.table("mp_factset_data.sym_v1.sym_ticker_region").select(
        F.col("ticker_region"),
        F.col("fsym_id").alias("sym_fsym_id")
    )
    ff_map = spark.read.table("mp_factset_data.ff_v3.ff_sec_map").select(
        F.col("fsym_id").alias("map_fsym_id"),
        F.col("fsym_company_id").alias("ff_company_id")
    )
    ff_basic = spark.read.table("mp_factset_data.ff_v3.ff_basic_af").withColumnRenamed("fsym_id", "basic_fsym_id")

    window = Window.partitionBy("ticker_region").orderBy(F.col("DATE").desc())

    return (
        sym
        .join(ff_map, F.col("sym_fsym_id") == F.col("map_fsym_id"))
        .join(ff_basic, F.col("ff_company_id") == F.col("basic_fsym_id"))
        .filter(F.col("DATE") >= "2023-01-01")
        .filter(F.col("FF_EPS_BASIC").isNotNull())
        .filter(F.abs(F.col("FF_EPS_BASIC")) < 50)   # guard against malformed EPS values
        .filter(F.col("FF_SALES") > 0)               # require real revenue
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .select(
            F.col("ticker_region"),
            F.col("DATE").alias("fiscal_date"),
            F.col("FF_SALES").alias("revenue"),
            F.col("FF_NET_INCOME").alias("net_income"),
            F.col("FF_EPS_BASIC").alias("eps"),
            F.col("FF_COM_EQ").alias("shareholders_equity"),
            F.col("FF_FUNDS_OPER_GROSS").alias("operating_cash_flow"),
            F.col("FF_DEBT_LT").alias("long_term_debt"),
            F.round(F.col("FF_NET_INCOME") / F.when(F.col("FF_SALES") != 0, F.col("FF_SALES")) * 100, 2).alias("profit_margin_pct"),
            F.round(F.col("FF_COM_EQ") / F.when(F.col("FF_ASSETS") != 0, F.col("FF_ASSETS")) * 100, 2).alias("equity_ratio_pct"),
            F.round(F.col("FF_DEBT_LT") / F.when(F.col("FF_COM_EQ") != 0, F.col("FF_COM_EQ")) , 2).alias("debt_to_equity_ratio")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source: FactSet Analyst Estimates (Marketplace)

# COMMAND ----------

@dlt.view(
    name="factset_estimates",
    comment="Forward-looking EPS consensus estimates from FactSet via Databricks Marketplace, with sanity filters on malformed values."
)
def factset_estimates():
    sym = spark.read.table("mp_factset_data.sym_v1.sym_ticker_region").select(
        F.col("ticker_region"),
        F.col("fsym_id").alias("sym_fsym_id")
    )
    fe_map = spark.read.table("mp_factset_data.fe_v4.fe_sec_map").select(
        F.col("fsym_id").alias("map_fsym_id"),
        F.col("fsym_company_id").alias("fe_company_id")
    )
    fe_basic = spark.read.table("mp_factset_data.fe_v4.fe_basic_conh_af").withColumnRenamed("fsym_id", "basic_fsym_id")

    window = Window.partitionBy("ticker_region").orderBy(F.col("FE_FP_END").asc())

    return (
        sym
        .join(fe_map, F.col("sym_fsym_id") == F.col("map_fsym_id"))
        .join(fe_basic, F.col("fe_company_id") == F.col("basic_fsym_id"))
        .filter(F.col("FE_ITEM") == "EPS")
        .filter(F.col("FE_FP_END") >= F.current_date())
        .filter(F.col("CONS_END_DATE").isNull())
        .filter(F.abs(F.col("FE_MEAN")) < 50)         # guard against malformed forward EPS values
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .select(
            F.col("ticker_region"),
            F.col("FE_FP_END").alias("next_estimate_period"),
            F.col("FE_MEAN").alias("forward_eps"),
            F.col("FE_NUM_EST").alias("num_analysts"),
            F.col("FE_STD_DEV").alias("analyst_disagreement")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join: Portfolio Dashboard
# MAGIC *Federated on-prem holdings + FactSet fundamentals + FactSet estimates*

# COMMAND ----------

@dlt.view(
    name="portfolio_dashboard",
    comment="Combines federated on-premise holdings with FactSet fundamentals and estimates. The core join — holdings data never leaves on-premise."
)
def portfolio_dashboard():
    holdings = dlt.read("federated_holdings")
    fundamentals = dlt.read("factset_fundamentals")
    estimates = dlt.read("factset_estimates")

    return (
        holdings
        .join(fundamentals, "ticker_region", "inner")
        .join(estimates, "ticker_region", "left")
        .select(
            holdings.symbol,
            holdings.shares_held,
            fundamentals.fiscal_date,
            F.round(fundamentals.revenue / 1000000, 2).alias("revenue_mm"),
            F.round(fundamentals.net_income / 1000000, 2).alias("net_income_mm"),
            F.round(fundamentals.operating_cash_flow / 1000000, 2).alias("operating_cf_mm"),
            fundamentals.profit_margin_pct,
            fundamentals.equity_ratio_pct,
            fundamentals.debt_to_equity_ratio,
            fundamentals.eps.alias("current_eps"),
            estimates.forward_eps,
            estimates.next_estimate_period,
            estimates.num_analysts.alias("num_analysts_covering"),
            F.round(estimates.analyst_disagreement / F.when(F.abs(estimates.forward_eps) > 0, F.abs(estimates.forward_eps)) * 100, 2).alias("analyst_disagreement_pct"),
            F.round(holdings.shares_held * fundamentals.eps, 2).alias("my_current_annual_earnings"),
            F.round(holdings.shares_held * estimates.forward_eps, 2).alias("my_projected_annual_earnings"),
            F.round(holdings.shares_held * (estimates.forward_eps - fundamentals.eps), 2).alias("my_expected_earnings_increase"),
            F.round(
                F.when(F.abs(fundamentals.eps) > 0,
                    (estimates.forward_eps - fundamentals.eps) / F.abs(fundamentals.eps) * 100
                ), 2
            ).alias("projected_eps_growth_pct")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analytics: Stock Rankings

# COMMAND ----------

@dlt.view(
    name="stock_rankings",
    comment="Stocks ranked by composite performance score with risk assessments and action recommendations."
)
def stock_rankings():
    dashboard = dlt.read("portfolio_dashboard")

    return (
        dashboard
        .withColumn("risk_assessment",
            F.when(F.col("net_income_mm") < 0, "High Risk - Unprofitable")
            .when(F.col("debt_to_equity_ratio") > 2.5, "High Risk - Excessive Debt")
            .when(F.col("projected_eps_growth_pct") < -10, "Medium Risk - Declining Earnings")
            .when(F.col("profit_margin_pct") < 5, "Medium Risk - Low Margins")
            .when((F.col("profit_margin_pct") > 15) & (F.col("debt_to_equity_ratio") < 1), "Low Risk - Strong Fundamentals")
            .otherwise("Low Risk")
        )
        .withColumn("action_recommendation",
            F.when(F.col("net_income_mm") < 0, "SELL - Company Losing Money")
            .when(F.col("projected_eps_growth_pct") < -10, "SELL - Earnings Declining")
            .when(F.col("projected_eps_growth_pct").isNull(), "HOLD - Monitor")
            .when(F.col("projected_eps_growth_pct") > 20, "STRONG BUY - High Growth Expected")
            .when(F.col("projected_eps_growth_pct") > 5, "BUY - Positive Growth")
            .when(F.col("projected_eps_growth_pct").between(-5, 5), "HOLD - Stable")
            .otherwise("HOLD - Monitor")
        )
        .withColumn("performance_score",
            F.round(
                F.coalesce(F.least(F.col("projected_eps_growth_pct") / 2, F.lit(30)), F.lit(0)) +
                F.coalesce(F.least(F.col("profit_margin_pct") / 2, F.lit(25)), F.lit(0)) +
                F.when(F.col("debt_to_equity_ratio") < 0.5, 25)
                 .when(F.col("debt_to_equity_ratio") < 1, 20)
                 .when(F.col("debt_to_equity_ratio") < 2, 10)
                 .otherwise(0) +
                F.when(F.col("num_analysts_covering") > 20, 20)
                 .when(F.col("num_analysts_covering") > 10, 15)
                 .when(F.col("num_analysts_covering") > 5, 10)
                 .otherwise(5)
            , 2)
        )
        .select(
            "symbol", "shares_held", "my_current_annual_earnings",
            "projected_eps_growth_pct", "profit_margin_pct", "debt_to_equity_ratio",
            "num_analysts_covering", "performance_score",
            "risk_assessment", "action_recommendation"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analytics: Portfolio Summary

# COMMAND ----------

@dlt.view(
    name="portfolio_summary",
    comment="Aggregate portfolio-level metrics — total holdings, earnings, risk counts."
)
def portfolio_summary():
    rankings = dlt.read("stock_rankings")

    return (
        rankings
        .agg(
            F.lit("My Portfolio").alias("portfolio_name"),
            F.count("*").alias("total_holdings"),
            F.round(F.sum("my_current_annual_earnings"), 2).alias("total_current_earnings"),
            F.round(F.avg("projected_eps_growth_pct"), 2).alias("avg_growth_rate"),
            F.round(F.avg("profit_margin_pct"), 2).alias("avg_profit_margin"),
            F.round(F.avg("debt_to_equity_ratio"), 2).alias("avg_debt_to_equity"),
            F.sum(F.when(F.col("risk_assessment").contains("High Risk"), 1).otherwise(0)).alias("high_risk_stocks"),
            F.sum(F.when(F.col("action_recommendation").contains("SELL"), 1).otherwise(0)).alias("stocks_to_sell"),
            F.sum(F.when(F.col("action_recommendation").contains("BUY"), 1).otherwise(0)).alias("buying_opportunities"),
            F.current_timestamp().alias("last_updated")
        )
    )
