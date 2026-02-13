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
# MAGIC FactSet data uses a **FactSet Sim ID** (unique identifier) instead of standard ticker symbols.
# MAGIC
# MAGIC **Key FactSet Schemas:**
# MAGIC - `FE` - FactSet Estimates (analyst estimates, consensus data)
# MAGIC - `FF` - FactSet Fundamentals (financial statements, ratios)
# MAGIC - `SYM` - Symbology tables (maps ticker symbols to FactSet Sim IDs)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explore FactSet Fundamentals schema
# MAGIC SHOW TABLES IN factset_catalog.ff_basic;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: FactSet annual financials (uses factset_entity_id, not ticker)
# MAGIC SELECT *
# MAGIC FROM factset_catalog.ff_basic.ff_basic_af
# MAGIC LIMIT 5;
# MAGIC
# MAGIC -- Notice: No ticker symbol column! Uses factset_entity_id instead

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: The Symbology Mapping Table
# MAGIC
# MAGIC To use FactSet data with ticker symbols, we need the **symbology (SYM) table** to map tickers to FactSet IDs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Symbology table maps ticker symbols to FactSet entity IDs
# MAGIC SELECT
# MAGIC   ticker,
# MAGIC   factset_entity_id,
# MAGIC   proper_name AS company_name,
# MAGIC   entity_type
# MAGIC FROM factset_catalog.sym_basic.sym_coverage
# MAGIC WHERE ticker IN ('MSFT', 'AAPL', 'GOOGL', 'AMZN', 'TSLA')
# MAGIC LIMIT 10;

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
# MAGIC ## Step 5: The Three-Way Join Pattern
# MAGIC
# MAGIC To combine portfolio with FactSet data, we need to join through the symbology table:
# MAGIC
# MAGIC ```
# MAGIC Portfolio (ticker)  →  Symbology (ticker → factset_id)  →  FactSet Data (factset_id)
# MAGIC   [On-Premise]              [Databricks]                        [Databricks]
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
# MAGIC     shares_held,
# MAGIC     cost_basis,
# MAGIC     shares_held * cost_basis AS position_value
# MAGIC   FROM portfolio_federated.dbo.customer_holdings
# MAGIC ),
# MAGIC ticker_mapping AS (
# MAGIC   SELECT
# MAGIC     ticker,
# MAGIC     factset_entity_id,
# MAGIC     proper_name AS company_name
# MAGIC   FROM factset_catalog.sym_basic.sym_coverage
# MAGIC ),
# MAGIC financials AS (
# MAGIC   SELECT
# MAGIC     factset_entity_id,
# MAGIC     fiscal_year,
# MAGIC     sales AS revenue,
# MAGIC     net_income,
# MAGIC     total_assets,
# MAGIC     total_equity,
# MAGIC     eps_basic
# MAGIC   FROM factset_catalog.ff_basic.ff_basic_af
# MAGIC   WHERE fiscal_year = 2023
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   p.customer_id,
# MAGIC   p.ticker_symbol,
# MAGIC   tm.company_name,
# MAGIC   p.shares_held,
# MAGIC   p.position_value,
# MAGIC   f.revenue,
# MAGIC   f.net_income,
# MAGIC   f.eps_basic,
# MAGIC   ROUND(p.position_value / f.net_income, 2) AS position_to_profit_ratio
# MAGIC FROM portfolio p
# MAGIC JOIN ticker_mapping tm
# MAGIC   ON p.ticker_symbol = tm.ticker
# MAGIC JOIN financials f
# MAGIC   ON tm.factset_entity_id = f.factset_entity_id
# MAGIC ORDER BY p.position_value DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Analyst Estimates for Investment Decisions
# MAGIC
# MAGIC Now let's combine portfolio with **FactSet Estimates** to see analyst projections.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Portfolio enriched with analyst estimates
# MAGIC -- Helps answer: "Should I buy/sell based on analyst consensus?"
# MAGIC
# MAGIC WITH portfolio AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     ticker_symbol,
# MAGIC     shares_held,
# MAGIC     cost_basis
# MAGIC   FROM portfolio_federated.dbo.customer_holdings
# MAGIC ),
# MAGIC ticker_mapping AS (
# MAGIC   SELECT
# MAGIC     ticker,
# MAGIC     factset_entity_id,
# MAGIC     proper_name AS company_name
# MAGIC   FROM factset_catalog.sym_basic.sym_coverage
# MAGIC ),
# MAGIC estimates AS (
# MAGIC   SELECT
# MAGIC     factset_entity_id,
# MAGIC     fiscal_year,
# MAGIC     mean_estimate AS consensus_eps_estimate,
# MAGIC     high_estimate AS high_eps_estimate,
# MAGIC     low_estimate AS low_eps_estimate,
# MAGIC     num_estimates AS analyst_count
# MAGIC   FROM factset_catalog.fe_basic.fe_basic_eps
# MAGIC   WHERE fiscal_year = 2024  -- Forward-looking estimates
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   p.customer_id,
# MAGIC   p.ticker_symbol,
# MAGIC   tm.company_name,
# MAGIC   p.shares_held,
# MAGIC   e.consensus_eps_estimate,
# MAGIC   e.high_eps_estimate,
# MAGIC   e.low_eps_estimate,
# MAGIC   e.analyst_count,
# MAGIC   ROUND(p.shares_held * e.consensus_eps_estimate, 2) AS estimated_earnings_value
# MAGIC FROM portfolio p
# MAGIC JOIN ticker_mapping tm
# MAGIC   ON p.ticker_symbol = tm.ticker
# MAGIC JOIN estimates e
# MAGIC   ON tm.factset_entity_id = e.factset_entity_id
# MAGIC ORDER BY estimated_earnings_value DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Investment Decision Support
# MAGIC
# MAGIC Combine historical performance + forward estimates for comprehensive portfolio analysis.

# COMMAND ----------

from pyspark.sql import functions as F

# Portfolio data (federated from on-prem)
portfolio_df = spark.sql("""
    SELECT
        customer_id,
        ticker_symbol,
        shares_held,
        cost_basis,
        shares_held * cost_basis AS position_value
    FROM portfolio_federated.dbo.customer_holdings
""")

# Symbology mapping
symbology_df = spark.sql("""
    SELECT
        ticker,
        factset_entity_id,
        proper_name AS company_name
    FROM factset_catalog.sym_basic.sym_coverage
""")

# FactSet fundamentals (historical)
fundamentals_df = spark.sql("""
    SELECT
        factset_entity_id,
        sales AS revenue,
        net_income,
        eps_basic AS eps_actual
    FROM factset_catalog.ff_basic.ff_basic_af
    WHERE fiscal_year = 2023
""")

# FactSet estimates (forward-looking)
estimates_df = spark.sql("""
    SELECT
        factset_entity_id,
        mean_estimate AS eps_estimate_2024
    FROM factset_catalog.fe_basic.fe_basic_eps
    WHERE fiscal_year = 2024
""")

# Join everything together
investment_analysis = portfolio_df \
    .join(symbology_df, portfolio_df.ticker_symbol == symbology_df.ticker) \
    .join(fundamentals_df, symbology_df.factset_entity_id == fundamentals_df.factset_entity_id) \
    .join(estimates_df, symbology_df.factset_entity_id == estimates_df.factset_entity_id) \
    .select(
        "customer_id",
        "ticker_symbol",
        "company_name",
        "shares_held",
        "position_value",
        "revenue",
        "net_income",
        "eps_actual",
        "eps_estimate_2024"
    ) \
    .withColumn(
        "eps_growth_projection",
        F.round(((F.col("eps_estimate_2024") - F.col("eps_actual")) / F.col("eps_actual")) * 100, 2)
    ) \
    .withColumn(
        "investment_signal",
        F.when(F.col("eps_growth_projection") > 10, "Strong Buy")
         .when(F.col("eps_growth_projection") > 0, "Buy")
         .when(F.col("eps_growth_projection") > -10, "Hold")
         .otherwise("Sell")
    )

display(investment_analysis.orderBy(F.col("position_value").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Portfolio Risk Assessment

# COMMAND ----------

# Customer-level portfolio summary with risk metrics
portfolio_summary = investment_analysis.groupBy("customer_id").agg(
    F.count("ticker_symbol").alias("num_positions"),
    F.sum("position_value").alias("total_portfolio_value"),
    F.avg("eps_growth_projection").alias("avg_growth_projection"),
    F.sum(F.when(F.col("investment_signal") == "Sell", F.col("position_value")).otherwise(0)).alias("at_risk_value"),
    F.sum(F.when(F.col("net_income") < 0, 1).otherwise(0)).alias("unprofitable_holdings")
).withColumn(
    "risk_score",
    F.round((F.col("at_risk_value") / F.col("total_portfolio_value")) * 100, 2)
).withColumn(
    "portfolio_health",
    F.when(F.col("risk_score") > 30, "High Risk")
     .when(F.col("risk_score") > 15, "Medium Risk")
     .otherwise("Healthy")
)

display(portfolio_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Real-World Example - Microsoft Portfolio Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Detailed analysis for a specific stock (e.g., Microsoft)
# MAGIC
# MAGIC WITH portfolio AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     ticker_symbol,
# MAGIC     shares_held,
# MAGIC     cost_basis,
# MAGIC     purchase_date
# MAGIC   FROM portfolio_federated.dbo.customer_holdings
# MAGIC   WHERE ticker_symbol = 'MSFT'
# MAGIC ),
# MAGIC symbology AS (
# MAGIC   SELECT factset_entity_id, ticker, proper_name
# MAGIC   FROM factset_catalog.sym_basic.sym_coverage
# MAGIC   WHERE ticker = 'MSFT'
# MAGIC ),
# MAGIC financials AS (
# MAGIC   SELECT
# MAGIC     factset_entity_id,
# MAGIC     fiscal_year,
# MAGIC     sales,
# MAGIC     net_income,
# MAGIC     eps_basic
# MAGIC   FROM factset_catalog.ff_basic.ff_basic_af
# MAGIC   WHERE fiscal_year IN (2021, 2022, 2023)
# MAGIC ),
# MAGIC estimates AS (
# MAGIC   SELECT
# MAGIC     factset_entity_id,
# MAGIC     fiscal_year,
# MAGIC     mean_estimate,
# MAGIC     num_estimates
# MAGIC   FROM factset_catalog.fe_basic.fe_basic_eps
# MAGIC   WHERE fiscal_year = 2024
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   p.customer_id,
# MAGIC   s.proper_name AS company,
# MAGIC   p.shares_held,
# MAGIC   p.cost_basis,
# MAGIC   p.shares_held * p.cost_basis AS current_position_value,
# MAGIC   f.fiscal_year,
# MAGIC   f.sales AS revenue,
# MAGIC   f.net_income,
# MAGIC   f.eps_basic AS historical_eps,
# MAGIC   e.mean_estimate AS estimated_eps_2024,
# MAGIC   e.num_estimates AS analyst_count,
# MAGIC   ROUND(p.shares_held * e.mean_estimate, 2) AS projected_earnings_value
# MAGIC FROM portfolio p
# MAGIC JOIN symbology s ON p.ticker_symbol = s.ticker
# MAGIC LEFT JOIN financials f ON s.factset_entity_id = f.factset_entity_id
# MAGIC LEFT JOIN estimates e ON s.factset_entity_id = e.factset_entity_id
# MAGIC ORDER BY f.fiscal_year;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Create Dashboard View

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a materialized view for dashboard consumption
# MAGIC CREATE OR REPLACE VIEW main.analytics.portfolio_factset_dashboard AS
# MAGIC
# MAGIC WITH portfolio AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     ticker_symbol,
# MAGIC     shares_held,
# MAGIC     cost_basis,
# MAGIC     shares_held * cost_basis AS position_value,
# MAGIC     account_type
# MAGIC   FROM portfolio_federated.dbo.customer_holdings
# MAGIC ),
# MAGIC symbology AS (
# MAGIC   SELECT
# MAGIC     ticker,
# MAGIC     factset_entity_id,
# MAGIC     proper_name,
# MAGIC     entity_type
# MAGIC   FROM factset_catalog.sym_basic.sym_coverage
# MAGIC ),
# MAGIC fundamentals AS (
# MAGIC   SELECT
# MAGIC     factset_entity_id,
# MAGIC     sales AS revenue,
# MAGIC     net_income,
# MAGIC     total_assets,
# MAGIC     eps_basic AS eps_2023
# MAGIC   FROM factset_catalog.ff_basic.ff_basic_af
# MAGIC   WHERE fiscal_year = 2023
# MAGIC ),
# MAGIC estimates AS (
# MAGIC   SELECT
# MAGIC     factset_entity_id,
# MAGIC     mean_estimate AS eps_est_2024,
# MAGIC     num_estimates AS analyst_count
# MAGIC   FROM factset_catalog.fe_basic.fe_basic_eps
# MAGIC   WHERE fiscal_year = 2024
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   p.customer_id,
# MAGIC   p.ticker_symbol,
# MAGIC   s.proper_name AS company_name,
# MAGIC   p.shares_held,
# MAGIC   p.position_value,
# MAGIC   p.account_type,
# MAGIC   f.revenue,
# MAGIC   f.net_income,
# MAGIC   f.eps_2023,
# MAGIC   e.eps_est_2024,
# MAGIC   e.analyst_count,
# MAGIC   ROUND(((e.eps_est_2024 - f.eps_2023) / f.eps_2023) * 100, 2) AS eps_growth_pct,
# MAGIC   CASE
# MAGIC     WHEN f.net_income < 0 THEN 'High Risk'
# MAGIC     WHEN ((e.eps_est_2024 - f.eps_2023) / f.eps_2023) < -0.1 THEN 'Medium Risk'
# MAGIC     ELSE 'Low Risk'
# MAGIC   END AS risk_level
# MAGIC FROM portfolio p
# MAGIC JOIN symbology s ON p.ticker_symbol = s.ticker
# MAGIC LEFT JOIN fundamentals f ON s.factset_entity_id = f.factset_entity_id
# MAGIC LEFT JOIN estimates e ON s.factset_entity_id = e.factset_entity_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.analytics.portfolio_factset_dashboard
# MAGIC ORDER BY position_value DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Key Accomplishments
# MAGIC
# MAGIC ### What We Demonstrated:
# MAGIC
# MAGIC 1. ✅ **Portfolio data stayed on-premise** - Never moved to Databricks
# MAGIC 2. ✅ **FactSet Sim ID mapping** - Properly joined through symbology table
# MAGIC 3. ✅ **Unified analytics** - Combined on-prem + marketplace data seamlessly
# MAGIC 4. ✅ **Investment insights** - Historical financials + forward estimates
# MAGIC 5. ✅ **Risk assessment** - Identified high-risk positions automatically
# MAGIC 6. ✅ **Dashboard-ready** - Created views for BI tool consumption
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
# MAGIC ## 📊 Example Use Cases
# MAGIC
# MAGIC ### 1. Portfolio Management
# MAGIC - Combine client holdings with FactSet fundamentals and estimates
# MAGIC - Generate buy/sell recommendations based on analyst consensus
# MAGIC - Track portfolio performance against benchmarks
# MAGIC
# MAGIC ### 2. Risk Management
# MAGIC - Identify exposure to declining companies
# MAGIC - Monitor portfolio concentration
# MAGIC - Alert on positions with negative analyst sentiment
# MAGIC
# MAGIC ### 3. Client Reporting
# MAGIC - Generate personalized portfolio reports with FactSet data
# MAGIC - Show how holdings compare to industry averages
# MAGIC - Provide forward-looking estimates and projections
# MAGIC
# MAGIC ### 4. Compliance & Auditing
# MAGIC - Keep transaction data in regulated systems
# MAGIC - Query for compliance checks without data export
# MAGIC - Maintain complete audit trail of data access

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
# MAGIC ## 🚀 Next Steps
# MAGIC
# MAGIC 1. **Set up your connection** to on-premise/Azure SQL database
# MAGIC 2. **Access FactSet data** from Databricks Marketplace
# MAGIC 3. **Map your ticker symbols** using the symbology table
# MAGIC 4. **Build your queries** following the three-way join pattern
# MAGIC 5. **Create dashboards** using the unified view
# MAGIC
# MAGIC ## 📚 Resources
# MAGIC
# MAGIC - [FactSet on Databricks Marketplace](https://marketplace.databricks.com/)
# MAGIC - [Lakehouse Federation Documentation](https://docs.databricks.com/query-federation/)
# MAGIC - [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/)
