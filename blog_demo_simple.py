# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse Federation: Query Data Without Moving It
# MAGIC
# MAGIC ## The Challenge
# MAGIC In regulated industries, organizations often have:
# MAGIC - **Secure data** in on-premise or cloud databases (e.g., customer PII, holdings)
# MAGIC - **Analytical data** in their lakehouse (e.g., market data, reference data)
# MAGIC - **Compliance requirements** that prevent moving all data to the cloud
# MAGIC
# MAGIC ## The Solution: Lakehouse Federation
# MAGIC Query and analyze data across sources **without moving it** into Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Connect to Azure SQL (One-Time Setup)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create reusable connection to Azure SQL Database
# MAGIC CREATE CONNECTION IF NOT EXISTS azure_sql_prod
# MAGIC TYPE sqlserver
# MAGIC OPTIONS (
# MAGIC   host 'your-server.database.windows.net',
# MAGIC   port '1433',
# MAGIC   user 'your-username',
# MAGIC   password secret('azure-sql-secrets', 'password')
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create foreign catalog that federates Azure SQL
# MAGIC CREATE CATALOG IF NOT EXISTS customer_data_federated
# MAGIC USING CONNECTION azure_sql_prod
# MAGIC OPTIONS (database 'CustomerDB');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Query Federated Data (Stays in Azure SQL)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query customer holdings directly from Azure SQL
# MAGIC -- Data NEVER leaves Azure SQL Database
# MAGIC
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   ticker_symbol,
# MAGIC   shares_held,
# MAGIC   cost_basis,
# MAGIC   purchase_date
# MAGIC FROM customer_data_federated.dbo.customer_holdings
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Query Managed Data (In Databricks)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query FactSet financial data from Databricks Marketplace
# MAGIC SELECT
# MAGIC   ticker,
# MAGIC   fiscal_year,
# MAGIC   revenue,
# MAGIC   net_income,
# MAGIC   total_assets,
# MAGIC   eps_basic
# MAGIC FROM factset.ff_basic.annual_financials
# MAGIC WHERE fiscal_year = 2023
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Combine Both Sources in a Single Query
# MAGIC
# MAGIC This is where the magic happens - unified analytics across federated and managed data!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Portfolio Analysis: Join customer holdings (Azure SQL) with FactSet fundamentals (Databricks)
# MAGIC
# MAGIC WITH customer_holdings AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     ticker_symbol,
# MAGIC     shares_held,
# MAGIC     cost_basis,
# MAGIC     shares_held * cost_basis AS investment_value
# MAGIC   FROM customer_data_federated.dbo.customer_holdings
# MAGIC ),
# MAGIC company_financials AS (
# MAGIC   SELECT
# MAGIC     ticker,
# MAGIC     revenue,
# MAGIC     net_income,
# MAGIC     total_equity / total_assets AS equity_ratio
# MAGIC   FROM factset.ff_basic.annual_financials
# MAGIC   WHERE fiscal_year = 2023
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   h.customer_id,
# MAGIC   h.ticker_symbol,
# MAGIC   h.shares_held,
# MAGIC   h.investment_value,
# MAGIC   f.revenue,
# MAGIC   f.net_income,
# MAGIC   f.equity_ratio,
# MAGIC   CASE
# MAGIC     WHEN f.net_income < 0 THEN 'High Risk'
# MAGIC     WHEN f.equity_ratio < 0.3 THEN 'Medium Risk'
# MAGIC     ELSE 'Low Risk'
# MAGIC   END AS risk_level
# MAGIC FROM customer_holdings h
# MAGIC LEFT JOIN company_financials f
# MAGIC   ON h.ticker_symbol = f.ticker
# MAGIC ORDER BY h.investment_value DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Advanced Analytics with PySpark

# COMMAND ----------

from pyspark.sql import functions as F

# Query federated customer data
holdings = spark.sql("""
    SELECT
        customer_id,
        ticker_symbol,
        shares_held,
        cost_basis
    FROM customer_data_federated.dbo.customer_holdings
""")

# Query managed FactSet data
financials = spark.sql("""
    SELECT
        ticker,
        revenue,
        net_income,
        total_assets,
        eps_basic
    FROM factset.ff_basic.annual_financials
    WHERE fiscal_year = 2023
""")

# Join and analyze
portfolio_analysis = holdings.join(
    financials,
    holdings.ticker_symbol == financials.ticker,
    "left"
).groupBy("customer_id").agg(
    F.count("ticker_symbol").alias("total_positions"),
    F.sum(F.col("shares_held") * F.col("cost_basis")).alias("portfolio_value"),
    F.avg("revenue").alias("avg_company_revenue"),
    F.avg("net_income").alias("avg_company_profit"),
    F.sum(F.when(F.col("net_income") < 0, 1).otherwise(0)).alias("loss_making_positions")
).withColumn(
    "risk_category",
    F.when(F.col("loss_making_positions") > F.col("total_positions") * 0.3, "High Risk")
     .when(F.col("loss_making_positions") > F.col("total_positions") * 0.1, "Medium Risk")
     .otherwise("Low Risk")
)

display(portfolio_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Dashboard-Ready Views

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a view for dashboard consumption
# MAGIC CREATE OR REPLACE VIEW main.default.customer_portfolio_dashboard AS
# MAGIC
# MAGIC WITH enriched_holdings AS (
# MAGIC   SELECT
# MAGIC     h.customer_id,
# MAGIC     h.ticker_symbol,
# MAGIC     h.shares_held,
# MAGIC     h.cost_basis,
# MAGIC     h.shares_held * h.cost_basis AS position_value,
# MAGIC     f.revenue AS company_revenue,
# MAGIC     f.net_income AS company_profit,
# MAGIC     f.eps_basic AS earnings_per_share
# MAGIC   FROM customer_data_federated.dbo.customer_holdings h
# MAGIC   LEFT JOIN factset.ff_basic.annual_financials f
# MAGIC     ON h.ticker_symbol = f.ticker
# MAGIC     AND f.fiscal_year = 2023
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   COUNT(*) AS num_positions,
# MAGIC   SUM(position_value) AS total_portfolio_value,
# MAGIC   AVG(company_revenue) AS avg_revenue,
# MAGIC   AVG(company_profit) AS avg_profit,
# MAGIC   SUM(CASE WHEN company_profit < 0 THEN 1 ELSE 0 END) AS high_risk_count,
# MAGIC   SUM(CASE WHEN company_profit < 0 THEN position_value ELSE 0 END) AS high_risk_exposure
# MAGIC FROM enriched_holdings
# MAGIC GROUP BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the dashboard view
# MAGIC SELECT * FROM main.default.customer_portfolio_dashboard;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Key Results Demonstrated
# MAGIC
# MAGIC ✅ **Customer holdings data stayed in Azure SQL** throughout the entire analysis
# MAGIC ✅ **FactSet data queried** from Databricks managed catalog
# MAGIC ✅ **Unified analytics** across both sources with standard SQL/Spark
# MAGIC ✅ **Real-time insights** without ETL pipelines or data movement
# MAGIC ✅ **Security maintained** - sensitive data never left the regulated environment
# MAGIC
# MAGIC ## 🏢 Why This Matters for Regulated Industries
# MAGIC
# MAGIC **Traditional Approach (Data Movement)**
# MAGIC - Copy all data into lakehouse ❌
# MAGIC - Additional compliance burden ❌
# MAGIC - Data duplication costs ❌
# MAGIC - ETL pipeline complexity ❌
# MAGIC - Data freshness lag ❌
# MAGIC
# MAGIC **Lakehouse Federation (No Data Movement)**
# MAGIC - Query data in place ✅
# MAGIC - Keep existing security controls ✅
# MAGIC - No storage duplication ✅
# MAGIC - Simple, direct queries ✅
# MAGIC - Always current data ✅
# MAGIC
# MAGIC ## 🎯 Use Cases
# MAGIC
# MAGIC 1. **Financial Services**: Customer accounts + market data analytics
# MAGIC 2. **Healthcare**: Patient records + research data analysis
# MAGIC 3. **Retail**: Transactional systems + customer behavior insights
# MAGIC 4. **Insurance**: Policy data + risk modeling
# MAGIC 5. **Manufacturing**: ERP systems + IoT sensor analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Query Performance Insights

# COMMAND ----------

# MAGIC %sql
# MAGIC -- See the query plan - notice pushdown to Azure SQL
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   ticker_symbol,
# MAGIC   shares_held
# MAGIC FROM customer_data_federated.dbo.customer_holdings
# MAGIC WHERE purchase_date >= '2023-01-01';

# COMMAND ----------

# MAGIC %md
# MAGIC Notice in the query plan:
# MAGIC - **Filter pushdown**: WHERE clause executes in Azure SQL, not Databricks
# MAGIC - **Projection pushdown**: Only selected columns transferred
# MAGIC - **Optimized data transfer**: Minimal network overhead
# MAGIC
# MAGIC This means Databricks intelligently pushes computation to the data source!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Next Steps
# MAGIC
# MAGIC 1. **Connect** your Azure SQL or other JDBC database
# MAGIC 2. **Create** foreign catalogs for federated access
# MAGIC 3. **Query** across federated and managed sources
# MAGIC 4. **Build** dashboards with unified data
# MAGIC 5. **Scale** to additional data sources (PostgreSQL, MySQL, Snowflake, etc.)
# MAGIC
# MAGIC ## 📚 Learn More
# MAGIC
# MAGIC - [Lakehouse Federation Docs](https://docs.databricks.com/en/query-federation/)
# MAGIC - [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/)
# MAGIC - [Databricks Marketplace](https://marketplace.databricks.com/)
