# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse Federation Demo: Processing Data Without Moving It
# MAGIC
# MAGIC This notebook demonstrates how to:
# MAGIC 1. Federate an Azure SQL Database instance
# MAGIC 2. Query data from the federated catalog (Azure SQL)
# MAGIC 3. Query data from a managed catalog in Databricks (e.g., FactSet data)
# MAGIC 4. Combine and process data from both sources without moving data into Databricks
# MAGIC
# MAGIC **Key Message**: You don't need to physically move secure data into Databricks to query and analyze it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Set Up Azure SQL Federation Connection
# MAGIC
# MAGIC First, we'll create a connection to Azure SQL Database and set up a foreign catalog.

# COMMAND ----------

# Connection parameters for Azure SQL
azure_sql_host = "<your-server>.database.windows.net"
azure_sql_port = "1433"
azure_sql_database = "<your-database-name>"
azure_sql_user = "<your-username>"

# Note: Store password securely in Databricks secrets
# dbutils.secrets.get(scope="<scope-name>", key="azure-sql-password")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a connection to Azure SQL Database
# MAGIC -- This connection can be reused across multiple catalogs and schemas
# MAGIC
# MAGIC CREATE CONNECTION IF NOT EXISTS azure_sql_connection
# MAGIC TYPE sqlserver
# MAGIC OPTIONS (
# MAGIC   host '<your-server>.database.windows.net',
# MAGIC   port '1433',
# MAGIC   user '<your-username>',
# MAGIC   password secret('<scope-name>', 'azure-sql-password')
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the connection was created
# MAGIC SHOW CONNECTIONS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Foreign Catalog for Azure SQL
# MAGIC
# MAGIC A foreign catalog allows you to query external data sources as if they were native Databricks tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a foreign catalog that federates the Azure SQL database
# MAGIC CREATE CATALOG IF NOT EXISTS azure_sql_federated
# MAGIC USING CONNECTION azure_sql_connection
# MAGIC OPTIONS (
# MAGIC   database '<your-database-name>'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View schemas in the federated catalog
# MAGIC SHOW SCHEMAS IN azure_sql_federated;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Query Data from Federated Catalog (Azure SQL)
# MAGIC
# MAGIC Now we can query the Azure SQL data without moving it into Databricks.
# MAGIC For this demo, let's assume we have customer or security holdings data in Azure SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Query customer holdings data from Azure SQL (federated)
# MAGIC -- This data stays in Azure SQL and is queried in place
# MAGIC
# MAGIC SELECT *
# MAGIC FROM azure_sql_federated.dbo.customer_holdings
# MAGIC LIMIT 10;

# COMMAND ----------

# Read federated data into a DataFrame for processing
customer_holdings_df = spark.sql("""
    SELECT
        customer_id,
        ticker_symbol,
        shares_held,
        purchase_date,
        cost_basis
    FROM azure_sql_federated.dbo.customer_holdings
""")

display(customer_holdings_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Query Data from Managed Catalog (FactSet Data)
# MAGIC
# MAGIC Now let's access FactSet financial data from the Databricks managed catalog.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Query FactSet Fundamentals data from managed catalog
# MAGIC -- This is data available through Databricks Marketplace
# MAGIC
# MAGIC SELECT *
# MAGIC FROM factset_catalog.ff_basic.annual_financials
# MAGIC LIMIT 10;

# COMMAND ----------

# Read FactSet data into a DataFrame
factset_financials_df = spark.sql("""
    SELECT
        ticker,
        fiscal_year,
        revenue,
        net_income,
        total_assets,
        total_equity,
        eps_basic
    FROM factset_catalog.ff_basic.annual_financials
    WHERE fiscal_year >= 2020
""")

display(factset_financials_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Combine and Process Federated + Managed Data
# MAGIC
# MAGIC Now we'll join customer holdings (Azure SQL) with FactSet financials (Databricks) to create insights.
# MAGIC This demonstrates processing data across sources **without moving it into Databricks**.

# COMMAND ----------

from pyspark.sql import functions as F

# Join customer holdings with FactSet financial data
combined_df = customer_holdings_df.join(
    factset_financials_df,
    customer_holdings_df.ticker_symbol == factset_financials_df.ticker,
    "left"
)

# Calculate portfolio metrics
portfolio_analysis = combined_df.groupBy("customer_id").agg(
    F.count("ticker_symbol").alias("num_holdings"),
    F.sum("shares_held").alias("total_shares"),
    F.sum(F.col("shares_held") * F.col("cost_basis")).alias("total_investment"),
    F.avg("revenue").alias("avg_company_revenue"),
    F.avg("net_income").alias("avg_company_net_income"),
    F.avg("eps_basic").alias("avg_eps")
)

display(portfolio_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Advanced Analytics - Portfolio Risk Assessment
# MAGIC
# MAGIC Let's create a more sophisticated analysis that combines both data sources.

# COMMAND ----------

# Calculate portfolio value and financial health metrics
portfolio_health = combined_df.withColumn(
    "investment_value",
    F.col("shares_held") * F.col("cost_basis")
).withColumn(
    "company_health_score",
    F.when(F.col("net_income") > 0,
           (F.col("total_equity") / F.col("total_assets")) * 100
    ).otherwise(0)
).select(
    "customer_id",
    "ticker_symbol",
    "shares_held",
    "investment_value",
    "revenue",
    "net_income",
    "company_health_score",
    "fiscal_year"
)

display(portfolio_health)

# COMMAND ----------

# Identify high-risk holdings (companies with negative net income)
high_risk_holdings = portfolio_health.filter(
    F.col("net_income") < 0
).select(
    "customer_id",
    "ticker_symbol",
    "investment_value",
    "net_income",
    "company_health_score"
).orderBy(F.col("investment_value").desc())

display(high_risk_holdings)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Summary Dashboard Data
# MAGIC
# MAGIC Generate aggregated data suitable for dashboard visualization.

# COMMAND ----------

# Customer portfolio summary
customer_summary = combined_df.groupBy("customer_id").agg(
    F.count("ticker_symbol").alias("total_holdings"),
    F.sum(F.col("shares_held") * F.col("cost_basis")).alias("portfolio_value"),
    F.countDistinct("ticker_symbol").alias("unique_tickers"),
    F.avg("revenue").alias("avg_holding_revenue"),
    F.sum(F.when(F.col("net_income") < 0, 1).otherwise(0)).alias("negative_income_holdings")
).withColumn(
    "risk_level",
    F.when(F.col("negative_income_holdings") > F.col("total_holdings") * 0.3, "High")
     .when(F.col("negative_income_holdings") > F.col("total_holdings") * 0.1, "Medium")
     .otherwise("Low")
)

display(customer_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Save Results to Delta Table (Optional)
# MAGIC
# MAGIC If needed, we can persist the processed results in Delta format for faster access.
# MAGIC Note: We're only storing the **processed results**, not the raw federated data.

# COMMAND ----------

# Save portfolio analysis results to a Delta table
customer_summary.write.format("delta").mode("overwrite").saveAsTable(
    "main.default.customer_portfolio_analysis"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **No Data Movement Required**: Customer holdings data remained in Azure SQL throughout the entire analysis
# MAGIC 2. **Unified Query Interface**: Used standard SQL and Spark to query both federated and managed data sources
# MAGIC 3. **Real-time Access**: Queries execute directly against Azure SQL, ensuring up-to-date data
# MAGIC 4. **Security & Compliance**: Sensitive customer data stays in the regulated Azure SQL environment
# MAGIC 5. **Cost Effective**: No data transfer costs or storage duplication
# MAGIC 6. **Flexible Architecture**: Supports hybrid cloud architectures common in regulated industries
# MAGIC
# MAGIC **Use Cases**:
# MAGIC - Financial services with regulatory requirements
# MAGIC - Healthcare with HIPAA-compliant databases
# MAGIC - Enterprises with existing on-premise/cloud databases
# MAGIC - Multi-cloud architectures requiring unified analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Examples: Different Join Patterns

# COMMAND ----------

# Example: Filter federated data before joining (pushdown optimization)
# Databricks pushes filters to Azure SQL for better performance
recent_holdings = spark.sql("""
    SELECT
        customer_id,
        ticker_symbol,
        shares_held,
        cost_basis
    FROM azure_sql_federated.dbo.customer_holdings
    WHERE purchase_date >= '2023-01-01'
""")

# Join with FactSet for recent investments only
recent_analysis = recent_holdings.join(
    factset_financials_df,
    recent_holdings.ticker_symbol == factset_financials_df.ticker,
    "inner"
).select(
    "customer_id",
    "ticker_symbol",
    "shares_held",
    "revenue",
    "net_income",
    "eps_basic"
)

display(recent_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization Tips
# MAGIC
# MAGIC 1. **Predicate Pushdown**: Filter data in SQL before joining to minimize data transfer
# MAGIC 2. **Broadcast Joins**: For smaller federated tables, use broadcast joins
# MAGIC 3. **Caching**: Cache frequently accessed federated data in Spark for repeated queries
# MAGIC 4. **Indexing**: Ensure proper indexes on Azure SQL tables for join columns
# MAGIC 5. **Connection Pooling**: Reuse connections for multiple queries

# COMMAND ----------

# Example: Cache federated data for multiple operations
customer_holdings_df.cache()

# Perform multiple operations on cached data
summary1 = customer_holdings_df.groupBy("customer_id").count()
summary2 = customer_holdings_df.groupBy("ticker_symbol").agg(F.sum("shares_held"))

# Unpersist when done
customer_holdings_df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment to remove the foreign catalog
# MAGIC -- DROP CATALOG IF EXISTS azure_sql_federated CASCADE;
# MAGIC
# MAGIC -- Uncomment to remove the connection
# MAGIC -- DROP CONNECTION IF EXISTS azure_sql_connection;
