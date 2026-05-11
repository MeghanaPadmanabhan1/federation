# Databricks notebook source
# MAGIC %md
# MAGIC # Portfolio Semantic Layer: Metric Views & Genie Space
# MAGIC
# MAGIC This notebook creates:
# MAGIC 1. **Databricks Metric View** - Semantic layer with governed measures and dimensions
# MAGIC 2. **Genie Space** - Natural language interface for portfolio analytics
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Databricks Runtime 17.2+ (for Metric Views)
# MAGIC - `mp_catalog.analytics.my_portfolio_dashboard` view must exist
# MAGIC - Unity Catalog enabled workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Portfolio Metric View
# MAGIC
# MAGIC [Databricks Metric Views](https://docs.databricks.com/aws/en/metric-views/) provide a semantic layer with:
# MAGIC - **Measures**: Aggregations computed at query time (SUM, AVG, COUNT)
# MAGIC - **Dimensions**: Categorical attributes for filtering and grouping
# MAGIC - **Governed definitions**: Consistent business logic across dashboards, Genie, and alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the metric view was created
# MAGIC DESCRIBE EXTENDED mp_catalog.analytics.portfolio_metrics;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Test Metric View Queries
# MAGIC
# MAGIC Use the `MEASURE()` function to aggregate measures with flexible dimension grouping.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Portfolio Summary
# MAGIC SELECT
# MAGIC   MEASURE(`Total Holdings`) AS total_holdings,
# MAGIC   ROUND(MEASURE(`Current Annual Earnings`), 2) AS current_earnings,
# MAGIC   ROUND(MEASURE(`Projected Annual Earnings`), 2) AS projected_earnings,
# MAGIC   ROUND(MEASURE(`Portfolio Growth Rate`), 2) AS growth_rate_pct,
# MAGIC   MEASURE(`High Risk Count`) AS high_risk_stocks,
# MAGIC   MEASURE(`Buy Opportunities`) AS buy_opportunities,
# MAGIC   MEASURE(`Sell Recommendations`) AS sell_recommendations
# MAGIC FROM mp_catalog.analytics.portfolio_metrics;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- By Risk Category
# MAGIC SELECT
# MAGIC   `Risk Category`,
# MAGIC   MEASURE(`Total Holdings`) AS num_stocks,
# MAGIC   ROUND(MEASURE(`Current Annual Earnings`), 2) AS current_earnings,
# MAGIC   ROUND(MEASURE(`Projected Annual Earnings`), 2) AS projected_earnings
# MAGIC FROM mp_catalog.analytics.portfolio_metrics
# MAGIC GROUP BY `Risk Category`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- By Recommendation
# MAGIC SELECT
# MAGIC   `Recommendation`,
# MAGIC   MEASURE(`Total Holdings`) AS num_stocks,
# MAGIC   ROUND(MEASURE(`Current Annual Earnings`), 2) AS earnings_impact,
# MAGIC   ROUND(MEASURE(`Avg EPS Growth Rate`), 2) AS avg_growth_pct
# MAGIC FROM mp_catalog.analytics.portfolio_metrics
# MAGIC GROUP BY `Recommendation`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Genie Space
# MAGIC
# MAGIC Create a Genie Space for natural language queries about your portfolio.

# COMMAND ----------

# Genie Space Configuration
GENIE_SPACE_NAME = "Personal Investment Portfolio Assistant"

GENIE_DESCRIPTION = """
Ask questions about your personal investment portfolio in plain English.

This Genie space combines your on-premise portfolio holdings with FactSet
financial data to provide institutional-grade investment insights.

**Example Questions:**

📊 Portfolio Performance:
- "How is my portfolio projected to perform next year?"
- "What's my total current annual earnings?"
- "Which stocks contribute most to my earnings?"

📈 Growth Analysis:
- "Which stocks have the highest growth potential?"
- "Show me stocks with projected growth above 15%"
- "Compare current vs projected earnings for my top holdings"

⚠️ Risk Assessment:
- "Show me my high-risk holdings"
- "Which stocks have debt-to-equity above 2?"
- "What percentage of my portfolio is high risk?"

💡 Recommendations:
- "Which stocks should I consider selling?"
- "Show me strong buy opportunities"
- "What are my top performers?"

🔍 Deep Dive:
- "Give me details on my Microsoft position"
- "Which stocks have the best profit margins?"
- "Break down my portfolio by financial health"
"""

GENIE_INSTRUCTIONS = """
You are a personal investment advisor assistant with access to the user's portfolio data.

**Data Context:**
- Portfolio holdings are federated from an on-premise database (data never moves to cloud)
- Financial metrics come from FactSet institutional-grade data
- All earnings figures represent the USER's share of company earnings (shares × EPS)

**Key Measures (use with MEASURE() function):**
- `Current Annual Earnings`: User's current earnings from their shares
- `Projected Annual Earnings`: User's projected earnings based on analyst estimates
- `Portfolio Growth Rate`: Percentage growth in portfolio earnings
- `Avg EPS Growth Rate`: Average EPS growth across holdings
- `Avg Profit Margin`: Average profit margin of companies
- `Avg Debt to Equity`: Average leverage
- `High Risk Count`: Number of high-risk positions
- `Buy Opportunities`: Stocks recommended to buy
- `Sell Recommendations`: Stocks recommended to sell

**Key Dimensions (for filtering/grouping):**
- `Stock Symbol`: Individual stock (e.g., MSFT-US)
- `Risk Category`: High Risk, Medium Risk, Low Risk
- `Recommendation`: Strong Buy, Buy, Hold, Sell
- `Performance Tier`: Top Performer, Above Average, Average, Below Average, Underperformer
- `Financial Health`: Excellent, Good, Fair, Weak, Poor
- `Growth Outlook`: Strong Growth, Moderate Growth, Flat, Declining, Sharp Decline

**Response Guidelines:**
- Clarify when showing user's personal earnings vs company figures
- Include context about why stocks are categorized certain ways
- Suggest follow-up questions when appropriate
- Round monetary values to 2 decimal places
- Use the MEASURE() function for all aggregations
"""

# Tables for Genie Space
GENIE_TABLES = [
    "mp_catalog.analytics.portfolio_metrics",
    "mp_catalog.analytics.my_portfolio_dashboard"
]

print(f"Genie Space Configuration")
print("=" * 60)
print(f"Name: {GENIE_SPACE_NAME}")
print(f"\nTables: {', '.join(GENIE_TABLES)}")

# COMMAND ----------

# Create Genie Space using Databricks SDK
from databricks.sdk import WorkspaceClient

# Initialize workspace client
w = WorkspaceClient()

# Get current warehouse ID — spark.conf raises on serverless compute, so guard.
try:
    warehouse_id = spark.conf.get("spark.databricks.warehouse.id", None)
except Exception:
    warehouse_id = None

if not warehouse_id:
    # Try to get a warehouse
    warehouses = list(w.warehouses.list())
    if warehouses:
        warehouse_id = warehouses[0].id
        print(f"Using warehouse: {warehouses[0].name} ({warehouse_id})")
    else:
        print("⚠️ No SQL warehouse found. Please specify warehouse_id manually.")

# COMMAND ----------

# Create the Genie Space
try:
    genie_space = w.genie.create(
        display_name=GENIE_SPACE_NAME,
        description=GENIE_DESCRIPTION,
        warehouse_id=warehouse_id,
        table_identifiers=GENIE_TABLES
    )

    print(f"✅ Genie Space Created Successfully!")
    print(f"   ID: {genie_space.id}")
    print(f"   Name: {genie_space.display_name}")
    print(f"\n📍 Access your Genie Space at:")
    print(f"   {w.config.host}/genie/rooms/{genie_space.id}")

    # Store the ID for later use
    genie_space_id = genie_space.id

except Exception as e:
    print(f"⚠️ Could not create Genie Space via API: {str(e)}")
    print("\n" + "=" * 60)
    print("MANUAL CREATION STEPS:")
    print("=" * 60)
    print("\n1. Navigate to: Databricks Workspace → Genie")
    print("2. Click 'Create' → 'Genie Space'")
    print(f"3. Name: {GENIE_SPACE_NAME}")
    print(f"4. Add these tables:")
    for table in GENIE_TABLES:
        print(f"   - {table}")
    print("\n5. Add these instructions to the Genie Space:")
    print("-" * 40)
    print(GENIE_INSTRUCTIONS[:500] + "...")
    print("-" * 40)
    genie_space_id = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test Genie Space
# MAGIC
# MAGIC Once created, try these sample questions in your Genie Space:
# MAGIC
# MAGIC ### Portfolio Overview
# MAGIC - "How many stocks do I own and what are my total earnings?"
# MAGIC - "What's my portfolio's projected growth rate?"
# MAGIC - "Give me a summary of my portfolio health"
# MAGIC
# MAGIC ### Performance Analysis
# MAGIC - "Which are my top 5 performing stocks?"
# MAGIC - "Compare my top performers vs underperformers"
# MAGIC - "Show me stocks by performance tier"
# MAGIC
# MAGIC ### Risk Questions
# MAGIC - "What percentage of my portfolio is high risk?"
# MAGIC - "Show me stocks with debt-to-equity above 2"
# MAGIC - "Which stocks have the highest risk?"
# MAGIC
# MAGIC ### Investment Decisions
# MAGIC - "Which stocks should I sell?"
# MAGIC - "Show me buy opportunities"
# MAGIC - "What stocks should I buy more of?"
# MAGIC
# MAGIC ### Earnings Analysis
# MAGIC - "How much will I earn next year?"
# MAGIC - "Which stocks contribute most to my earnings?"
# MAGIC - "Show earnings change by risk category"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Objects Created
# MAGIC
# MAGIC | Object | Type | Location |
# MAGIC |--------|------|----------|
# MAGIC | `portfolio_metrics` | Metric View | `mp_catalog.analytics.portfolio_metrics` |
# MAGIC | Portfolio Assistant | Genie Space | Databricks Workspace → Genie |
# MAGIC
# MAGIC ### Metric View Dimensions
# MAGIC
# MAGIC | Dimension | Values | Use Case |
# MAGIC |-----------|--------|----------|
# MAGIC | Stock Symbol | MSFT-US, AAPL-US, ... | Individual stock analysis |
# MAGIC | Risk Category | High, Medium, Low | Risk filtering |
# MAGIC | Recommendation | Strong Buy, Buy, Hold, Sell | Action prioritization |
# MAGIC | Performance Tier | Top Performer → Underperformer | Segment analysis |
# MAGIC | Financial Health | Excellent → Poor | Quality filtering |
# MAGIC | Growth Outlook | Strong Growth → Sharp Decline | Forward-looking analysis |
# MAGIC
# MAGIC ### Metric View Measures
# MAGIC
# MAGIC | Measure | Aggregation | Description |
# MAGIC |---------|-------------|-------------|
# MAGIC | Total Holdings | COUNT DISTINCT | Number of stocks |
# MAGIC | Current Annual Earnings | SUM | Your current earnings |
# MAGIC | Projected Annual Earnings | SUM | Your projected earnings |
# MAGIC | Portfolio Growth Rate | Calculated | % earnings growth |
# MAGIC | Avg EPS Growth Rate | AVG | Average company growth |
# MAGIC | High Risk Count | COUNT | High-risk positions |
# MAGIC | Buy Opportunities | COUNT | Stocks to buy |
# MAGIC | Sell Recommendations | COUNT | Stocks to sell |
# MAGIC