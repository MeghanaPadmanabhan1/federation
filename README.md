# Lakehouse Federation with FactSet Demo

## 📋 Overview

This repository contains a complete demonstration of **Databricks Lakehouse Federation** combined with **FactSet financial data** from the Databricks Marketplace. It showcases how to query on-premise portfolio data **without moving it** and join it with FactSet fundamentals and estimates for real-time investment analysis.

### Key Message
**You don't need to physically move secure data into Databricks to analyze it.**

---

## 🎯 Use Case

### The Scenario
A financial services firm has:
- **Customer portfolio data** in an on-premise SQL Server (ticker symbols, shares held, cost basis)
- **FactSet financial data** available via Databricks Marketplace (fundamentals, estimates)
- **Security/compliance requirements** preventing portfolio data from moving to the cloud

### The Challenge
- FactSet uses **FactSet Sim ID** (unique identifier), not standard ticker symbols
- Need to combine on-premise portfolio with cloud-based FactSet data
- Must maintain data security and compliance

### The Solution
**Lakehouse Federation** allows querying on-premise data in place and joining it with FactSet data through a symbology mapping table.

```
Portfolio (ticker)  →  Symbology (ticker → ID)  →  FactSet Data (ID)
  [On-Premise]            [Databricks]                [Databricks]
       ↓                       ↓                           ↓
  STAYS THERE            Maps ticker              Fundamentals +
  (federated)            to FactSet ID               Estimates
```

---

## 📁 Repository Structure

### Setup runbook

1. **`SETUP_GUIDE.md`** ⭐ **READ THIS FIRST**
   - The single end-to-end runbook. Walks through generating sample data, loading it into your on-prem SQL Server, installing FactSet from Marketplace, creating the federation connection and foreign catalog, deploying the notebooks, running the workflow, importing the dashboard, and verification queries.

### Notebooks

2. **`generate_sample_holdings.py`**
   - Produces `~/equity_holdings.csv` plus the SQL Server `CREATE TABLE dbo.equity_holdings` DDL.

3. **`create_metrics_and_genie.py`**
   - One-time setup: creates the `mp_catalog.analytics.portfolio_metrics` Metric View and the Genie space *Personal Investment Portfolio Assistant*.

4. **`jobs/`** — seven notebooks orchestrated by the **Portfolio Federation Pipeline** workflow:
   - `read_factset_from_marketplace`, `read_portfolio_from_onprem_sql_server`
   - `join_portfolio_with_factset` — the core federation join, defined as a non-materialized VIEW
   - `build_dashboard_views` — all derived dashboard views
   - `refresh_metric_view`, `refresh_aibi_dashboard`, `refresh_genie_room`

5. **`blog.ipynb`** — the companion blog post.

### Dashboard

6. **`portfolio_dashboard.lvdash.json`** — the AI/BI dashboard definition.

---

## 🚀 Quick Start

### Prerequisites

1. **Databricks Workspace** (Azure, AWS, or GCP)
   - Unity Catalog enabled
   - Permissions to create connections and catalogs

2. **FactSet Data** from Databricks Marketplace
   - Install from Marketplace with your FactSet license
   - Note the catalog name (e.g., `factset_catalog`)

3. **SQL Database** (on-premise or cloud)
   - Azure SQL, SQL Server, PostgreSQL, etc.
   - Network connectivity from Databricks
   - Credentials with SELECT permissions

### Step 1: Generate Sample Holdings Data

Run **`generate_sample_holdings.py`** (in Databricks or any local Python 3 environment — no SDK or extra packages required). It produces:

- `~/equity_holdings.csv` — 200 rows of synthetic equity positions using real US tickers, matching the schema this demo expects.
- A `CREATE TABLE dbo.equity_holdings (...)` DDL block printed to stdout.

```bash
python3 generate_sample_holdings.py
```

### Step 2: Load the Sample Data into Your On-Premise SQL Server

In your on-prem Microsoft SQL Server:

1. Run the `CREATE TABLE dbo.equity_holdings` DDL printed by Step 1.
2. Bulk-load `equity_holdings.csv` into that table (use whatever tool your environment supports — `BULK INSERT`, `bcp`, the SSMS Import Wizard, Azure Data Studio import, etc.). The CSV has a header row and three columns matching the table definition.

The point of these two steps is just to get a `dbo.equity_holdings` table populated in your SQL Server so the rest of the demo has something to federate over. We deliberately don't prescribe the load mechanism — use whatever fits your environment.

### Step 3: Configure Databricks Secrets

```bash
# Create secret scope
databricks secrets create-scope --scope onprem-secrets

# Add password
databricks secrets put-secret \
  --scope onprem-secrets \
  --key sql-password \
  --string-value "your-password"
```

### Step 4: Upload Demo Notebook

1. Upload the `jobs/` notebooks (and `create_metrics_and_genie.py`) to your Databricks workspace, or clone this repo as a Git folder
2. Attach to a cluster with Unity Catalog enabled
3. Update connection parameters:
   - SQL Server host
   - Database name
   - Secret scope name
   - FactSet catalog name

### Step 5: Run the Demo

Open the notebook and execute cells in order. The notebook will:
1. Create a connection to your on-premise database
2. Create a foreign catalog for federated access
3. Query portfolio data (stays on-premise)
4. Map tickers to FactSet IDs using symbology table
5. Join with FactSet fundamentals and estimates
6. Generate investment insights

---

## 🔑 Key Concepts

### What is Lakehouse Federation?

Lakehouse Federation allows Databricks to query external data sources **without moving the data**. Data stays in its original location while you can query it using standard SQL.

### The FactSet Challenge

FactSet data uses **FactSet Entity IDs** instead of ticker symbols:

```sql
-- ❌ This won't work - no ticker column
SELECT *
FROM factset_catalog.ff_basic.ff_basic_af
WHERE ticker = 'MSFT';

-- ✅ Must use factset_entity_id
SELECT *
FROM factset_catalog.ff_basic.ff_basic_af
WHERE factset_entity_id = '0016YD-E';
```

### The Solution: Symbology Table

The symbology table maps tickers to FactSet IDs:

```sql
SELECT ticker, factset_entity_id, proper_name
FROM factset_catalog.sym_basic.sym_coverage
WHERE ticker = 'MSFT';

-- Output:
-- ticker: MSFT
-- factset_entity_id: 0016YD-E
-- proper_name: Microsoft Corporation
```

### The Three-Way Join

```sql
SELECT
  portfolio.ticker_symbol,
  symbology.proper_name,
  portfolio.shares_held,
  factset.revenue,
  factset.net_income
FROM onprem.dbo.customer_holdings AS portfolio
JOIN factset.sym_basic.sym_coverage AS symbology
  ON portfolio.ticker_symbol = symbology.ticker
JOIN factset.ff_basic.ff_basic_af AS factset
  ON symbology.factset_entity_id = factset.factset_entity_id
WHERE factset.fiscal_year = 2023;
```

---

## 📊 Demo Highlights

### 1. No Data Movement
Portfolio data stays in the on-premise SQL Server throughout the entire analysis.

### 2. Real-Time Insights
Queries execute against live data - no ETL lag.

### 3. Unified Analytics
Single interface (SQL/PySpark) to query both federated and managed data.

### 4. Investment Decisions
Combine historical performance with forward-looking analyst estimates:
- Portfolio holdings (on-prem)
- FactSet fundamentals (historical)
- FactSet estimates (forward-looking)
= Investment recommendations

### 5. Security Maintained
Sensitive customer data never leaves the approved, compliant database.

---

## 💡 Use Cases

### Financial Services
- Portfolio analysis with market data
- Risk assessment across holdings
- Client reporting with FactSet insights
- Compliance reporting without data movement

### Wealth Management
- Personalized investment recommendations
- Portfolio rebalancing suggestions
- Performance attribution analysis
- Tax-loss harvesting opportunities

### Hedge Funds
- Multi-strategy portfolio analysis
- Factor exposure analysis
- Alpha generation insights
- Real-time risk monitoring

---

## 🔧 Customization Guide

### Update Connection Parameters

In the notebook, modify these values:

```python
# Connection details
host = "your-server.database.windows.net"
database = "PortfolioDB"
user = "your-username"
secret_scope = "onprem-secrets"
secret_key = "sql-password"

# FactSet catalog name
factset_catalog = "factset_catalog"  # or your catalog name
```

### Add Your Own Tickers

Edit `onprem_portfolio_setup.sql` and add your ticker symbols:

```sql
INSERT INTO dbo.customer_holdings (customer_id, ticker_symbol, shares_held, cost_basis, purchase_date, account_type, account_number)
VALUES
    (1008, 'YOUR_TICKER', 100.0000, 150.0000, '2024-01-15', 'Brokerage', 'BRK-1008-001');
```

### Extend the Analysis

Add more FactSet tables:
- `ff_basic.ff_basic_qf` - Quarterly financials
- `fe_basic.fe_basic_sales` - Sales estimates
- `fe_basic.fe_basic_rec` - Analyst recommendations

---

## 📈 Performance Optimization

### 1. Predicate Pushdown
Filters are automatically pushed to the source database:

```sql
-- Filter executes in SQL Server, not Databricks
SELECT *
FROM portfolio_federated.dbo.customer_holdings
WHERE customer_id = 1001;  -- Pushed down
```

### 2. Projection Pushdown
Only selected columns are transferred:

```sql
-- Only transfers 2 columns, not all
SELECT ticker_symbol, shares_held
FROM portfolio_federated.dbo.customer_holdings;
```

### 3. Cache Frequently Used Tables

```python
# Cache symbology for repeated joins
sym = spark.table("factset_catalog.sym_basic.sym_coverage")
sym.cache()
```

### 4. Index Your On-Prem Tables

```sql
-- On SQL Server
CREATE INDEX IX_ticker ON customer_holdings(ticker_symbol);
CREATE INDEX IX_customer ON customer_holdings(customer_id);
```

---

## 🐛 Troubleshooting

### Connection Issues

```sql
-- Test connection
SHOW CONNECTIONS;

-- Check connection details
DESCRIBE CONNECTION onprem_sql_connection;
```

### No Results from Join

```sql
-- Verify tickers exist in symbology
SELECT ticker
FROM factset_catalog.sym_basic.sym_coverage
WHERE ticker IN (
  SELECT DISTINCT ticker_symbol
  FROM portfolio_federated.dbo.customer_holdings
);
```

### Slow Queries

```sql
-- Check query plan
EXPLAIN FORMATTED
SELECT *
FROM portfolio_federated.dbo.customer_holdings
WHERE customer_id = 1001;

-- Look for "Scan JDBCRelation" (good)
-- Look for predicate pushdown (good)
```

---

## 📚 Additional Resources

### Databricks Documentation
- [Lakehouse Federation](https://docs.databricks.com/query-federation/)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/)
- [Foreign Catalogs](https://docs.databricks.com/query-federation/index.html)

### FactSet Documentation
- [FactSet on Databricks Marketplace](https://marketplace.databricks.com/)
- Browse FactSet schema documentation in the Marketplace listing

### Best Practices
- See `SETUP_GUIDE.md` for the full end-to-end runbook (federation connection, FactSet install, workflow setup, dashboard import, troubleshooting).

---

## 🎯 Blog Messaging

### Key Points for Your Blog

1. **The Problem**
   - Regulated industries have data in approved, secure systems
   - Want to combine with cloud analytics (FactSet)
   - Can't move data due to security/compliance

2. **The Solution**
   - Lakehouse Federation queries data in place
   - No ETL, no data movement
   - Unified SQL interface

3. **The FactSet Twist**
   - FactSet uses entity IDs, not tickers
   - Symbology table provides the mapping
   - Three-way join enables seamless integration

4. **The Value**
   - Security: Data stays in approved locations
   - Compliance: No additional governance burden
   - Cost: No duplication or transfer fees
   - Agility: Real-time insights without pipelines

### Target Audience
- **Financial services IT leaders** concerned about data governance
- **Data engineers** managing hybrid architectures
- **Portfolio managers** wanting FactSet insights
- **Compliance officers** evaluating cloud solutions

---

## 🤝 Contributing

To extend this demo:

1. Add more FactSet schemas (FE estimates, FF fundamentals)
2. Include other data sources (Snowflake, PostgreSQL, etc.)
3. Create additional dashboard examples
4. Add MLflow integration for predictive models

---

## 📧 Support

For questions or issues:
- Databricks Federation: [Documentation](https://docs.databricks.com/query-federation/)
- FactSet Data: Databricks Marketplace support
- General setup: See `SETUP_GUIDE.md`

---

## ✅ Checklist

Use this checklist to ensure your demo is ready:

- [ ] On-premise database set up with portfolio data
- [ ] FactSet data accessible from Databricks Marketplace
- [ ] Databricks secrets configured with SQL password
- [ ] Network connectivity verified (Databricks → SQL Server)
- [ ] Connection and foreign catalog created
- [ ] Symbology table accessible
- [ ] Can query portfolio data (federated)
- [ ] Can join portfolio with FactSet data
- [ ] Demo notebook runs end-to-end
- [ ] Dashboard view created successfully

---

## 📄 License

This demo code is provided for educational and demonstration purposes.

**FactSet Data:** Requires a valid FactSet license and Databricks Marketplace agreement.

---

**Ready to demonstrate Lakehouse Federation with FactSet?**

Start with `SETUP_GUIDE.md` and follow it end to end. 🚀
