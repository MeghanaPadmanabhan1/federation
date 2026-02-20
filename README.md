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

### Core Demo Notebooks

1. **`factset_federation_demo.py`** ⭐ **PRIMARY DEMO**
   - Complete end-to-end demonstration
   - Shows the three-way join pattern (Portfolio → Symbology → FactSet)
   - Includes fundamentals + estimates analysis
   - Investment decision support examples
   - **Use this for technical demos and blog content**

### Setup Guide

2. ** SETUP GUIDE **
   - setup guide
   - Includes instructions to setup connectivity to on-prem azure sql database

### Reference Documents

3. **`FACTSET_QUICK_REFERENCE.md`** ⭐ **KEY REFERENCE**
   - FactSet-specific join patterns
   - Symbology table usage
   - Common query patterns
   - Troubleshooting guide
   - **Keep this handy during development**

4. **`QUICK_REFERENCE.md`**
   - General federation commands
   - Connection setup
   - Performance optimization
   - Common troubleshooting

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

### Step 1: Set Up On-Premise Database

```bash
# Run the portfolio setup script in your SQL Server
sqlcmd -S your-server.database.windows.net \
       -d PortfolioDB \
       -U your-username \
       -P your-password \
       -i onprem_portfolio_setup.sql
```

Or execute `onprem_portfolio_setup.sql` in Azure Data Studio / SSMS.

### Step 2: Configure Databricks Secrets

```bash
# Create secret scope
databricks secrets create-scope --scope onprem-secrets

# Add password
databricks secrets put-secret \
  --scope onprem-secrets \
  --key sql-password \
  --string-value "your-password"
```

### Step 3: Upload Demo Notebook

1. Upload `factset_federation_demo.py` to your Databricks workspace
2. Attach to a cluster with Unity Catalog enabled
3. Update connection parameters:
   - SQL Server host
   - Database name
   - Secret scope name
   - FactSet catalog name

### Step 4: Run the Demo

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

## 🎬 5-Minute Demo Script

Use this script for live demonstrations:

```sql
-- 1. Show on-prem portfolio (30 sec)
SELECT * FROM portfolio_federated.dbo.customer_holdings LIMIT 5;

-- 2. Show FactSet uses IDs (30 sec)
SELECT * FROM factset_catalog.ff_basic.ff_basic_af LIMIT 5;

-- 3. Show symbology mapping (30 sec)
SELECT ticker, factset_entity_id, proper_name
FROM factset_catalog.sym_basic.sym_coverage
WHERE ticker IN ('MSFT', 'AAPL');

-- 4. The three-way join (2 min)
SELECT
  p.customer_id,
  p.ticker_symbol,
  s.proper_name,
  p.shares_held * p.cost_basis AS position_value,
  f.net_income,
  e.mean_estimate AS eps_est_2024
FROM portfolio_federated.dbo.customer_holdings p
JOIN factset_catalog.sym_basic.sym_coverage s
  ON p.ticker_symbol = s.ticker
JOIN factset_catalog.ff_basic.ff_basic_af f
  ON s.factset_entity_id = f.factset_entity_id
JOIN factset_catalog.fe_basic.fe_basic_eps e
  ON s.factset_entity_id = e.factset_entity_id
WHERE f.fiscal_year = 2023 AND e.fiscal_year = 2024
ORDER BY position_value DESC
LIMIT 10;

-- 5. Explain the value (1 min)
-- "Portfolio data NEVER moved"
-- "Queried in place using Lakehouse Federation"
-- "Combined with FactSet for real-time insights"
```

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
- See `FACTSET_QUICK_REFERENCE.md` for query patterns
- See `SETUP_GUIDE.md` for architecture guidance
- See `QUICK_REFERENCE.md` for general federation tips

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

Start with `factset_federation_demo.py` and the `FACTSET_QUICK_REFERENCE.md`! 🚀
