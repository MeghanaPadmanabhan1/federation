# FactSet + Lakehouse Federation Quick Reference

## The FactSet Challenge

**Problem:** FactSet data uses **FactSet Sim ID** (unique identifier), not standard ticker symbols.

**Solution:** Use the **symbology table** to map tickers → FactSet IDs.

---

## Key FactSet Schemas

| Schema | Description | Example Tables |
|--------|-------------|----------------|
| **FF** | FactSet Fundamentals | `ff_basic_af` (annual financials), `ff_basic_qf` (quarterly) |
| **FE** | FactSet Estimates | `fe_basic_eps` (EPS estimates), `fe_basic_sales` |
| **SYM** | Symbology Mapping | `sym_coverage` (ticker → factset_entity_id) |

---

## The Three-Way Join Pattern

```
Portfolio (ticker)  →  Symbology (ticker → ID)  →  FactSet Data (ID)
  [On-Premise]            [Databricks]                [Databricks]
```

### Basic Pattern

```sql
SELECT
  portfolio.customer_id,
  portfolio.ticker_symbol,
  portfolio.shares_held,
  factset.revenue,
  factset.net_income
FROM onprem.dbo.customer_holdings AS portfolio
JOIN factset.sym_basic.sym_coverage AS sym
  ON portfolio.ticker_symbol = sym.ticker
JOIN factset.ff_basic.ff_basic_af AS factset
  ON sym.factset_entity_id = factset.factset_entity_id
WHERE factset.fiscal_year = 2023;
```

---

## Step-by-Step Setup

### 1. Access FactSet from Databricks Marketplace

```sql
-- After installing FactSet from Marketplace, verify access
SHOW SCHEMAS IN factset_catalog;

-- Should see:
-- ff_basic, fe_basic, sym_basic, etc.
```

### 2. Explore the Symbology Table

```sql
-- Map ticker symbols to FactSet IDs
SELECT
  ticker,
  factset_entity_id,
  proper_name AS company_name,
  entity_type
FROM factset_catalog.sym_basic.sym_coverage
WHERE ticker IN ('MSFT', 'AAPL', 'GOOGL')
LIMIT 10;
```

**Output Example:**
| ticker | factset_entity_id | company_name | entity_type |
|--------|------------------|--------------|-------------|
| MSFT   | 0016YD-E         | Microsoft Corporation | EQY |
| AAPL   | 000C7F-E         | Apple Inc. | EQY |
| GOOGL  | 02079K-E         | Alphabet Inc. | EQY |

### 3. Create On-Prem Connection

```sql
CREATE CONNECTION onprem_sql
TYPE sqlserver
OPTIONS (
  host '<your-server>.database.windows.net',
  port '1433',
  user '<username>',
  password secret('onprem-secrets', 'password')
);
```

### 4. Create Foreign Catalog

```sql
CREATE CATALOG portfolio_federated
USING CONNECTION onprem_sql
OPTIONS (
  database 'PortfolioDB'
);
```

---

## Common Query Patterns

### Pattern 1: Portfolio + Fundamentals

```sql
-- Enrich portfolio with company financials
WITH portfolio AS (
  SELECT
    customer_id,
    ticker_symbol,
    shares_held,
    cost_basis
  FROM portfolio_federated.dbo.customer_holdings
)

SELECT
  p.customer_id,
  p.ticker_symbol,
  s.proper_name,
  p.shares_held,
  p.shares_held * p.cost_basis AS position_value,
  f.sales AS revenue,
  f.net_income,
  f.eps_basic
FROM portfolio p
JOIN factset_catalog.sym_basic.sym_coverage s
  ON p.ticker_symbol = s.ticker
JOIN factset_catalog.ff_basic.ff_basic_af f
  ON s.factset_entity_id = f.factset_entity_id
WHERE f.fiscal_year = 2023
ORDER BY position_value DESC;
```

### Pattern 2: Portfolio + Analyst Estimates

```sql
-- Show portfolio with forward-looking estimates
SELECT
  p.customer_id,
  p.ticker_symbol,
  s.proper_name,
  p.shares_held,
  e.mean_estimate AS consensus_eps_2024,
  e.high_estimate,
  e.low_estimate,
  e.num_estimates AS analyst_count,
  p.shares_held * e.mean_estimate AS projected_earnings
FROM portfolio_federated.dbo.customer_holdings p
JOIN factset_catalog.sym_basic.sym_coverage s
  ON p.ticker_symbol = s.ticker
JOIN factset_catalog.fe_basic.fe_basic_eps e
  ON s.factset_entity_id = e.factset_entity_id
WHERE e.fiscal_year = 2024
ORDER BY projected_earnings DESC;
```

### Pattern 3: Combined Historical + Forward

```sql
-- Investment decision support: past performance + future estimates
WITH portfolio AS (
  SELECT
    customer_id,
    ticker_symbol,
    shares_held,
    cost_basis
  FROM portfolio_federated.dbo.customer_holdings
),
symbology AS (
  SELECT ticker, factset_entity_id, proper_name
  FROM factset_catalog.sym_basic.sym_coverage
),
fundamentals AS (
  SELECT
    factset_entity_id,
    eps_basic AS eps_2023
  FROM factset_catalog.ff_basic.ff_basic_af
  WHERE fiscal_year = 2023
),
estimates AS (
  SELECT
    factset_entity_id,
    mean_estimate AS eps_est_2024
  FROM factset_catalog.fe_basic.fe_basic_eps
  WHERE fiscal_year = 2024
)

SELECT
  p.customer_id,
  p.ticker_symbol,
  s.proper_name,
  p.shares_held * p.cost_basis AS position_value,
  f.eps_2023,
  e.eps_est_2024,
  ROUND(((e.eps_est_2024 - f.eps_2023) / f.eps_2023) * 100, 2) AS eps_growth_pct,
  CASE
    WHEN ((e.eps_est_2024 - f.eps_2023) / f.eps_2023) > 0.15 THEN 'Strong Buy'
    WHEN ((e.eps_est_2024 - f.eps_2023) / f.eps_2023) > 0.05 THEN 'Buy'
    WHEN ((e.eps_est_2024 - f.eps_2023) / f.eps_2023) > -0.05 THEN 'Hold'
    ELSE 'Sell'
  END AS recommendation
FROM portfolio p
JOIN symbology s ON p.ticker_symbol = s.ticker
JOIN fundamentals f ON s.factset_entity_id = f.factset_entity_id
JOIN estimates e ON s.factset_entity_id = e.factset_entity_id
ORDER BY position_value DESC;
```

---

## Important FactSet Tables

### Fundamentals (FF Schema)

```sql
-- Annual financials
SELECT
  factset_entity_id,
  fiscal_year,
  sales,           -- Revenue
  net_income,      -- Net income
  total_assets,
  total_equity,
  eps_basic        -- Basic EPS
FROM factset_catalog.ff_basic.ff_basic_af
WHERE fiscal_year = 2023;
```

### Estimates (FE Schema)

```sql
-- EPS estimates
SELECT
  factset_entity_id,
  fiscal_year,
  mean_estimate,    -- Consensus estimate
  high_estimate,
  low_estimate,
  num_estimates,    -- Number of analysts
  standard_deviation
FROM factset_catalog.fe_basic.fe_basic_eps
WHERE fiscal_year = 2024;
```

### Symbology (SYM Schema)

```sql
-- Ticker to ID mapping
SELECT
  ticker,
  factset_entity_id,
  proper_name,
  entity_type,
  iso_country
FROM factset_catalog.sym_basic.sym_coverage
WHERE ticker = 'MSFT';
```

---

## PySpark Pattern

```python
from pyspark.sql import functions as F

# Portfolio (federated - stays on-prem)
portfolio = spark.sql("""
    SELECT
        customer_id,
        ticker_symbol,
        shares_held,
        cost_basis
    FROM portfolio_federated.dbo.customer_holdings
""")

# Symbology (Databricks)
symbology = spark.sql("""
    SELECT
        ticker,
        factset_entity_id,
        proper_name
    FROM factset_catalog.sym_basic.sym_coverage
""")

# Fundamentals (Databricks)
fundamentals = spark.sql("""
    SELECT
        factset_entity_id,
        sales,
        net_income,
        eps_basic
    FROM factset_catalog.ff_basic.ff_basic_af
    WHERE fiscal_year = 2023
""")

# Three-way join
result = portfolio \
    .join(symbology, "ticker_symbol" == "ticker") \
    .join(fundamentals, symbology.factset_entity_id == fundamentals.factset_entity_id) \
    .select(
        "customer_id",
        "ticker_symbol",
        "proper_name",
        "shares_held",
        (F.col("shares_held") * F.col("cost_basis")).alias("position_value"),
        "sales",
        "net_income",
        "eps_basic"
    )

display(result)
```

---

## Key Metadata

FactSet tables have excellent metadata in column comments:

```sql
-- View column descriptions
DESCRIBE EXTENDED factset_catalog.ff_basic.ff_basic_af;

-- Or use SHOW COLUMNS
SHOW COLUMNS IN factset_catalog.ff_basic.ff_basic_af;
```

This is crucial because column names like `sales`, `ni`, `at`, etc. are not intuitive without the descriptions.

---

## Performance Tips

### 1. Filter Before Joining

```sql
-- ✅ Good: Filter federated data first
WITH recent_portfolio AS (
  SELECT *
  FROM portfolio_federated.dbo.customer_holdings
  WHERE purchase_date >= '2023-01-01'  -- Pushed to SQL Server
)
SELECT *
FROM recent_portfolio p
JOIN factset_catalog.sym_basic.sym_coverage s
  ON p.ticker_symbol = s.ticker;
```

### 2. Use Specific Fiscal Years

```sql
-- ✅ Good: Specify fiscal year to reduce data scanned
WHERE fiscal_year = 2023

-- ❌ Avoid: Scanning all years
WHERE fiscal_year >= 2020
```

### 3. Cache Symbology for Repeated Queries

```python
# Cache symbology table for multiple joins
symbology_df = spark.table("factset_catalog.sym_basic.sym_coverage")
symbology_df.cache()

# Use in multiple queries
result1 = portfolio1.join(symbology_df, ...)
result2 = portfolio2.join(symbology_df, ...)

# Unpersist when done
symbology_df.unpersist()
```

---

## Troubleshooting

### No Results from Join?

**Problem:** Ticker symbol doesn't match

```sql
-- Check if ticker exists in symbology
SELECT *
FROM factset_catalog.sym_basic.sym_coverage
WHERE ticker = 'MSFT';

-- Check exact ticker in your portfolio
SELECT DISTINCT ticker_symbol
FROM portfolio_federated.dbo.customer_holdings;
```

**Fix:** Ensure ticker symbols are uppercase and match FactSet format.

### Missing FactSet Entity ID?

```sql
-- Verify the join key exists
SELECT
  p.ticker_symbol,
  s.factset_entity_id,
  s.proper_name
FROM portfolio_federated.dbo.customer_holdings p
LEFT JOIN factset_catalog.sym_basic.sym_coverage s
  ON p.ticker_symbol = s.ticker
WHERE s.factset_entity_id IS NULL;
```

### Slow Queries?

```sql
-- Check query plan
EXPLAIN FORMATTED
SELECT *
FROM portfolio_federated.dbo.customer_holdings p
JOIN factset_catalog.sym_basic.sym_coverage s
  ON p.ticker_symbol = s.ticker;

-- Look for:
-- ✅ "Predicate pushdown" to SQL Server
-- ✅ "Broadcast join" for small tables
```

---

## Blog Demo Script (5 minutes)

```sql
-- 1. Show on-prem portfolio (30 sec)
SELECT * FROM portfolio_federated.dbo.customer_holdings LIMIT 5;
-- "This data is in our on-premise SQL Server"

-- 2. Show FactSet uses IDs, not tickers (30 sec)
SELECT * FROM factset_catalog.ff_basic.ff_basic_af LIMIT 5;
-- "Notice: no ticker column, only factset_entity_id"

-- 3. Show symbology mapping (30 sec)
SELECT ticker, factset_entity_id, proper_name
FROM factset_catalog.sym_basic.sym_coverage
WHERE ticker IN ('MSFT', 'AAPL', 'GOOGL');
-- "This table maps tickers to FactSet IDs"

-- 4. The magic: three-way join (2 min)
SELECT
  p.customer_id,
  p.ticker_symbol,
  s.proper_name,
  p.shares_held,
  f.sales AS revenue,
  f.net_income,
  e.mean_estimate AS eps_est_2024
FROM portfolio_federated.dbo.customer_holdings p
JOIN factset_catalog.sym_basic.sym_coverage s
  ON p.ticker_symbol = s.ticker
JOIN factset_catalog.ff_basic.ff_basic_af f
  ON s.factset_entity_id = f.factset_entity_id AND f.fiscal_year = 2023
JOIN factset_catalog.fe_basic.fe_basic_eps e
  ON s.factset_entity_id = e.factset_entity_id AND e.fiscal_year = 2024
ORDER BY p.shares_held * p.cost_basis DESC
LIMIT 10;

-- 5. Key message (1 min)
-- "Portfolio data NEVER left the on-premise database"
-- "We joined it with FactSet data using Lakehouse Federation"
-- "No ETL, no data movement, real-time insights"
```

---

## Resources

- [FactSet on Databricks Marketplace](https://marketplace.databricks.com/)
- [Lakehouse Federation Docs](https://docs.databricks.com/query-federation/)
- [FactSet Data Structures](https://docs.databricks.com/) (Marketplace provider docs)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/)

---

## Key Messaging for Blog

### The Traditional Problem
❌ Customer has portfolio in on-prem SQL (can't move due to security)
❌ Wants to use FactSet data for analysis
❌ Would need complex ETL to combine both sources
❌ Data becomes stale, compliance burden increases

### The Federation Solution
✅ Query on-prem portfolio **in place** (no movement)
✅ Join with FactSet data from Marketplace
✅ Use symbology table to map tickers → FactSet IDs
✅ Real-time insights, zero data duplication
✅ Maintain security posture and compliance

### Why It Matters
- **Security**: Sensitive portfolio data stays in approved systems
- **Compliance**: No additional governance burden
- **Cost**: No data transfer or storage duplication
- **Agility**: Leverage FactSet immediately without migration
- **Real-time**: Always querying current data
