# Lakehouse Federation Quick Reference

## Core Concept
**Query data across multiple sources without moving it into Databricks**

```
┌─────────────────────────────────────────┐
│      Databricks (Query Engine)          │
│  ┌────────────┐      ┌──────────────┐  │
│  │ Federated  │      │   Managed    │  │
│  │ (External) │      │ (Lakehouse)  │  │
│  └─────┬──────┘      └──────┬───────┘  │
└────────┼────────────────────┼───────────┘
         │                    │
         ▼                    ▼
    Azure SQL            Delta Lake
  (stays there)      (Databricks storage)
```

---

## 1. Create Connection (One-Time Setup)

```sql
CREATE CONNECTION azure_sql_connection
TYPE sqlserver
OPTIONS (
  host '<server>.database.windows.net',
  port '1433',
  user '<username>',
  password secret('<scope>', '<key>')
);
```

**Supported Types:**
- `sqlserver` - Azure SQL, SQL Server
- `mysql` - MySQL, MariaDB
- `postgresql` - PostgreSQL
- `snowflake` - Snowflake
- `databricks` - Databricks (cross-workspace)

---

## 2. Create Foreign Catalog

```sql
CREATE CATALOG azure_sql_federated
USING CONNECTION azure_sql_connection
OPTIONS (
  database '<database-name>'
);
```

**Result:** All tables in Azure SQL are now queryable as:
```
azure_sql_federated.<schema>.<table>
```

---

## 3. Query Federated Data

```sql
-- Simple query (data stays in Azure SQL)
SELECT *
FROM azure_sql_federated.dbo.customer_holdings
WHERE purchase_date >= '2023-01-01';
```

**Key Point:** Databricks pushes filters and projections to the source database (predicate/projection pushdown)

---

## 4. Join Federated + Managed Data

```sql
-- Unified query across sources
SELECT
  h.customer_id,
  h.ticker_symbol,
  h.shares_held,
  f.revenue,
  f.net_income
FROM azure_sql_federated.dbo.customer_holdings h
JOIN factset_catalog.ff_basic.annual_financials f
  ON h.ticker_symbol = f.ticker
WHERE f.fiscal_year = 2023;
```

---

## 5. PySpark Integration

```python
# Read federated data
holdings = spark.sql("""
    SELECT * FROM azure_sql_federated.dbo.customer_holdings
""")

# Read managed data
financials = spark.sql("""
    SELECT * FROM factset.ff_basic.annual_financials
""")

# Join and process
result = holdings.join(financials, "ticker_symbol")
```

---

## 6. Performance Optimization

### Predicate Pushdown (Automatic)
```sql
-- Filter executes in Azure SQL, not Databricks
SELECT *
FROM azure_sql_federated.dbo.customer_holdings
WHERE customer_id = 1001;  -- Pushed down
```

### Projection Pushdown (Automatic)
```sql
-- Only transfers selected columns
SELECT customer_id, ticker_symbol
FROM azure_sql_federated.dbo.customer_holdings;  -- Only 2 columns transferred
```

### Caching (Manual)
```python
# Cache frequently accessed federated data
holdings_df = spark.table("azure_sql_federated.dbo.customer_holdings")
holdings_df.cache()

# Use multiple times
result1 = holdings_df.filter("customer_id = 1001")
result2 = holdings_df.groupBy("ticker_symbol").count()

# Unpersist when done
holdings_df.unpersist()
```

---

## 7. Useful Commands

### List Connections
```sql
SHOW CONNECTIONS;
```

### Describe Connection
```sql
DESCRIBE CONNECTION azure_sql_connection;
```

### List Schemas in Foreign Catalog
```sql
SHOW SCHEMAS IN azure_sql_federated;
```

### List Tables in Schema
```sql
SHOW TABLES IN azure_sql_federated.dbo;
```

### Show Query Plan (Check Pushdown)
```sql
EXPLAIN FORMATTED
SELECT * FROM azure_sql_federated.dbo.customer_holdings
WHERE customer_id = 1001;
```

### Grant Permissions
```sql
GRANT USE CATALOG ON CATALOG azure_sql_federated TO `user@example.com`;
GRANT SELECT ON CATALOG azure_sql_federated TO `user@example.com`;
```

---

## 8. Security Best Practices

### Store Credentials in Secrets
```bash
# Create secret scope
databricks secrets create-scope --scope azure-sql-secrets

# Add password
databricks secrets put-secret \
  --scope azure-sql-secrets \
  --key password \
  --string-value "your-password"
```

### Use in Connection
```sql
CREATE CONNECTION azure_sql_connection
TYPE sqlserver
OPTIONS (
  host 'server.database.windows.net',
  port '1433',
  user 'username',
  password secret('azure-sql-secrets', 'password')  -- ✅ Secure
);
```

---

## 9. Common Patterns

### Pattern 1: Dimension Enrichment
```sql
-- Enrich federated transactions with managed reference data
SELECT
  t.transaction_id,
  t.customer_id,
  t.amount,
  c.country,
  c.region
FROM federated_db.transactions t
JOIN main.reference.countries c
  ON t.country_code = c.code;
```

### Pattern 2: Hybrid Aggregation
```sql
-- Aggregate across federated and managed sources
WITH federated_sales AS (
  SELECT product_id, SUM(quantity) AS total_qty
  FROM external_db.sales
  GROUP BY product_id
),
managed_products AS (
  SELECT product_id, category, price
  FROM main.catalog.products
)
SELECT
  p.category,
  SUM(s.total_qty * p.price) AS revenue
FROM federated_sales s
JOIN managed_products p ON s.product_id = p.product_id
GROUP BY p.category;
```

### Pattern 3: Real-Time + Historical
```sql
-- Combine real-time (federated) with historical (managed)
SELECT
  'Real-Time' AS source,
  COUNT(*) AS order_count,
  SUM(total_amount) AS revenue
FROM operational_db.orders
WHERE order_date = CURRENT_DATE()

UNION ALL

SELECT
  'Historical' AS source,
  COUNT(*) AS order_count,
  SUM(total_amount) AS revenue
FROM lakehouse.orders_history
WHERE order_date < CURRENT_DATE();
```

---

## 10. Troubleshooting

### Connection Test
```python
# Test connection with simple query
spark.sql("""
  SELECT 1 AS test
  FROM azure_sql_federated.dbo.customer_holdings
  LIMIT 1
""").show()
```

### Check Network Connectivity
```bash
# From Databricks cluster
%sh
nc -zv <server>.database.windows.net 1433
```

### View Detailed Errors
```python
import traceback

try:
    df = spark.table("azure_sql_federated.dbo.customer_holdings")
    df.show()
except Exception as e:
    print(traceback.format_exc())
```

### Common Issues

| Issue | Solution |
|-------|----------|
| Connection timeout | Check firewall rules, add Databricks IPs |
| Authentication failed | Verify credentials in secrets |
| Table not found | Check schema name (often `dbo` for SQL Server) |
| Slow queries | Add indexes on join columns in source DB |
| Permission denied | Grant SELECT on tables in source database |

---

## 11. Key Messages for Blog

✅ **No Data Movement** - Data stays in original location
✅ **Unified Analytics** - Single interface for all sources
✅ **Real-Time Access** - No ETL lag
✅ **Security** - Keep data in compliant environments
✅ **Cost Efficient** - No duplication or transfer costs
✅ **Hybrid Architecture** - Support on-prem + cloud

### For Regulated Industries
- **Financial Services**: Customer accounts, PII, transactions
- **Healthcare**: Patient records, PHI, clinical data
- **Government**: Sensitive, classified, or regulated data
- **Retail**: PCI-compliant payment data

---

## 12. Demo Script (5 min)

```sql
-- 1. Show connection (10 sec)
SHOW CONNECTIONS;

-- 2. Query federated data (30 sec)
SELECT * FROM azure_sql_federated.dbo.customer_holdings LIMIT 10;

-- 3. Query managed data (30 sec)
SELECT * FROM factset.ff_basic.annual_financials LIMIT 10;

-- 4. Join both sources (90 sec)
SELECT
  h.customer_id,
  h.ticker_symbol,
  h.shares_held * h.cost_basis AS investment,
  f.revenue,
  f.net_income
FROM azure_sql_federated.dbo.customer_holdings h
JOIN factset.ff_basic.annual_financials f
  ON h.ticker_symbol = f.ticker
WHERE f.fiscal_year = 2023
ORDER BY investment DESC
LIMIT 20;

-- 5. Advanced analytics (90 sec)
SELECT
  customer_id,
  COUNT(*) AS positions,
  SUM(shares_held * cost_basis) AS portfolio_value,
  SUM(CASE WHEN net_income < 0 THEN 1 ELSE 0 END) AS risk_count
FROM (previous query)
GROUP BY customer_id;

-- 6. Show it's federated (30 sec)
EXPLAIN SELECT * FROM azure_sql_federated.dbo.customer_holdings;
-- Point out "Scan JDBCRelation" in plan
```

**Total:** ~5 minutes, demonstrates core value prop

---

## Resources

- [Lakehouse Federation Docs](https://docs.databricks.com/query-federation/)
- [Supported Data Sources](https://docs.databricks.com/query-federation/index.html#supported-data-sources)
- [Best Practices](https://docs.databricks.com/query-federation/best-practices.html)
