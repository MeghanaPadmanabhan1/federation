# Lakehouse Federation Setup Guide

## Overview
This guide will help you set up Lakehouse Federation between Azure SQL Database and Databricks to demonstrate querying data without moving it.

## Prerequisites

1. **Azure SQL Database** with sample data (customer holdings, security positions, etc.)
2. **Databricks Workspace** on Azure (E2 or higher for Unity Catalog)
3. **FactSet Data** from Databricks Marketplace (or Delta Shared catalog)
4. **Permissions**: Account admin or sufficient privileges to create connections and catalogs

## Step 1: Prepare Azure SQL Database

### Create Sample Tables

```sql
-- Create a customer holdings table in Azure SQL
CREATE TABLE customer_holdings (
    customer_id INT,
    ticker_symbol VARCHAR(10),
    shares_held DECIMAL(18,2),
    purchase_date DATE,
    cost_basis DECIMAL(18,2),
    account_type VARCHAR(50)
);

-- Insert sample data
INSERT INTO customer_holdings VALUES
(1001, 'AAPL', 100.00, '2023-01-15', 150.25, 'Brokerage'),
(1001, 'MSFT', 50.00, '2023-02-20', 280.50, 'IRA'),
(1002, 'GOOGL', 75.00, '2023-03-10', 105.75, 'Brokerage'),
(1002, 'TSLA', 25.00, '2023-04-05', 220.00, '401k'),
(1003, 'AMZN', 60.00, '2023-05-12', 95.50, 'Brokerage');
```

### Note Your Connection Details
- Server: `<your-server>.database.windows.net`
- Database: `<your-database-name>`
- Username: `<your-username>`
- Password: (store in Databricks secrets)

## Step 2: Configure Databricks Secrets

```python
# In Databricks, create a secret scope and add your Azure SQL password
# Use Databricks CLI:

databricks secrets create-scope --scope azure-sql-secrets

databricks secrets put-secret --scope azure-sql-secrets \
  --key azure-sql-password \
  --string-value "<your-password>"
```

Or use the Databricks Secrets UI:
1. Go to `#secrets/createScope` in your workspace URL
2. Create scope named `azure-sql-secrets`
3. Add secret with key `azure-sql-password`

## Step 3: Set Up FactSet Data

### Option A: Use Marketplace Data (Recommended)
1. Go to Databricks Marketplace
2. Search for "FactSet"
3. Install FactSet Fundamentals (FF) and/or FactSet Estimates (FE)
4. Note the catalog name (e.g., `factset_catalog`)

### Option B: Delta Share from Another Workspace
If you need to copy data from E2 workspace to Azure workspace:

```python
# In E2 workspace - create a share
CREATE SHARE factset_share;

ALTER SHARE factset_share ADD TABLE factset_catalog.ff_basic.annual_financials;

# Create recipient
CREATE RECIPIENT azure_workspace_recipient
USING ID '<azure-workspace-metastore-id>';

GRANT SELECT ON SHARE factset_share TO RECIPIENT azure_workspace_recipient;

# In Azure workspace - create catalog from share
CREATE CATALOG factset_shared
USING SHARE <e2-metastore-id>.factset_share;
```

## Step 4: Network Configuration

Ensure Databricks can connect to Azure SQL:

1. **Firewall Rules**: Add Databricks workspace IPs to Azure SQL firewall
2. **Private Link** (Optional): For enhanced security, set up Azure Private Link
3. **Service Endpoints**: Configure VNet service endpoints if using VNet injection

## Step 5: Run the Demo Notebook

1. Upload `lakehouse_federation_demo.py` to your Databricks workspace
2. Update the connection parameters:
   - Azure SQL host, database, username
   - Secret scope name
   - FactSet catalog and schema names
3. Run the notebook cells in order

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│                    Databricks Workspace                 │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │         Lakehouse Federation Layer              │    │
│  │                                                  │    │
│  │  ┌──────────────────┐  ┌──────────────────┐   │    │
│  │  │ Foreign Catalog  │  │ Managed Catalog  │   │    │
│  │  │  (Azure SQL)     │  │   (FactSet)      │   │    │
│  │  └────────┬─────────┘  └────────┬─────────┘   │    │
│  │           │                     │              │    │
│  │           └──────────┬──────────┘              │    │
│  │                      │                          │    │
│  │              ┌───────▼────────┐                │    │
│  │              │  Unified Query │                │    │
│  │              │    Interface   │                │    │
│  │              └────────────────┘                │    │
│  └────────────────────────────────────────────────┘    │
└──────────────────────┬──────────────────────────────────┘
                       │
         ┌─────────────┴──────────────┐
         │                            │
         ▼                            ▼
┌─────────────────┐          ┌──────────────────┐
│  Azure SQL DB   │          │ Delta Lake       │
│  (Federated)    │          │ (Managed)        │
│                 │          │                  │
│ - Customer Data │          │ - FactSet Data   │
│ - Holdings      │          │ - Market Data    │
│ - Positions     │          │ - Reference Data │
└─────────────────┘          └──────────────────┘
   (Data stays in             (Data in Databricks
    original location)         managed storage)
```

## Key Benefits for Regulated Industries

1. **Data Sovereignty**: Keep regulated data in approved locations
2. **Compliance**: Maintain existing security controls and audit trails
3. **Cost Efficiency**: Avoid data duplication and transfer costs
4. **Flexibility**: Support hybrid architectures (on-prem + cloud)
5. **Real-time**: Query live data without ETL delays
6. **Unified Analytics**: Single interface for all data sources

## Use Cases

### Financial Services
- Combine customer holdings (Azure SQL) with market data (Databricks)
- Portfolio analysis without moving PII data
- Risk assessment across federated and managed sources

### Healthcare
- Patient records (HIPAA-compliant DB) + research data (Databricks)
- Clinical trials analysis
- Population health analytics

### Retail
- Transactional data (operational DB) + customer behavior (Databricks)
- Inventory optimization
- Personalization engines

## Troubleshooting

### Connection Issues
```python
# Test connection
%sql
SHOW CONNECTIONS;

# Check connection status
%sql
DESCRIBE CONNECTION azure_sql_connection;
```

### Permission Errors
```sql
-- Grant necessary privileges
GRANT USAGE ON CONNECTION azure_sql_connection TO `user@example.com`;
GRANT USE CATALOG ON CATALOG azure_sql_federated TO `user@example.com`;
```

### Performance Optimization
1. Add indexes on join columns in Azure SQL
2. Use predicate pushdown (filter before joining)
3. Cache frequently accessed federated data
4. Monitor query plans with `EXPLAIN`

## Next Steps

1. Review the demo notebook: `lakehouse_federation_demo.py`
2. Customize with your actual table names and schemas
3. Create dashboards using the combined data
4. Set up monitoring and alerting
5. Document your architecture for compliance teams

## Resources

- [Databricks Lakehouse Federation Documentation](https://docs.databricks.com/en/query-federation/index.html)
- [Unity Catalog Foreign Catalogs](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [Azure SQL Database Documentation](https://learn.microsoft.com/en-us/azure/azure-sql/)
- [FactSet Data on Databricks Marketplace](https://marketplace.databricks.com/)
