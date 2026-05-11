# Lakehouse Federation + FactSet Demo

Combine **on-premise portfolio holdings** with **FactSet financial data** from the Databricks Marketplace — without moving any portfolio data to the cloud. The on-prem holdings stay in SQL Server; Databricks queries them live via Lakehouse Federation and joins them with FactSet fundamentals + estimates to produce investment analytics, an AI/BI dashboard, and a Genie space.

## What's in this repo

| File | Role |
|---|---|
| `blog.ipynb` | The blog post |
| `generate_sample_holdings.py` | Generates `~/equity_holdings.csv` + the SQL Server DDL you'll need |
| `create_metrics_and_genie.py` | One-time setup: creates the UC Metric View `portfolio_metrics` and the Genie space |
| `portfolio_dashboard.lvdash.json` | The AI/BI dashboard definition |
| `jobs/` | Seven notebook tasks that make up the operational workflow |

## Prerequisites

- A Databricks workspace with Unity Catalog enabled
- The **FactSet** data product installed from Databricks Marketplace, exposed as the `mp_factset_data` catalog (or update the catalog name in `jobs/join_portfolio_with_factset` and `create_metrics_and_genie`)
- A Microsoft SQL Server instance reachable from your Databricks workspace
- A SQL warehouse to run the workflow (the workflow currently uses `862f1d757f0424f7` — update if needed)

## How to run

### 1. Generate sample portfolio data
Run `generate_sample_holdings.py` (in Databricks or local Python — no dependencies). It writes `~/equity_holdings.csv` (200 rows, real US tickers) and prints the SQL Server `CREATE TABLE dbo.equity_holdings` DDL.

### 2. Load the CSV into your on-prem SQL Server
Run the DDL printed in Step 1 to create `dbo.equity_holdings`, then bulk-load the CSV into it (use `BULK INSERT`, `bcp`, the SSMS Import Wizard, or whatever fits your environment).

### 3. Federate the SQL Server table into Databricks
In Databricks, create a SQL Server connection and a foreign catalog so the on-prem table appears as `mp_portfolio_federated.dbo.equity_holdings`:
```sql
CREATE CONNECTION onprem_sql_connection
  TYPE sqlserver
  OPTIONS (
    host '<your-sql-server-host>',
    port '1433',
    user '<username>',
    password secret('<scope>', '<key>')
  );

CREATE FOREIGN CATALOG mp_portfolio_federated
  USING CONNECTION onprem_sql_connection
  OPTIONS (database '<your-database>');
```
Store the SQL Server password in a Databricks secret scope first.

### 4. Run `create_metrics_and_genie` (one-time)
Open the notebook and run it once. It creates the `mp_catalog.analytics.portfolio_metrics` Metric View and the Genie space *Personal Investment Portfolio Assistant*.

### 5. Run the workflow
Trigger the **`Portfolio Federation Pipeline`** job (job ID `796814768384996`). The seven `jobs/*` tasks execute in this order:
```
read_factset_from_marketplace ──┐
                                ├─► join_portfolio_with_factset ─┬─► build_dashboard_views ─► refresh_aibi_dashboard
read_portfolio_from_onprem ─────┘                                └─► refresh_metric_view ───► refresh_genie_room
```
On success the workflow creates eight views under `mp_catalog.analytics.*` (one base join + seven derived dashboard views) and republishes the dashboard.

### 6. View the result
- **Dashboard** — Workspace → Dashboards → *portfolio_dashboard*
- **Genie** — Workspace → Genie → *Personal Investment Portfolio Assistant*

## Notes

- **No federated data is materialized.** Every `mp_catalog.analytics.*` object is a plain `VIEW`, so on-prem holdings are queried live on each dashboard load or pipeline run.
- The workflow is idempotent; rerunning it is safe.
- If your portfolio's totals look skewed by a few extreme rows, see the EPS sanity filters in `jobs/join_portfolio_with_factset` (`ABS(FF_EPS_BASIC) < 50`, `FF_SALES > 0`).
