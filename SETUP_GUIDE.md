# Setup Guide — Lakehouse Federation + FactSet Demo

A single end-to-end runbook. Follow the parts in order; each part is self-contained.

---

## What you'll end up with

- An on-prem SQL Server table `dbo.equity_holdings` holding ~200 sample portfolio positions.
- A Databricks **foreign catalog** `mp_portfolio_federated` that queries that on-prem table live (no data copied).
- The **FactSet** marketplace catalog `mp_factset_data` installed in your workspace.
- A Databricks **Workflow** that joins the two and builds views.
- A **UC Metric View**, an **AI/BI dashboard**, and a **Genie space** that all read those views.

---

## Prerequisites

- Databricks workspace with Unity Catalog enabled.
- Permissions in that workspace to create connections, catalogs, jobs, dashboards, and secret scopes.
- A Microsoft SQL Server instance reachable from your Databricks workspace (allow Databricks workspace IPs in the firewall).
- A SQL Server user with `SELECT` permission and a password you can store in a secret.
- Either a SQL warehouse you'll use for the workflow, or willingness to use a job cluster.

---

## Part 1 — Set up the on-prem SQL Server source

### 1.1 Generate the sample data

Run `generate_sample_holdings.py` (in Databricks or any local Python 3 environment — no extra packages needed).

```bash
python3 generate_sample_holdings.py
```

It outputs two things:

1. `~/equity_holdings.csv` — 200 rows of synthetic equity positions using real US tickers. The schema is `(symbol, instrument_type, number_of_shares)`.
2. A `CREATE TABLE dbo.equity_holdings (...)` DDL block printed to stdout.

### 1.2 Create the table and load the CSV in SQL Server

In your on-prem SQL Server (using SSMS, Azure Data Studio, `sqlcmd`, or any client):

1. Run the DDL that was printed in 1.1 to create `dbo.equity_holdings`.
2. Bulk-load `equity_holdings.csv` into the table. Use whatever tool fits your environment — `BULK INSERT`, `bcp`, SSMS Import Wizard, Azure Data Studio import, `pandas + pyodbc`, etc. The CSV has a header row and three columns in the order matching the table.

Verify with a quick `SELECT TOP 5 * FROM dbo.equity_holdings;` — you should see 5 sample rows.

---

## Part 2 — Get FactSet from the Databricks Marketplace

FactSet data is what we'll join the federated holdings with. You only need to do this once per workspace.

### 2.1 What's in FactSet (so you know why the join looks the way it does)

FactSet identifies securities by `fsym_id`, not ticker symbols. To go from a ticker to a financial statement you walk three tables:

| Schema | Table | What's in it |
|---|---|---|
| `sym_v1` | `sym_ticker_region` | Maps `ticker_region` (e.g. `MSFT-US`) → `fsym_id` |
| `ff_v3` | `ff_sec_map` | Maps `fsym_id` → `fsym_company_id` (security → company) |
| `ff_v3` | `ff_basic_af` | Annual fundamentals (revenue, net income, EPS, assets, debt, …) keyed by `fsym_company_id` |
| `fe_v4` | `fe_sec_map` | Same security → company map, for the estimates schema |
| `fe_v4` | `fe_basic_conh_af` | Consensus analyst estimates (forward EPS, analyst count, …) |

So the join chain is:
```
holdings.ticker → sym_ticker_region.fsym_id → ff_sec_map.fsym_company_id → ff_basic_af.<financials>
holdings.ticker → sym_ticker_region.fsym_id → fe_sec_map.fsym_company_id → fe_basic_conh_af.<estimates>
```

The `join_portfolio_with_factset` task in `jobs/` does exactly this and materializes it as the view `mp_catalog.analytics.my_portfolio_dashboard`.

### 2.2 Install FactSet from the Marketplace

1. In your Databricks workspace, open **Marketplace** from the left nav.
2. Search for **FactSet** (or the specific FactSet product your org licenses — e.g. *FactSet Fundamentals & Estimates*).
3. Click **Get instant access** (or **Request access** if your org's licensing requires it).
4. When prompted for a catalog name, name it `mp_factset_data`. If you choose a different name, you'll need to update it in `jobs/read_factset_from_marketplace`, `jobs/join_portfolio_with_factset`, and `create_metrics_and_genie`.

Sanity-check it:
```sql
SELECT * FROM mp_factset_data.sym_v1.sym_ticker_region WHERE ticker_region = 'MSFT-US';
```
You should get a row back with an `fsym_id`.

---

## Part 3 — Federate the SQL Server table into Databricks

### 3.1 Store the SQL Server password as a secret

```bash
databricks secrets create-scope onprem-secrets
databricks secrets put-secret --scope onprem-secrets --key sql-password
# (paste the password when prompted)
```

Or use the workspace UI: **Settings → Secrets → New scope**.

### 3.2 Create the federation CONNECTION

In a Databricks SQL editor or notebook:

```sql
CREATE CONNECTION onprem_sql_connection
  TYPE sqlserver
  OPTIONS (
    host '<your-sql-server-host>',     -- e.g. 'sqlserver.example.com'
    port '1433',
    user '<sql-username>',
    password secret('onprem-secrets', 'sql-password')
  );
```

### 3.3 Create the FOREIGN CATALOG

```sql
CREATE FOREIGN CATALOG mp_portfolio_federated
  USING CONNECTION onprem_sql_connection
  OPTIONS (database '<your-database-name>');
```

If you choose a different catalog name, update it in the same three places noted in 2.2.

### 3.4 Verify

```sql
SELECT COUNT(*) FROM mp_portfolio_federated.dbo.equity_holdings;
```

You should see ~200 — the data is queried live from your SQL Server, not stored in Databricks.

---

## Part 4 — Deploy the demo to your workspace

Clone the GitHub repo (`MeghanaPadmanabhan1/federation`, branch `final_code`) into the workspace, or upload each notebook manually:

- `generate_sample_holdings.py`
- `create_metrics_and_genie.py`
- `portfolio_dashboard.lvdash.json`
- `jobs/read_factset_from_marketplace.py`
- `jobs/read_portfolio_from_onprem_sql_server.py`
- `jobs/join_portfolio_with_factset.py`
- `jobs/build_dashboard_views.py`
- `jobs/refresh_metric_view.py`
- `jobs/refresh_aibi_dashboard.py`
- `jobs/refresh_genie_room.py`

Easiest path: **Workspace → Create → Git folder**, point at this repo's `final_code` branch.

---

## Part 5 — Create the Metric View + Genie space (one-time)

Open `create_metrics_and_genie.py` and run it once.

What it does:

1. Creates `mp_catalog.analytics.portfolio_metrics` — a Unity Catalog **Metric View** that defines governed measures (`Current Annual Earnings`, `Projected Annual Earnings`, `High Risk Count`, etc.) and dimensions (`Risk Category`, `Recommendation`, `Stock Symbol`).
2. Creates a **Genie space** titled *Personal Investment Portfolio Assistant* attached to that metric view and to the base portfolio view.

This step depends on the views existing, so run Part 6 once first **or** know that the first run of this notebook may fail at the Genie-space step until the views are built. You can re-run it safely.

---

## Part 6 — Create and run the Workflow

### 6.1 Create the Workflow

In **Workflows → Create job**, configure seven notebook tasks pointing at the `jobs/` notebooks. Use a SQL warehouse for the five SQL-only tasks and serverless compute for the two Python tasks. Set up dependencies so the DAG matches:

```
read_factset_from_marketplace ──┐
                                ├─► join_portfolio_with_factset ─┬─► build_dashboard_views ─► refresh_aibi_dashboard
read_portfolio_from_onprem ─────┘                                └─► refresh_metric_view ───► refresh_genie_room
```

| Task | Notebook | Compute |
|---|---|---|
| `read_factset_from_marketplace` | `jobs/read_factset_from_marketplace` | SQL warehouse |
| `read_portfolio_from_onprem_sql_server` | `jobs/read_portfolio_from_onprem_sql_server` | SQL warehouse |
| `join_portfolio_with_factset` | `jobs/join_portfolio_with_factset` | SQL warehouse |
| `build_dashboard_views` | `jobs/build_dashboard_views` | SQL warehouse |
| `refresh_metric_view` | `jobs/refresh_metric_view` | SQL warehouse |
| `refresh_aibi_dashboard` | `jobs/refresh_aibi_dashboard` | Serverless (uses Databricks SDK) |
| `refresh_genie_room` | `jobs/refresh_genie_room` | Serverless (uses Databricks SDK) |

In `jobs/refresh_aibi_dashboard`, update `DASHBOARD_ID` and `WAREHOUSE_ID` to match your dashboard / warehouse.

### 6.2 Run the workflow

Click **Run now**. The seven tasks execute in dependency order. On success:

- The base view `mp_catalog.analytics.my_portfolio_dashboard` is updated.
- Seven derived views are created/refreshed: `my_portfolio_summary`, `my_stock_rankings`, `my_action_items`, `aibi_portfolio_overview`, `aibi_risk_distribution`, `aibi_action_distribution`, `aibi_stock_performance`.
- The AI/BI dashboard's cached snapshot is republished.
- Genie's grounding views are validated.

Every object created here is a regular `VIEW` — **no federated on-prem data is materialized into cloud storage**. Each query re-evaluates against SQL Server live.

---

## Part 7 — Set up the AI/BI Dashboard

The dashboard definition lives in `portfolio_dashboard.lvdash.json`. To deploy it:

1. In Databricks, open **Dashboards → New dashboard → Import**.
2. Upload `portfolio_dashboard.lvdash.json`.
3. Set the warehouse the dashboard should use for its queries.
4. **Publish** the dashboard.
5. Note the **dashboard ID** in the URL (`/dashboardsv3/<id>`) and paste it into `jobs/refresh_aibi_dashboard` so future workflow runs can republish it automatically.

The dashboard's widgets all query views in `mp_catalog.analytics.*` — nothing dashboard-side needs to change as long as those views exist.

---

## Part 8 — Verify everything works

Run these in a SQL editor:

```sql
-- All eight analytics objects should be VIEW (plus portfolio_metrics is METRIC_VIEW)
SELECT table_name, table_type
FROM system.information_schema.tables
WHERE table_catalog = 'mp_catalog' AND table_schema = 'analytics'
ORDER BY table_name;

-- Top-line dashboard numbers
SELECT total_holdings, total_current_earnings, total_projected_earnings
FROM mp_catalog.analytics.my_portfolio_summary;

-- Confirm federation is live (this query talks to your SQL Server, not Delta)
SELECT COUNT(*) FROM mp_portfolio_federated.dbo.equity_holdings;
```

Then open the dashboard and ask Genie a question like *"How is my portfolio projected to perform?"*

---

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `Cannot resolve mp_portfolio_federated.dbo.equity_holdings` | Foreign catalog not created, or wrong database name in Part 3.3. |
| Workflow's first task fails on FactSet tables | `mp_factset_data` catalog not installed, or named differently. |
| Workflow succeeds but the dashboard still shows old numbers | Click the refresh icon in the dashboard toolbar, or rerun `refresh_aibi_dashboard`. |
| `NO_TABLES_IN_PIPELINE` | You converted the workflow into an SDP pipeline. SDP pipelines require ≥1 `@dlt.table()`. Keep this as a job, not a pipeline. |
| Federation queries are slow | Add an index on `symbol` and `instrument_type` in your SQL Server table; enable predicate pushdown by avoiding `SELECT *`. |
| Total earnings look wildly negative | Some FactSet rows have malformed `FF_EPS_BASIC` for micro-caps. The view in `jobs/join_portfolio_with_factset` already filters `ABS(FF_EPS_BASIC) < 50` and `FF_SALES > 0`. Loosen/tighten as needed. |
