# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh Genie Room
# MAGIC
# MAGIC Genie spaces answer natural-language questions by querying the tables and
# MAGIC metric views attached to them. The space is compute-on-read, so nothing
# MAGIC needs materialized refresh — this task
# MAGIC
# MAGIC 1. Confirms the Genie space `Personal Investment Portfolio Assistant` is
# MAGIC    discoverable via the Workspace API.
# MAGIC 2. Runs queries against its grounding context (the `portfolio_metrics`
# MAGIC    metric view and the `my_portfolio_dashboard` base view) to confirm
# MAGIC    Genie will see fresh results on the next question.

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient

GENIE_SPACE_NAME = "Personal Investment Portfolio Assistant"

w = WorkspaceClient()

# List Genie spaces via REST — the SDK client does not yet expose list_spaces.
resp = w.api_client.do("GET", "/api/2.0/genie/spaces")
spaces = resp.get("spaces", []) if isinstance(resp, dict) else []

match = next((s for s in spaces if s.get("title") == GENIE_SPACE_NAME), None)

if match is None:
    print(f"No Genie space titled '{GENIE_SPACE_NAME}' is attached to this workspace.")
    print("Run create_metrics_and_genie to provision it (optional — Genie is a UX layer on top of the same views).")
else:
    space_id = match.get("space_id") or match.get("id")
    print(f"Genie space is live")
    print(f"  id:    {space_id}")
    print(f"  title: {match.get('title')}")
    print(f"  url:   {w.config.host}/genie/rooms/{space_id}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm Genie's grounding context (metric view + base view) is queryable.
# MAGIC -- Same views Genie will hit on the next user question.
# MAGIC SELECT 'portfolio_metrics'      AS grounding_object, MEASURE(`Total Holdings`) AS total_holdings
# MAGIC FROM mp_catalog.analytics.portfolio_metrics
# MAGIC UNION ALL
# MAGIC SELECT 'my_portfolio_dashboard',                     COUNT(*)
# MAGIC FROM mp_catalog.analytics.my_portfolio_dashboard
