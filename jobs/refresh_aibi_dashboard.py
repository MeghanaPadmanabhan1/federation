# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh AI/BI Dashboard
# MAGIC
# MAGIC Republishes the `portfolio_dashboard` AI/BI dashboard so its cached query
# MAGIC results reflect the freshly-built views. Without this step, viewers see a
# MAGIC stale snapshot until they click the refresh icon in the dashboard UI.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

DASHBOARD_ID = "01f10e203541134391ad9bbbcd9d52d6"
WAREHOUSE_ID = "862f1d757f0424f7"

w = WorkspaceClient()

published = w.lakeview.publish(
    dashboard_id=DASHBOARD_ID,
    embed_credentials=True,
    warehouse_id=WAREHOUSE_ID,
)

print(f"Dashboard republished")
print(f"  dashboard_id: {DASHBOARD_ID}")
print(f"  display_name: {published.display_name}")
print(f"  revision:     {published.revision_create_time}")
print(f"  url:          {w.config.host}/dashboardsv3/{DASHBOARD_ID}/published")
