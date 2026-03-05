# Databricks Insights: Workspace Observability Platform

## Complete End-to-End Implementation Guide

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Prerequisites & Workspace Setup](#2-prerequisites--workspace-setup)
3. [Phase 1 — Data Foundation (System Tables & Observability Schema)](#3-phase-1--data-foundation)
4. [Phase 2 — Lakeflow Declarative Pipeline for Data Ingestion](#4-phase-2--lakeflow-declarative-pipeline)
5. [Phase 3 — SQL Views & Materialized Aggregations](#5-phase-3--sql-views--materialized-aggregations)
6. [Phase 4 — Databricks App (Dash Frontend)](#6-phase-4--databricks-app)
7. [Phase 5 — Alerts & Automated Actions](#7-phase-5--alerts--automated-actions)
8. [Phase 6 — AI Layer (Natural Language Querying)](#8-phase-6--ai-layer)
9. [Deployment with Databricks Asset Bundles](#9-deployment-with-databricks-asset-bundles)
10. [Operational Runbook](#10-operational-runbook)

---

## 1. Architecture Overview

The Databricks Insights platform is built on four pillars:

**Data Layer** — System tables (`system.billing`, `system.access`, `system.compute`, `system.lakeflow`, `system.query`) feed into a curated observability schema via a Lakeflow Declarative Pipeline.

**Compute Layer** — A dedicated serverless SQL warehouse powers all queries from the Databricks App and the AI layer.

**App Layer** — A Databricks App (Python/Dash) serves the admin dashboard with SSO-integrated authentication, deployed on serverless compute.

**AI Layer** — A Foundation Model API endpoint (or external model via AI Gateway) enables natural language querying over the observability data using AI Functions.

```
┌─────────────────────────────────────────────────────┐
│                  Databricks App (Dash)               │
│   ┌──────────┬──────────┬──────────┬──────────┐     │
│   │  Cost &  │  Jobs &  │ Govern-  │  AI Chat │     │
│   │ Compute  │Pipelines │  ance    │ Interface│     │
│   └────┬─────┴────┬─────┴────┬─────┴────┬─────┘     │
│        │          │          │          │            │
│        └──────────┴──────┬───┴──────────┘            │
│                          │                           │
│              ┌───────────▼──────────────┐            │
│              │  Serverless SQL Warehouse │            │
│              └───────────┬──────────────┘            │
│                          │                           │
│         ┌────────────────▼──────────────────┐        │
│         │  databricks_insights (Unity Catalog Schema)   │        │
│         │  - gold_cost_daily                 │        │
│         │  - gold_job_health                 │        │
│         │  - gold_compute_inventory          │        │
│         │  - gold_governance_posture         │        │
│         │  - gold_user_activity              │        │
│         └────────────────┬──────────────────┘        │
│                          │                           │
│         ┌────────────────▼──────────────────┐        │
│         │  Lakeflow Declarative Pipeline     │        │
│         └────────────────┬──────────────────┘        │
│                          │                           │
│   ┌──────────┬───────────┴──────┬──────────────┐     │
│   │ system.  │ system.access.   │ system.      │     │
│   │ billing  │ audit            │ compute      │     │
│   │ .usage   │                  │ .clusters    │     │
│   ├──────────┼──────────────────┼──────────────┤     │
│   │ system.  │ system.query.    │ system.      │     │
│   │ billing  │ history          │ lakeflow.*   │     │
│   │.list_    │                  │              │     │
│   │ prices   │                  │              │     │
│   └──────────┴──────────────────┴──────────────┘     │
└─────────────────────────────────────────────────────┘
```

---

## 2. Prerequisites & Workspace Setup

### 2.1 Enable System Tables

System tables require Unity Catalog. An account admin must enable them:

1. Navigate to **Account Console → Settings → Feature enablement**
2. Ensure **System Tables** is toggled on
3. Grant access to the service principal or admin group that will query them:

```sql
-- Run as a metastore admin
GRANT USE SCHEMA ON SCHEMA system.billing TO `databricks-insights-admins`;
GRANT SELECT ON SCHEMA system.billing TO `databricks-insights-admins`;

GRANT USE SCHEMA ON SCHEMA system.access TO `databricks-insights-admins`;
GRANT SELECT ON SCHEMA system.access TO `databricks-insights-admins`;

GRANT USE SCHEMA ON SCHEMA system.compute TO `databricks-insights-admins`;
GRANT SELECT ON SCHEMA system.compute TO `databricks-insights-admins`;

GRANT USE SCHEMA ON SCHEMA system.lakeflow TO `databricks-insights-admins`;
GRANT SELECT ON SCHEMA system.lakeflow TO `databricks-insights-admins`;

GRANT USE SCHEMA ON SCHEMA system.query TO `databricks-insights-admins`;
GRANT SELECT ON SCHEMA system.query TO `databricks-insights-admins`;
```

### 2.2 Enable Databricks Apps

A workspace admin must enable Databricks Apps:

1. Navigate to **Workspace Settings → Previews**
2. Enable **Databricks Apps**

### 2.3 Enable Verbose Audit Logs (Optional but Recommended)

Verbose audit logs capture notebook command execution details:

1. Navigate to **Workspace Settings → Advanced**
2. Enable **Verbose Audit Logs**

This populates the `runCommand` action in `system.access.audit`, giving you per-command observability.

### 2.4 Create the Observability Catalog and Schema

```sql
-- Create a dedicated catalog (or use an existing one)
CREATE CATALOG IF NOT EXISTS observability;

-- Create the databricks_insights schema
CREATE SCHEMA IF NOT EXISTS observability.databricks_insights
  COMMENT 'Databricks Insights workspace observability platform — curated tables for admin dashboards';

-- Grant the app's service principal access
GRANT USE CATALOG ON CATALOG observability TO `databricks-insights-app-sp`;
GRANT USE SCHEMA ON SCHEMA observability.databricks_insights TO `databricks-insights-app-sp`;
GRANT SELECT ON SCHEMA observability.databricks_insights TO `databricks-insights-app-sp`;
```

### 2.5 Create a Serverless SQL Warehouse

```sql
-- Via the UI: Compute → SQL Warehouses → Create
-- Recommended settings:
--   Name: databricks-insights-warehouse
--   Type: Serverless
--   Size: Small (scales automatically)
--   Auto-stop: 10 minutes
--   Channel: Current
```

Alternatively, use the REST API:

```bash
curl -X POST "https://<workspace-url>/api/2.0/sql/warehouses" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "databricks-insights-warehouse",
    "cluster_size": "2X-Small",
    "auto_stop_mins": 10,
    "warehouse_type": "PRO",
    "enable_serverless_compute": true
  }'
```

---

## 3. Phase 1 — Data Foundation

This section establishes the system tables you'll query and introduces the key schemas.

### 3.1 System Tables Reference

Below is the complete inventory of system tables used by Databricks Insights:

| System Table | Schema | Purpose |
|---|---|---|
| `system.billing.usage` | Billing | All billable usage records with DBU quantities, SKUs, identities, and metadata |
| `system.billing.list_prices` | Billing | Historical SKU pricing — join with usage to calculate dollar costs |
| `system.access.audit` | Access | Every audited action — logins, permission changes, data access, notebook runs |
| `system.compute.clusters` | Compute | SCD2 table of all cluster configurations, owners, and state changes |
| `system.compute.warehouse_events` | Compute | SQL warehouse scaling and lifecycle events |
| `system.lakeflow.jobs` | Lakeflow | SCD2 table of all job definitions |
| `system.lakeflow.job_run_timeline` | Lakeflow | Timeline of job runs with start/end times and result states |
| `system.lakeflow.job_task_run_timeline` | Lakeflow | Per-task run details within jobs |
| `system.lakeflow.pipelines` | Lakeflow | SCD2 table of Lakeflow Declarative Pipeline definitions |
| `system.lakeflow.pipeline_update_timeline` | Lakeflow | Pipeline update history with outcomes and timing |
| `system.query.history` | Query | All SQL queries with execution time, rows returned, and warehouse used |

### 3.2 Verify System Table Access

Run these validation queries to confirm data is flowing:

```sql
-- Check billing data recency
SELECT MAX(usage_date) AS latest_billing_date,
       COUNT(*) AS total_records
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE - INTERVAL 7 DAYS;

-- Check audit log recency
SELECT MAX(event_date) AS latest_audit_date,
       COUNT(*) AS total_events
FROM system.access.audit
WHERE event_date >= CURRENT_DATE - INTERVAL 1 DAY;

-- Check cluster data
SELECT COUNT(*) AS cluster_records
FROM system.compute.clusters
WHERE change_time >= CURRENT_DATE - INTERVAL 30 DAYS;

-- Check job data
SELECT COUNT(*) AS job_records
FROM system.lakeflow.jobs;
```

If any return zero rows, verify the grants from Section 2.1 and confirm system tables are enabled for your account.

---

## 4. Phase 2 — Lakeflow Declarative Pipeline

Create a Lakeflow pipeline that materializes curated gold tables from raw system tables. This decouples the app from direct system table queries and enables pre-aggregated, fast reads.

### 4.1 Pipeline Notebook: `databricks_insights_pipeline`

Create a new notebook in your workspace with the following cells. This uses the latest Lakeflow Spark Declarative Pipelines Python API.

**Cell 1 — Imports and config:**

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
```

**Cell 2 — Gold: Daily Cost Summary:**

```python
@dp.materialized_view(
    comment="Daily cost summary by product, SKU, and workspace"
)
def gold_cost_daily():
    return (
        spark.read.table("system.billing.usage")
        .join(
            spark.read.table("system.billing.list_prices"),
            on=[
                F.col("usage.sku_name") == F.col("list_prices.sku_name"),
                F.col("usage.usage_end_time") >= F.col("list_prices.price_start_time"),
                (F.col("list_prices.price_end_time").isNull()) |
                (F.col("usage.usage_end_time") < F.col("list_prices.price_end_time"))
            ],
            how="left"
        )
        .groupBy(
            F.col("usage.usage_date").alias("date"),
            F.col("usage.workspace_id"),
            F.col("usage.billing_origin_product").alias("product"),
            F.col("usage.sku_name")
        )
        .agg(
            F.sum("usage.usage_quantity").alias("total_dbus"),
            F.sum(
                F.col("usage.usage_quantity") *
                F.col("list_prices.pricing.effective_list.default")
            ).alias("estimated_cost_usd"),
            F.countDistinct("usage.identity_metadata.run_as").alias("unique_users")
        )
    )
```

**Cell 3 — Gold: Job Health Summary:**

```python
@dp.materialized_view(
    comment="Job run health metrics — success rates, durations, failures"
)
def gold_job_health():
    # Get latest job metadata
    jobs = (
        spark.read.table("system.lakeflow.jobs")
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("workspace_id", "job_id")
            .orderBy(F.col("change_time").desc())
        ))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Aggregate run timeline
    runs = (
        spark.read.table("system.lakeflow.job_run_timeline")
        .filter(F.col("period_start_time") >= F.current_timestamp() - F.expr("INTERVAL 30 DAYS"))
        .groupBy("workspace_id", "job_id", "run_id")
        .agg(
            F.min("period_start_time").alias("run_start"),
            F.max("period_end_time").alias("run_end"),
            F.first("result_state", ignorenulls=True).alias("result_state")
        )
    )

    return (
        runs.groupBy("workspace_id", "job_id")
        .agg(
            F.count("*").alias("total_runs_30d"),
            F.sum(F.when(F.col("result_state") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
            F.sum(F.when(F.col("result_state") == "FAILED", 1).otherwise(0)).alias("failure_count"),
            F.avg(
                F.unix_timestamp("run_end") - F.unix_timestamp("run_start")
            ).alias("avg_duration_seconds"),
            F.percentile_approx(
                F.unix_timestamp("run_end") - F.unix_timestamp("run_start"), 0.95
            ).alias("p95_duration_seconds"),
            F.max("run_start").alias("last_run_time")
        )
        .join(jobs, on=["workspace_id", "job_id"], how="left")
        .select(
            "workspace_id", "job_id",
            F.col("name").alias("job_name"),
            "total_runs_30d", "success_count", "failure_count",
            F.round(F.col("success_count") / F.col("total_runs_30d") * 100, 2).alias("success_rate_pct"),
            F.round(F.col("avg_duration_seconds"), 0).alias("avg_duration_seconds"),
            F.round(F.col("p95_duration_seconds"), 0).alias("p95_duration_seconds"),
            "last_run_time",
            F.col("creator_user_name").alias("job_owner")
        )
    )
```

**Cell 4 — Gold: Compute Inventory (Zombie Detection):**

```python
@dp.materialized_view(
    comment="Active compute inventory with idle cluster detection"
)
def gold_compute_inventory():
    clusters = (
        spark.read.table("system.compute.clusters")
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("workspace_id", "cluster_id")
            .orderBy(F.col("change_time").desc())
        ))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Join with recent billing to find usage
    recent_usage = (
        spark.read.table("system.billing.usage")
        .filter(F.col("usage_date") >= F.current_date() - F.expr("INTERVAL 7 DAYS"))
        .filter(F.col("usage_metadata.cluster_id").isNotNull())
        .groupBy(F.col("usage_metadata.cluster_id").alias("cluster_id"))
        .agg(
            F.sum("usage_quantity").alias("dbus_last_7d"),
            F.max("usage_end_time").alias("last_active_time")
        )
    )

    return (
        clusters
        .join(recent_usage, on="cluster_id", how="left")
        .select(
            "workspace_id",
            "cluster_id",
            "cluster_name",
            F.col("owned_by").alias("owner"),
            "cluster_source",
            "driver_node_type_id",
            "node_type_id",
            "autoscale",
            "num_workers",
            F.coalesce("dbus_last_7d", F.lit(0)).alias("dbus_last_7d"),
            "last_active_time",
            F.when(
                (F.coalesce("dbus_last_7d", F.lit(0)) == 0) &
                (F.col("state") == "RUNNING"),
                F.lit("ZOMBIE")
            ).when(
                F.col("state") == "RUNNING",
                F.lit("ACTIVE")
            ).otherwise(F.lit("TERMINATED")).alias("health_status"),
            "change_time"
        )
    )
```

**Cell 5 — Gold: User Activity:**

```python
@dp.materialized_view(
    comment="User activity summary for the last 30 days"
)
def gold_user_activity():
    logins = (
        spark.read.table("system.access.audit")
        .filter(F.col("event_date") >= F.current_date() - F.expr("INTERVAL 30 DAYS"))
        .filter(F.col("action_name").isin(
            "workspaceInHouseOAuthClientAuthentication",
            "mintOAuthToken", "mintOAuthAuthorizationCode"
        ))
        .groupBy(
            F.col("workspace_id"),
            F.col("user_identity.email").alias("user_email")
        )
        .agg(
            F.count("*").alias("login_count_30d"),
            F.max("event_time").alias("last_login"),
            F.min("event_time").alias("first_login_in_period"),
            F.countDistinct(F.to_date("event_time")).alias("active_days")
        )
    )

    # Get per-user cost from billing
    user_cost = (
        spark.read.table("system.billing.usage")
        .filter(F.col("usage_date") >= F.current_date() - F.expr("INTERVAL 30 DAYS"))
        .filter(F.col("identity_metadata.run_as").isNotNull())
        .join(
            spark.read.table("system.billing.list_prices"),
            on=[
                F.col("usage.sku_name") == F.col("list_prices.sku_name"),
                F.col("usage.usage_end_time") >= F.col("list_prices.price_start_time"),
                (F.col("list_prices.price_end_time").isNull()) |
                (F.col("usage.usage_end_time") < F.col("list_prices.price_end_time"))
            ],
            how="left"
        )
        .groupBy(F.col("usage.identity_metadata.run_as").alias("user_email"))
        .agg(
            F.sum("usage.usage_quantity").alias("total_dbus_30d"),
            F.sum(
                F.col("usage.usage_quantity") *
                F.col("list_prices.pricing.effective_list.default")
            ).alias("estimated_cost_30d")
        )
    )

    return logins.join(user_cost, on="user_email", how="full_outer")
```

**Cell 6 — Gold: Governance Posture Score:**

```python
@dp.materialized_view(
    comment="Governance posture indicators and score"
)
def gold_governance_posture():
    # Permission changes in last 24h
    perm_changes = (
        spark.read.table("system.access.audit")
        .filter(F.col("event_date") >= F.current_date() - F.expr("INTERVAL 1 DAY"))
        .filter(
            (F.col("service_name") == "unityCatalog") &
            (F.col("action_name") == "updatePermissions")
        )
        .groupBy("workspace_id")
        .agg(F.count("*").alias("permission_changes_24h"))
    )

    # IP access denials
    ip_denials = (
        spark.read.table("system.access.audit")
        .filter(F.col("event_date") >= F.current_date() - F.expr("INTERVAL 1 DAY"))
        .filter(F.col("action_name").isin("IpAccessDenied", "accountIpAclsValidationFailed"))
        .groupBy("workspace_id")
        .agg(F.count("*").alias("ip_denials_24h"))
    )

    # Destructive operations
    destructive = (
        spark.read.table("system.access.audit")
        .filter(F.col("event_date") >= F.current_date() - F.expr("INTERVAL 1 DAY"))
        .filter(F.col("action_name").rlike("^delete"))
        .groupBy("workspace_id")
        .agg(
            F.count("*").alias("destructive_ops_24h"),
            F.countDistinct("user_identity.email").alias("users_with_destructive_ops")
        )
    )

    # Admin changes
    admin_changes = (
        spark.read.table("system.access.audit")
        .filter(F.col("event_date") >= F.current_date() - F.expr("INTERVAL 1 DAY"))
        .filter(
            (F.col("action_name").isin("addPrincipalToGroup", "removePrincipalFromGroup")) &
            (F.col("request_params.targetGroupName") == "admins")
        )
        .groupBy("workspace_id")
        .agg(F.count("*").alias("admin_group_changes_24h"))
    )

    return (
        perm_changes
        .join(ip_denials, on="workspace_id", how="full_outer")
        .join(destructive, on="workspace_id", how="full_outer")
        .join(admin_changes, on="workspace_id", how="full_outer")
        .na.fill(0)
    )
```

**Cell 7 — Gold: Pipeline Health:**

```python
@dp.materialized_view(
    comment="Lakeflow Declarative Pipeline update health"
)
def gold_pipeline_health():
    pipelines = (
        spark.read.table("system.lakeflow.pipelines")
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("workspace_id", "pipeline_id")
            .orderBy(F.col("change_time").desc())
        ))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    updates = (
        spark.read.table("system.lakeflow.pipeline_update_timeline")
        .filter(F.col("period_start_time") >= F.current_timestamp() - F.expr("INTERVAL 7 DAYS"))
        .filter(F.col("result_state").isNotNull())
        .groupBy("workspace_id", "pipeline_id")
        .agg(
            F.count(F.when(F.col("result_state") == "COMPLETED", True)).alias("success_count"),
            F.count(F.when(F.col("result_state") == "FAILED", True)).alias("failure_count"),
            F.count("*").alias("total_updates_7d")
        )
    )

    return (
        pipelines
        .join(updates, on=["workspace_id", "pipeline_id"], how="left")
        .select(
            "workspace_id", "pipeline_id", "name",
            F.coalesce("total_updates_7d", F.lit(0)).alias("total_updates_7d"),
            F.coalesce("success_count", F.lit(0)).alias("success_count"),
            F.coalesce("failure_count", F.lit(0)).alias("failure_count"),
            F.round(
                F.coalesce("success_count", F.lit(0)) /
                F.greatest(F.coalesce("total_updates_7d", F.lit(1)), F.lit(1)) * 100, 2
            ).alias("success_rate_pct")
        )
    )
```

### 4.2 Create the Pipeline

1. Navigate to **Workflows → Lakeflow Pipelines → Create Pipeline**
2. Configure:
   - **Name:** `databricks-insights-pipeline`
   - **Source code:** Point to the notebook from 4.1
   - **Target catalog:** `observability`
   - **Target schema:** `databricks_insights`
   - **Cluster mode:** Enhanced Autoscaling
   - **Channel:** Current
   - **Pipeline mode:** Triggered (we'll schedule via a Job)
3. Click **Create**
4. Run a manual update to populate the initial data

### 4.3 Schedule the Pipeline

Create a Lakeflow Job to refresh the pipeline:

1. Navigate to **Workflows → Jobs → Create Job**
2. Add a task:
   - **Task type:** Pipeline
   - **Pipeline:** `databricks-insights-pipeline`
3. Set schedule: Every 15 minutes (or hourly for lower cost)
4. Add email notifications for task failures

---

## 5. Phase 3 — SQL Views & Materialized Aggregations

In addition to the gold tables from the pipeline, create SQL views for real-time queries the app uses directly against system tables.

### 5.1 Real-Time Cost Burn Rate View

```sql
CREATE OR REPLACE VIEW observability.databricks_insights.v_realtime_burn_rate AS
WITH hourly AS (
  SELECT
    date_trunc('HOUR', usage_start_time) AS hour,
    SUM(u.usage_quantity * lp.pricing.effective_list.default) AS cost_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices lp
    ON lp.sku_name = u.sku_name
    AND u.usage_end_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
  WHERE u.usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
  GROUP BY 1
)
SELECT
  hour,
  cost_usd,
  AVG(cost_usd) OVER (ORDER BY hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS rolling_24h_avg,
  SUM(cost_usd) OVER (
    PARTITION BY DATE(hour)
    ORDER BY hour
  ) AS cumulative_daily_cost
FROM hourly
ORDER BY hour DESC;
```

### 5.2 Currently Failing Jobs View

```sql
CREATE OR REPLACE VIEW observability.databricks_insights.v_currently_failing_jobs AS
WITH latest_runs AS (
  SELECT
    workspace_id,
    job_id,
    run_id,
    result_state,
    MIN(period_start_time) AS run_start,
    MAX(period_end_time) AS run_end,
    ROW_NUMBER() OVER (
      PARTITION BY workspace_id, job_id
      ORDER BY MIN(period_start_time) DESC
    ) AS run_rank
  FROM system.lakeflow.job_run_timeline
  WHERE period_start_time >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
  GROUP BY workspace_id, job_id, run_id, result_state
)
SELECT
  lr.workspace_id,
  lr.job_id,
  j.name AS job_name,
  j.creator_user_name AS owner,
  lr.result_state,
  lr.run_start,
  lr.run_end,
  TIMESTAMPDIFF(SECOND, lr.run_start, lr.run_end) AS duration_seconds
FROM latest_runs lr
LEFT JOIN (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY workspace_id, job_id ORDER BY change_time DESC
  ) AS rn
  FROM system.lakeflow.jobs
) j ON lr.workspace_id = j.workspace_id
  AND lr.job_id = j.job_id AND j.rn = 1
WHERE lr.run_rank = 1
  AND lr.result_state = 'FAILED'
ORDER BY lr.run_start DESC;
```

### 5.3 Top Cost Drivers View

```sql
CREATE OR REPLACE VIEW observability.databricks_insights.v_top_cost_drivers AS
SELECT
  identity_metadata.run_as AS user_or_sp,
  billing_origin_product AS product,
  sku_name,
  COALESCE(usage_metadata.job_name, usage_metadata.notebook_path, 'N/A') AS workload_name,
  SUM(u.usage_quantity) AS total_dbus,
  SUM(u.usage_quantity * lp.pricing.effective_list.default) AS estimated_cost_usd
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices lp
  ON lp.sku_name = u.sku_name
  AND u.usage_end_time >= lp.price_start_time
  AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
WHERE u.usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY estimated_cost_usd DESC
LIMIT 50;
```

### 5.4 Security Anomaly View

```sql
CREATE OR REPLACE VIEW observability.databricks_insights.v_security_anomalies AS

-- IP Access Denials
SELECT
  'IP_ACCESS_DENIED' AS anomaly_type,
  event_time,
  workspace_id,
  user_identity.email AS user_email,
  source_ip_address,
  action_name AS detail
FROM system.access.audit
WHERE action_name IN ('IpAccessDenied', 'accountIpAclsValidationFailed')
  AND event_date >= CURRENT_DATE - INTERVAL 1 DAY

UNION ALL

-- High-volume destructive operations (>10 deletes by one user in 1 hour)
SELECT
  'HIGH_DESTRUCTIVE_OPS' AS anomaly_type,
  MAX(event_time) AS event_time,
  workspace_id,
  user_identity.email AS user_email,
  '' AS source_ip_address,
  CONCAT(COUNT(*), ' delete operations in 1 hour') AS detail
FROM system.access.audit
WHERE action_name LIKE 'delete%'
  AND event_date >= CURRENT_DATE - INTERVAL 1 DAY
  AND user_identity.email NOT LIKE '%System-User%'
GROUP BY
  workspace_id,
  user_identity.email,
  date_trunc('HOUR', event_time)
HAVING COUNT(*) > 10

UNION ALL

-- Admin group changes
SELECT
  'ADMIN_GROUP_CHANGE' AS anomaly_type,
  event_time,
  workspace_id,
  user_identity.email AS user_email,
  '' AS source_ip_address,
  CONCAT(action_name, ': ', request_params.targetUserName) AS detail
FROM system.access.audit
WHERE action_name IN ('addPrincipalToGroup', 'removePrincipalFromGroup')
  AND request_params.targetGroupName = 'admins'
  AND event_date >= CURRENT_DATE - INTERVAL 1 DAY

ORDER BY event_time DESC;
```

### 5.5 Query Performance View

```sql
CREATE OR REPLACE VIEW observability.databricks_insights.v_query_performance AS
SELECT
  user_name,
  warehouse_id,
  statement_type,
  COUNT(*) AS query_count,
  AVG(duration / 1000) AS avg_duration_seconds,
  PERCENTILE(duration / 1000, 0.95) AS p95_duration_seconds,
  MAX(duration / 1000) AS max_duration_seconds,
  SUM(rows_produced) AS total_rows_produced
FROM system.query.history
WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
  AND status = 'FINISHED'
GROUP BY user_name, warehouse_id, statement_type
ORDER BY avg_duration_seconds DESC;
```

---

## 6. Phase 4 — Databricks App

### 6.1 Project Structure

```
databricks-insights-app/
├── app.yaml                 # Databricks Apps configuration
├── requirements.txt         # Python dependencies
├── app.py                   # Main Dash application
├── pages/
│   ├── cost.py              # Cost & compute panel
│   ├── jobs.py              # Jobs & pipeline health
│   ├── governance.py        # Security & governance
│   ├── users.py             # User activity
│   └── ai_chat.py           # AI natural language interface
├── utils/
│   ├── db_connector.py      # SQL warehouse connection helper
│   └── queries.py           # Parameterized query library
└── assets/
    └── styles.css           # Custom styling
```

### 6.2 `app.yaml` — App Configuration

```yaml
command:
  - python
  - app.py

env:
  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom: sql-warehouse.id
  - name: DATABRICKS_SERVING_ENDPOINT
    valueFrom: serving-endpoint.serving_endpoint_url

resources:
  - name: sql-warehouse
    type: sql_warehouse
    sql_warehouse:
      id: "<your-warehouse-id>"
      permission: CAN_USE
  - name: serving-endpoint
    type: serving_endpoint
    serving_endpoint:
      name: "databricks-insights-ai-endpoint"
      permission: CAN_QUERY
```

### 6.3 `requirements.txt`

```
dash>=2.14.0
dash-bootstrap-components>=1.5.0
plotly>=5.18.0
databricks-sql-connector>=3.0.0
databricks-sdk>=0.20.0
pandas>=2.0.0
```

### 6.4 `utils/db_connector.py` — Database Connector

```python
import os
from databricks import sql as dbsql


def get_connection():
    """
    Create a connection to the SQL warehouse.
    Credentials are automatically injected by Databricks Apps.
    """
    return dbsql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
        # Auth is handled automatically by the app's service principal
        credentials_provider=lambda: {
            "Authorization": f"Bearer {os.environ.get('DATABRICKS_TOKEN', '')}"
        },
    )


def run_query(query: str, params: dict = None) -> list:
    """Execute a query and return results as list of dicts."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
```

### 6.5 `app.py` — Main Application

```python
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
)

# Navigation sidebar
sidebar = dbc.Nav(
    [
        dbc.NavLink(
            [html.I(className="bi bi-currency-dollar me-2"), "Cost & Compute"],
            href="/", active="exact"
        ),
        dbc.NavLink(
            [html.I(className="bi bi-gear me-2"), "Jobs & Pipelines"],
            href="/jobs", active="exact"
        ),
        dbc.NavLink(
            [html.I(className="bi bi-shield-check me-2"), "Governance"],
            href="/governance", active="exact"
        ),
        dbc.NavLink(
            [html.I(className="bi bi-people me-2"), "Users"],
            href="/users", active="exact"
        ),
        dbc.NavLink(
            [html.I(className="bi bi-robot me-2"), "AI Assistant"],
            href="/ai", active="exact"
        ),
    ],
    vertical=True,
    pills=True,
    className="bg-dark",
)

app.layout = dbc.Container(
    [
        dbc.Row([
            dbc.Col([
                html.H2("Databricks Insights", className="text-primary mb-3 mt-3"),
                html.P("Workspace Observability", className="text-muted"),
                html.Hr(),
                sidebar,
            ], width=2, className="bg-dark vh-100 position-fixed"),
            dbc.Col([
                dash.page_container,
            ], width=10, className="ms-auto"),
        ]),
    ],
    fluid=True,
)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
```

### 6.6 `pages/cost.py` — Cost & Compute Panel

```python
import dash
from dash import html, dcc, callback, Output, Input
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from utils.db_connector import run_query

dash.register_page(__name__, path="/", name="Cost & Compute")


def layout():
    return dbc.Container([
        html.H3("Cost & Compute Overview", className="mt-3 mb-4"),

        # KPI Cards Row
        dbc.Row(id="cost-kpi-cards", className="mb-4"),

        # Burn rate chart
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Hourly Cost Burn Rate (Last 7 Days)"),
                    dbc.CardBody(dcc.Graph(id="burn-rate-chart")),
                ])
            ], width=8),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Cost by Product (30d)"),
                    dbc.CardBody(dcc.Graph(id="cost-by-product-chart")),
                ])
            ], width=4),
        ], className="mb-4"),

        # Top cost drivers table
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Top 20 Cost Drivers (Last 30 Days)"),
                    dbc.CardBody(id="cost-drivers-table"),
                ])
            ], width=12),
        ], className="mb-4"),

        # Zombie clusters
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.Span([
                        "Zombie Clusters ",
                        dbc.Badge("Action Required", color="danger", className="ms-2")
                    ])),
                    dbc.CardBody(id="zombie-clusters-table"),
                ])
            ], width=12),
        ]),

        # Auto-refresh every 5 minutes
        dcc.Interval(id="cost-refresh", interval=300_000, n_intervals=0),
    ])


@callback(
    Output("cost-kpi-cards", "children"),
    Output("burn-rate-chart", "figure"),
    Output("cost-by-product-chart", "figure"),
    Output("cost-drivers-table", "children"),
    Output("zombie-clusters-table", "children"),
    Input("cost-refresh", "n_intervals"),
)
def update_cost_panel(_):
    # KPI queries
    today_cost = run_query("""
        SELECT SUM(u.usage_quantity * lp.pricing.effective_list.default) AS cost
        FROM system.billing.usage u
        JOIN system.billing.list_prices lp
          ON lp.sku_name = u.sku_name
          AND u.usage_end_time >= lp.price_start_time
          AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
        WHERE u.usage_date = CURRENT_DATE
    """)

    mtd_cost = run_query("""
        SELECT SUM(u.usage_quantity * lp.pricing.effective_list.default) AS cost
        FROM system.billing.usage u
        JOIN system.billing.list_prices lp
          ON lp.sku_name = u.sku_name
          AND u.usage_end_time >= lp.price_start_time
          AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
        WHERE u.usage_date >= DATE_TRUNC('MONTH', CURRENT_DATE)
    """)

    active_clusters = run_query("""
        SELECT COUNT(DISTINCT cluster_id) AS cnt
        FROM observability.databricks_insights.gold_compute_inventory
        WHERE health_status = 'ACTIVE'
    """)

    zombie_count = run_query("""
        SELECT COUNT(DISTINCT cluster_id) AS cnt
        FROM observability.databricks_insights.gold_compute_inventory
        WHERE health_status = 'ZOMBIE'
    """)

    kpi_cards = dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H6("Today's Spend", className="text-muted"),
                html.H3(f"${today_cost[0]['cost'] or 0:,.2f}", className="text-success"),
            ])
        ]), width=3),
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H6("Month-to-Date", className="text-muted"),
                html.H3(f"${mtd_cost[0]['cost'] or 0:,.2f}", className="text-info"),
            ])
        ]), width=3),
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H6("Active Clusters", className="text-muted"),
                html.H3(f"{active_clusters[0]['cnt']}", className="text-warning"),
            ])
        ]), width=3),
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H6("Zombie Clusters", className="text-muted"),
                html.H3(
                    f"{zombie_count[0]['cnt']}",
                    className="text-danger" if zombie_count[0]['cnt'] > 0 else "text-success"
                ),
            ])
        ]), width=3),
    ])

    # Burn rate chart
    burn_data = run_query("""
        SELECT hour, cost_usd, rolling_24h_avg
        FROM observability.databricks_insights.v_realtime_burn_rate
        ORDER BY hour
    """)
    burn_df = pd.DataFrame(burn_data)
    burn_fig = go.Figure()
    if not burn_df.empty:
        burn_fig.add_trace(go.Bar(x=burn_df["hour"], y=burn_df["cost_usd"],
                                   name="Hourly Cost", opacity=0.6))
        burn_fig.add_trace(go.Scatter(x=burn_df["hour"], y=burn_df["rolling_24h_avg"],
                                       name="24h Rolling Avg", line=dict(color="red", width=2)))
    burn_fig.update_layout(template="plotly_dark", height=400, margin=dict(l=40, r=20, t=20, b=40))

    # Cost by product
    product_data = run_query("""
        SELECT product, SUM(estimated_cost_usd) AS cost
        FROM observability.databricks_insights.gold_cost_daily
        WHERE date >= CURRENT_DATE - INTERVAL 30 DAYS
        GROUP BY product
        ORDER BY cost DESC
    """)
    product_df = pd.DataFrame(product_data)
    product_fig = px.pie(product_df, values="cost", names="product",
                         template="plotly_dark", hole=0.4)
    product_fig.update_layout(height=400, margin=dict(l=20, r=20, t=20, b=20))

    # Top cost drivers
    drivers = run_query("SELECT * FROM observability.databricks_insights.v_top_cost_drivers LIMIT 20")
    drivers_table = dbc.Table.from_dataframe(
        pd.DataFrame(drivers),
        striped=True, bordered=True, hover=True, dark=True, responsive=True,
    ) if drivers else html.P("No data available")

    # Zombie clusters
    zombies = run_query("""
        SELECT cluster_id, cluster_name, owner, dbus_last_7d, last_active_time
        FROM observability.databricks_insights.gold_compute_inventory
        WHERE health_status = 'ZOMBIE'
    """)
    zombies_table = dbc.Table.from_dataframe(
        pd.DataFrame(zombies),
        striped=True, bordered=True, hover=True, dark=True, responsive=True,
    ) if zombies else html.P("No zombie clusters detected.", className="text-success")

    return kpi_cards, burn_fig, product_fig, drivers_table, zombies_table
```

### 6.7 `pages/jobs.py` — Jobs & Pipeline Health (Abbreviated)

```python
import dash
from dash import html, dcc, callback, Output, Input
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from utils.db_connector import run_query

dash.register_page(__name__, path="/jobs", name="Jobs & Pipelines")


def layout():
    return dbc.Container([
        html.H3("Jobs & Pipeline Health", className="mt-3 mb-4"),

        # KPI Row
        dbc.Row(id="jobs-kpis", className="mb-4"),

        # Failing jobs table
        dbc.Row([
            dbc.Col(dbc.Card([
                dbc.CardHeader("Currently Failing Jobs"),
                dbc.CardBody(id="failing-jobs-table"),
            ]), width=12),
        ], className="mb-4"),

        # Job success rate heatmap
        dbc.Row([
            dbc.Col(dbc.Card([
                dbc.CardHeader("Job Success Rates (Last 30 Days)"),
                dbc.CardBody(dcc.Graph(id="job-success-chart")),
            ]), width=8),
            dbc.Col(dbc.Card([
                dbc.CardHeader("Pipeline Health (7d)"),
                dbc.CardBody(id="pipeline-health-table"),
            ]), width=4),
        ]),

        dcc.Interval(id="jobs-refresh", interval=300_000, n_intervals=0),
    ])


@callback(
    Output("jobs-kpis", "children"),
    Output("failing-jobs-table", "children"),
    Output("job-success-chart", "figure"),
    Output("pipeline-health-table", "children"),
    Input("jobs-refresh", "n_intervals"),
)
def update_jobs_panel(_):
    # KPIs
    stats = run_query("""
        SELECT
          COUNT(*) AS total_jobs,
          AVG(success_rate_pct) AS avg_success_rate,
          SUM(failure_count) AS total_failures_30d
        FROM observability.databricks_insights.gold_job_health
    """)

    kpis = dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Total Jobs", className="text-muted"),
            html.H3(f"{stats[0]['total_jobs']}")
        ])), width=4),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Avg Success Rate", className="text-muted"),
            html.H3(f"{stats[0]['avg_success_rate']:.1f}%", className="text-success")
        ])), width=4),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Total Failures (30d)", className="text-muted"),
            html.H3(f"{stats[0]['total_failures_30d']}", className="text-danger")
        ])), width=4),
    ])

    # Failing jobs
    failing = run_query("""
        SELECT * FROM observability.databricks_insights.v_currently_failing_jobs LIMIT 25
    """)
    failing_table = dbc.Table.from_dataframe(
        pd.DataFrame(failing),
        striped=True, bordered=True, hover=True, dark=True, responsive=True,
    ) if failing else html.P("All jobs healthy!", className="text-success")

    # Job success chart — top 30 jobs by run count
    job_data = run_query("""
        SELECT job_name, success_rate_pct, total_runs_30d, failure_count
        FROM observability.databricks_insights.gold_job_health
        ORDER BY total_runs_30d DESC LIMIT 30
    """)
    job_df = pd.DataFrame(job_data)
    job_fig = px.bar(
        job_df, x="job_name", y="success_rate_pct",
        color="success_rate_pct",
        color_continuous_scale=["red", "yellow", "green"],
        range_color=[0, 100],
        template="plotly_dark",
    )
    job_fig.update_layout(height=400, xaxis_tickangle=-45)

    # Pipeline health
    pipelines = run_query("""
        SELECT name, total_updates_7d, success_count, failure_count, success_rate_pct
        FROM observability.databricks_insights.gold_pipeline_health
        ORDER BY failure_count DESC
    """)
    pipeline_table = dbc.Table.from_dataframe(
        pd.DataFrame(pipelines),
        striped=True, bordered=True, hover=True, dark=True, responsive=True,
    ) if pipelines else html.P("No pipeline data")

    return kpis, failing_table, job_fig, pipeline_table
```

### 6.8 Deploying the App

Deploy using the Databricks CLI:

```bash
# Install/update CLI
pip install databricks-cli --upgrade

# Configure authentication
databricks configure --token

# Create the app
databricks apps create databricks-insights-app \
  --description "Workspace observability platform for admins"

# Deploy the code
databricks apps deploy databricks-insights-app \
  --source-code-path ./databricks-insights-app/

# Check deployment status
databricks apps get databricks-insights-app
```

After deployment, the app is accessible at:
`https://<workspace-url>/apps/databricks-insights-app`

All workspace admins with permissions can access it through their existing SSO credentials.

---

## 7. Phase 5 — Alerts & Automated Actions

### 7.1 SQL Alerts for Key Thresholds

Create these alerts in Databricks SQL, each attached to a schedule and notification destination.

**Alert 1: Daily Cost Spike Detection**

```sql
-- Create as a Databricks SQL Query, then attach an alert
WITH daily_costs AS (
  SELECT
    usage_date,
    SUM(u.usage_quantity * lp.pricing.effective_list.default) AS daily_cost
  FROM system.billing.usage u
  JOIN system.billing.list_prices lp
    ON lp.sku_name = u.sku_name
    AND u.usage_end_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
  WHERE u.usage_date >= CURRENT_DATE - INTERVAL 14 DAYS
  GROUP BY usage_date
),
stats AS (
  SELECT
    AVG(daily_cost) AS avg_cost,
    STDDEV(daily_cost) AS stddev_cost
  FROM daily_costs
  WHERE usage_date < CURRENT_DATE
)
SELECT
  dc.daily_cost,
  s.avg_cost,
  dc.daily_cost / s.avg_cost AS cost_ratio
FROM daily_costs dc
CROSS JOIN stats s
WHERE dc.usage_date = CURRENT_DATE
  AND dc.daily_cost > s.avg_cost + (2 * s.stddev_cost)
```

Alert condition: Trigger when `cost_ratio` > 1 (i.e., any row is returned).
Schedule: Every hour.
Notification: Slack webhook or email to admin group.

**Alert 2: Zombie Cluster Alert**

```sql
SELECT
  cluster_id,
  cluster_name,
  owner,
  dbus_last_7d
FROM observability.databricks_insights.gold_compute_inventory
WHERE health_status = 'ZOMBIE'
```

Alert condition: Trigger when row count > 0.
Schedule: Every 4 hours.

**Alert 3: Job Failure Spike**

```sql
SELECT COUNT(*) AS failures_last_hour
FROM system.lakeflow.job_run_timeline
WHERE result_state = 'FAILED'
  AND period_start_time >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
HAVING COUNT(*) > 5
```

Alert condition: Trigger when `failures_last_hour` > 5.
Schedule: Every 15 minutes.

**Alert 4: Security Anomaly Alert**

```sql
SELECT COUNT(*) AS anomaly_count
FROM observability.databricks_insights.v_security_anomalies
```

Alert condition: Trigger when `anomaly_count` > 0.
Schedule: Every 30 minutes.

### 7.2 Setting Up Notification Destinations

```bash
# Create a Slack notification destination via API
curl -X POST "https://<workspace-url>/api/2.0/notification-destinations" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "display_name": "Databricks Insights Slack Alerts",
    "config": {
      "slack": {
        "url": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
      }
    }
  }'
```

### 7.3 Automated Zombie Cluster Termination (Optional)

Create a scheduled notebook that terminates zombie clusters automatically:

```python
# Notebook: databricks_insights_auto_terminate_zombies
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Query zombie clusters
zombies = spark.sql("""
    SELECT cluster_id, cluster_name, owner
    FROM observability.databricks_insights.gold_compute_inventory
    WHERE health_status = 'ZOMBIE'
""").collect()

terminated = []
for z in zombies:
    try:
        w.clusters.delete(cluster_id=z.cluster_id)
        terminated.append(z.cluster_name)
        print(f"Terminated zombie cluster: {z.cluster_name} ({z.cluster_id})")
    except Exception as e:
        print(f"Failed to terminate {z.cluster_name}: {e}")

if terminated:
    # Log to audit table
    spark.sql(f"""
        INSERT INTO observability.databricks_insights.termination_log
        VALUES (CURRENT_TIMESTAMP(), ARRAY({','.join(f"'{t}'" for t in terminated)}))
    """)
```

Schedule this as a Lakeflow Job running every 6 hours with appropriate approval gates.

---

## 8. Phase 6 — AI Layer

The AI layer enables admins to ask natural language questions about their workspace. This uses Databricks Foundation Model APIs with AI Functions for SQL generation.

### 8.1 Option A: AI Functions (Simplest — Recommended)

AI Functions let you call LLMs directly from SQL. This is the fastest path.

```sql
-- Example: Use ai_query() to generate insights from cost data
SELECT ai_query(
  'databricks-meta-llama-3-3-70b-instruct',
  CONCAT(
    'You are a Databricks workspace cost analyst. Based on this data, ',
    'provide a brief summary of cost trends and actionable recommendations. ',
    'Data: ',
    (SELECT to_json(collect_list(struct(*)))
     FROM observability.databricks_insights.gold_cost_daily
     WHERE date >= CURRENT_DATE - INTERVAL 7 DAYS)
  )
) AS cost_insight;
```

### 8.2 Option B: Model Serving Endpoint + App Integration

For a more interactive chat experience inside the app, create a serving endpoint:

**Step 1: Create an External Model Endpoint (or use pay-per-token)**

Navigate to **Serving → Create Serving Endpoint**:

- Name: `databricks-insights-ai-endpoint`
- Entity: Select a Foundation Model (e.g., `databricks-meta-llama-3-3-70b-instruct`) or an external model (Claude, GPT-4)
- Configure AI Gateway: Enable usage tracking and rate limiting

**Step 2: Create the Chat Backend in the App**

Add `pages/ai_chat.py`:

```python
import dash
from dash import html, dcc, callback, Output, Input, State
import dash_bootstrap_components as dbc
import os
import json
import requests
from utils.db_connector import run_query

dash.register_page(__name__, path="/ai", name="AI Assistant")

# System prompt with schema context
SYSTEM_PROMPT = """You are Databricks Insights AI, an expert Databricks workspace analyst.
You help admins understand their workspace costs, job health, security posture,
and user activity.

You have access to the following tables in the `observability.databricks_insights` schema:

1. gold_cost_daily: columns [date, workspace_id, product, sku_name,
   total_dbus, estimated_cost_usd, unique_users]
2. gold_job_health: columns [workspace_id, job_id, job_name,
   total_runs_30d, success_count, failure_count, success_rate_pct,
   avg_duration_seconds, p95_duration_seconds, last_run_time, job_owner]
3. gold_compute_inventory: columns [workspace_id, cluster_id,
   cluster_name, owner, health_status, dbus_last_7d, last_active_time]
4. gold_user_activity: columns [user_email, login_count_30d,
   last_login, active_days, total_dbus_30d, estimated_cost_30d]
5. gold_governance_posture: columns [workspace_id,
   permission_changes_24h, ip_denials_24h, destructive_ops_24h,
   admin_group_changes_24h]
6. gold_pipeline_health: columns [workspace_id, pipeline_id, name,
   total_updates_7d, success_count, failure_count, success_rate_pct]

When asked a question:
1. Generate a SQL query against these tables
2. Return ONLY the SQL wrapped in ```sql``` code blocks
3. Keep queries simple and efficient
"""


def query_ai_endpoint(user_message: str) -> str:
    """Send a message to the model serving endpoint."""
    endpoint_url = os.environ.get("DATABRICKS_SERVING_ENDPOINT")
    token = os.environ.get("DATABRICKS_TOKEN")

    response = requests.post(
        endpoint_url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            "max_tokens": 1024,
            "temperature": 0.1,
        },
    )
    data = response.json()
    return data["choices"][0]["message"]["content"]


def extract_sql(ai_response: str) -> str:
    """Extract SQL from markdown code blocks."""
    if "```sql" in ai_response:
        return ai_response.split("```sql")[1].split("```")[0].strip()
    elif "```" in ai_response:
        return ai_response.split("```")[1].split("```")[0].strip()
    return None


def layout():
    return dbc.Container([
        html.H3("AI Workspace Assistant", className="mt-3 mb-4"),
        html.P("Ask questions about your workspace in natural language.",
               className="text-muted"),

        # Chat history
        html.Div(id="chat-history", className="mb-3",
                 style={"maxHeight": "500px", "overflowY": "auto"}),

        # Input
        dbc.InputGroup([
            dbc.Input(
                id="user-input",
                placeholder="e.g., 'Why did costs spike last Tuesday?'",
                type="text",
            ),
            dbc.Button("Ask", id="send-btn", color="primary"),
        ], className="mb-3"),

        # Results area
        html.Div(id="ai-results"),

        # Store chat history
        dcc.Store(id="chat-store", data=[]),
    ])


@callback(
    Output("chat-history", "children"),
    Output("ai-results", "children"),
    Output("chat-store", "data"),
    Output("user-input", "value"),
    Input("send-btn", "n_clicks"),
    State("user-input", "value"),
    State("chat-store", "data"),
    prevent_initial_call=True,
)
def handle_chat(n_clicks, user_input, chat_history):
    if not user_input:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update

    # Get AI response
    ai_response = query_ai_endpoint(user_input)

    # Try to extract and run SQL
    sql_query = extract_sql(ai_response)
    result_component = html.Div()

    if sql_query:
        try:
            results = run_query(sql_query)
            import pandas as pd
            df = pd.DataFrame(results)
            result_component = html.Div([
                html.H6("Generated SQL:", className="text-muted mt-2"),
                dcc.Markdown(f"```sql\n{sql_query}\n```"),
                html.H6("Results:", className="text-muted mt-2"),
                dbc.Table.from_dataframe(
                    df, striped=True, bordered=True, hover=True,
                    dark=True, responsive=True
                ) if not df.empty else html.P("Query returned no results.")
            ])
        except Exception as e:
            result_component = html.Div([
                html.P(f"Query execution error: {str(e)}", className="text-danger"),
                dcc.Markdown(f"```sql\n{sql_query}\n```"),
            ])
    else:
        result_component = dcc.Markdown(ai_response)

    # Update chat history
    chat_history.append({"role": "user", "content": user_input})
    chat_history.append({"role": "assistant", "content": ai_response})

    chat_display = []
    for msg in chat_history:
        if msg["role"] == "user":
            chat_display.append(
                dbc.Alert(msg["content"], color="primary", className="text-end")
            )
        else:
            chat_display.append(
                dbc.Alert(msg["content"][:200] + "...", color="secondary")
            )

    return chat_display, result_component, chat_history, ""
```

### 8.3 Configure AI Gateway

For the serving endpoint, enable AI Gateway for observability:

1. Navigate to **Serving → databricks-insights-ai-endpoint → Edit AI Gateway**
2. Enable:
   - **Usage tracking** — logs token usage to an inference table
   - **Rate limiting** — set per-user limits (e.g., 60 requests/minute)
   - **AI Guardrails** — block PII in responses (optional)
3. Save configuration

The inference table logs all requests/responses to a Delta table in Unity Catalog, giving you full auditability of AI interactions.

---

## 9. Deployment with Databricks Asset Bundles

For production deployments, use Databricks Asset Bundles (DABs) to manage everything as code.

### 9.1 Bundle Configuration: `databricks.yml`

```yaml
bundle:
  name: databricks-insights

workspace:
  host: https://<your-workspace-url>

variables:
  warehouse_id:
    description: Serverless SQL warehouse ID for the app
  catalog:
    default: observability
  schema:
    default: databricks_insights

resources:
  pipelines:
    databricks_insights_pipeline:
      name: databricks-insights-pipeline
      target: ${var.catalog}.${var.schema}
      libraries:
        - notebook:
            path: ./pipeline/databricks_insights_pipeline.py
      configuration:
        pipeline_type: WORKSPACE
      clusters:
        - label: default
          autoscale:
            min_workers: 1
            max_workers: 4

  jobs:
    databricks_insights_refresh:
      name: databricks-insights-refresh
      schedule:
        quartz_cron_expression: "0 */15 * * * ?"
        timezone_id: UTC
      tasks:
        - task_key: refresh_pipeline
          pipeline_task:
            pipeline_id: ${resources.pipelines.databricks_insights_pipeline.id}

    databricks_insights_zombie_cleanup:
      name: databricks-insights-zombie-cleanup
      schedule:
        quartz_cron_expression: "0 0 */6 * * ?"
        timezone_id: UTC
      tasks:
        - task_key: terminate_zombies
          notebook_task:
            notebook_path: ./notebooks/auto_terminate_zombies.py

  apps:
    databricks_insights_app:
      name: databricks-insights-app
      source_code_path: ./databricks-insights-app/
      resources:
        - name: sql-warehouse
          type: sql_warehouse
          sql_warehouse:
            id: ${var.warehouse_id}
            permission: CAN_USE
        - name: serving-endpoint
          type: serving_endpoint
          serving_endpoint:
            name: databricks-insights-ai-endpoint
            permission: CAN_QUERY
```

### 9.2 Deploy

```bash
# Validate the bundle
databricks bundle validate

# Deploy to workspace
databricks bundle deploy --target production

# Run the pipeline once
databricks bundle run databricks_insights_refresh
```

---

## 10. Operational Runbook

### 10.1 Daily Admin Checklist

1. **Open Databricks Insights dashboard** — check the Cost panel for any burn rate anomalies
2. **Review zombie clusters** — terminate or investigate
3. **Check failing jobs** — triage by business impact
4. **Review security anomalies** — investigate IP denials and permission changes
5. **Ask the AI** — "What were the top 3 cost increases this week and why?"

### 10.2 Weekly Review Queries

```sql
-- Week-over-week cost comparison
SELECT
  billing_origin_product,
  SUM(CASE WHEN usage_date BETWEEN CURRENT_DATE - INTERVAL 14 DAYS
    AND CURRENT_DATE - INTERVAL 8 DAYS THEN usage_quantity END) AS prev_week_dbus,
  SUM(CASE WHEN usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
    THEN usage_quantity END) AS this_week_dbus,
  ROUND((this_week_dbus - prev_week_dbus) / NULLIF(prev_week_dbus, 0) * 100, 2)
    AS growth_pct
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE - INTERVAL 14 DAYS
GROUP BY billing_origin_product
ORDER BY growth_pct DESC;
```

### 10.3 Monthly Governance Report Query

```sql
-- Monthly governance posture summary
SELECT
  DATE_TRUNC('MONTH', event_date) AS month,
  COUNT(DISTINCT CASE WHEN action_name LIKE '%Permission%'
    THEN event_id END) AS permission_events,
  COUNT(DISTINCT CASE WHEN action_name LIKE 'delete%'
    THEN event_id END) AS destructive_events,
  COUNT(DISTINCT user_identity.email) AS active_users,
  COUNT(DISTINCT CASE WHEN action_name = 'IpAccessDenied'
    THEN event_id END) AS ip_denials
FROM system.access.audit
WHERE event_date >= CURRENT_DATE - INTERVAL 6 MONTHS
GROUP BY 1
ORDER BY 1 DESC;
```

### 10.4 Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| System tables return empty | Tables not enabled or no grants | Re-run Section 2.1 grants |
| App shows "connection error" | SQL warehouse stopped or wrong ID | Check warehouse status; update `app.yaml` |
| Pipeline fails on refresh | Schema permissions | Grant CREATE TABLE to pipeline service principal |
| AI endpoint returns errors | Endpoint not running or rate limited | Check serving endpoint status in UI |
| Cost data lagged >24h | Normal for billing tables | System tables update throughout the day, not real-time |
| Audit logs missing events | Verbose logging not enabled | Enable verbose audit logs (Section 2.3) |

---

## Summary

The Databricks Insights platform gives workspace admins a single pane of glass across five dimensions: cost, compute, jobs, governance, and user activity. By building on Databricks system tables, Lakeflow Declarative Pipelines, Databricks Apps, SQL Alerts, and Foundation Model APIs, the entire solution runs natively within the platform with zero external infrastructure.

**Key documentation references:**

- System Tables: `docs.databricks.com/admin/system-tables/`
- Databricks Apps: `docs.databricks.com/dev-tools/databricks-apps/`
- Lakeflow Declarative Pipelines: `docs.databricks.com/ldp/`
- AI Gateway: `docs.databricks.com/ai-gateway/`
- Foundation Model APIs: `docs.databricks.com/machine-learning/foundation-model-apis`
- SQL Alerts: `docs.databricks.com/sql/user/alerts/`
- Asset Bundles: `docs.databricks.com/dev-tools/bundles/`
