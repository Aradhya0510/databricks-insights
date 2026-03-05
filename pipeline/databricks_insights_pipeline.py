# Phase 2: Lakeflow Declarative Pipeline
# This notebook creates materialized views (gold tables) from system tables
# Designed to work with both classic and serverless Databricks workspaces

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Cell 1 — Gold: Daily Cost Summary
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


# Cell 2 — Gold: Job Health Summary
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


# Cell 3 — Gold: Compute Inventory (Zombie Detection)
# Works for both classic and serverless workspaces
# Uses schema-aware column selection to handle differences between workspace types
@dp.materialized_view(
    comment="Active compute inventory with idle compute detection (works for both classic and serverless)"
)
def gold_compute_inventory():
    # Get latest cluster records (if any exist - may be empty in pure serverless workspaces)
    clusters_raw = (
        spark.read.table("system.compute.clusters")
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("workspace_id", "cluster_id")
            .orderBy(F.col("change_time").desc())
        ))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Join with recent billing to find usage by cluster_id
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

    # Get schema to check which columns exist (works for both workspace types)
    schema = clusters_raw.schema
    schema_cols = {field.name.lower() for field in schema.fields}
    
    # Build select list dynamically based on available columns
    select_exprs = [
        "workspace_id",
        "cluster_id"
    ]
    
    # Add optional columns if they exist, with fallbacks
    if "cluster_name" in schema_cols:
        select_exprs.append("COALESCE(cluster_name, 'Unknown') AS cluster_name")
    else:
        select_exprs.append("'Unknown' AS cluster_name")
    
    if "owned_by" in schema_cols:
        select_exprs.append("COALESCE(owned_by, 'Unknown') AS owner")
    else:
        select_exprs.append("'Unknown' AS owner")
    
    if "cluster_source" in schema_cols:
        select_exprs.append("COALESCE(cluster_source, 'Unknown') AS cluster_source")
    else:
        select_exprs.append("'Unknown' AS cluster_source")
    
    if "change_time" in schema_cols:
        select_exprs.append("COALESCE(change_time, CURRENT_TIMESTAMP()) AS change_time")
    else:
        select_exprs.append("CURRENT_TIMESTAMP() AS change_time")
    
    # Select columns using the dynamic expression list
    clusters = clusters_raw.selectExpr(*select_exprs)

    # Join with usage data and calculate health status
    result = (
        clusters
        .join(recent_usage, on="cluster_id", how="left")
        .withColumn(
            "dbus_last_7d",
            F.coalesce("dbus_last_7d", F.lit(0))
        )
        .withColumn(
            "health_status",
            # Health status based on usage (works for both classic and serverless)
            # Classic: may have state column, but we use usage for consistency
            # Serverless: uses usage only
            F.when(
                F.coalesce("dbus_last_7d", F.lit(0)) == 0,
                F.lit("IDLE")
            ).otherwise(F.lit("ACTIVE"))
        )
        .select(
            "workspace_id",
            "cluster_id",
            "cluster_name",
            "owner",
            "cluster_source",
            "dbus_last_7d",
            "last_active_time",
            "health_status",
            "change_time"
        )
    )
    
    return result


# Cell 4 — Gold: User Activity
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


# Cell 5 — Gold: Governance Posture Score
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


# Cell 6 — Gold: Pipeline Health
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
