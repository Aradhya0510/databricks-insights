"""
Configuration for Databricks Insights deployment
Update these values before running the deployment notebook
"""

# Workspace configuration
WORKSPACE_URL = "https://<your-workspace-url>"  # Replace with your workspace URL
WAREHOUSE_ID = "<your-warehouse-id>"  # Replace with your serverless SQL warehouse ID

# Catalog and schema configuration
CATALOG = "observability"
SCHEMA = "databricks_insights"

# Service principal/group names (update these to match your workspace)
ADMIN_GROUP = "databricks-insights-admins"  # Group/service principal with system table access
APP_SERVICE_PRINCIPAL = "databricks-insights-app-sp"  # Service principal for the app

# Pipeline configuration
PIPELINE_NAME = "databricks-insights-pipeline"
PIPELINE_TARGET = f"{CATALOG}.{SCHEMA}"

# Job configuration
REFRESH_JOB_NAME = "databricks-insights-refresh"
ZOMBIE_CLEANUP_JOB_NAME = "databricks-insights-zombie-cleanup"

# App configuration
APP_NAME = "databricks-insights-app"
APP_DESCRIPTION = "Workspace observability platform for admins"

# AI Endpoint configuration (optional - set to None to skip)
AI_ENDPOINT_NAME = "databricks-insights-ai-endpoint"
AI_MODEL_NAME = "databricks-meta-llama-3-3-70b-instruct"  # Or your preferred model

# Job schedules
REFRESH_SCHEDULE = "0 */15 * * * ?"  # Every 15 minutes
ZOMBIE_CLEANUP_SCHEDULE = "0 0 */6 * * ?"  # Every 6 hours
