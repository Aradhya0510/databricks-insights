"""
Configuration for Databricks Insights deployment
Update these values before running the deployment notebook
"""

# Workspace configuration
WORKSPACE_URL = "https://<your-workspace-url>"  # Replace with your workspace URL
WAREHOUSE_ID = "<your-warehouse-id>"  # Replace with your serverless SQL warehouse ID

# Catalog and schema configuration
# Use 'main' catalog (already exists, no admin privileges needed)
CATALOG = "main"
SCHEMA = "databricks_insights"

# User configuration (workspace users - no service principals needed)
# These should be email addresses of workspace users
# Leave as None to use the current user running the notebook
INSIGHTS_USER = None  # Set to your email (e.g., "user@example.com") or None to use current user
APP_USER = None  # Set to app user email or None to use current user

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

# Note: System table access grants still require metastore admin privileges
# If you don't have admin access, ask your workspace admin to run the grants from sql/setup.sql
# The grants can be done once and will work for all users in your workspace
