"""
Shared configuration for Databricks Insights App
This file should be kept in sync with deploy/config.py
"""
import os

# Try to import from deploy config if available (when running in workspace)
try:
    import sys
    import os as os_module
    # Add parent directory to path to import deploy config
    parent_dir = os_module.path.dirname(os_module.path.dirname(os_module.path.abspath(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    from deploy.config import (
        CATALOG, SCHEMA, WAREHOUSE_ID, AI_ENDPOINT_NAME, 
        WORKSPACE_URL, APP_NAME
    )
except ImportError:
    # Fallback to environment variables or defaults
    CATALOG = os.getenv("DATABRICKS_INSIGHTS_CATALOG", "main")
    SCHEMA = os.getenv("DATABRICKS_INSIGHTS_SCHEMA", "databricks_insights")
    WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "<your-warehouse-id>")
    AI_ENDPOINT_NAME = os.getenv("DATABRICKS_INSIGHTS_AI_ENDPOINT", None)
    WORKSPACE_URL = os.getenv("DATABRICKS_HOST", "https://<your-workspace-url>")
    APP_NAME = os.getenv("DATABRICKS_INSIGHTS_APP_NAME", "databricks-insights-app")

# Full schema path
SCHEMA_PATH = f"{CATALOG}.{SCHEMA}"

# AI Endpoint URL (constructed from endpoint name if needed)
# In Databricks Apps, this is typically provided via environment variable
# from the app.yaml resources section
AI_ENDPOINT_URL = os.getenv("DATABRICKS_SERVING_ENDPOINT", None)
