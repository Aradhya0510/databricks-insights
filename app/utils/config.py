"""
Configuration for Databricks Insights App
Reads catalog and schema from environment variables or uses defaults
"""
import os

# Catalog and schema configuration
# Defaults to 'main.databricks_insights' to match deployment
CATALOG = os.getenv("DATABRICKS_INSIGHTS_CATALOG", "main")
SCHEMA = os.getenv("DATABRICKS_INSIGHTS_SCHEMA", "databricks_insights")

# Full schema path
SCHEMA_PATH = f"{CATALOG}.{SCHEMA}"
