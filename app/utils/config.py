"""
Configuration for Databricks Insights App
Imports from shared config.py which reads from deploy/config.py
"""
# Import from shared config (which reads from deploy/config.py)
try:
    from config import SCHEMA_PATH, AI_ENDPOINT_URL, AI_ENDPOINT_NAME
except ImportError:
    # Fallback if config.py not available
    import os
    CATALOG = os.getenv("DATABRICKS_INSIGHTS_CATALOG", "main")
    SCHEMA = os.getenv("DATABRICKS_INSIGHTS_SCHEMA", "databricks_insights")
    SCHEMA_PATH = f"{CATALOG}.{SCHEMA}"
    AI_ENDPOINT_URL = os.getenv("DATABRICKS_SERVING_ENDPOINT", None)
    AI_ENDPOINT_NAME = os.getenv("DATABRICKS_INSIGHTS_AI_ENDPOINT", None)
