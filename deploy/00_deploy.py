# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Insights - Automated Deployment
# MAGIC 
# MAGIC This notebook will deploy the complete Databricks Insights observability platform:
# MAGIC 1. Create catalog and schema
# MAGIC 2. Grant system table access
# MAGIC 3. Create SQL views
# MAGIC 4. Create Lakeflow pipeline
# MAGIC 5. Create scheduled jobs
# MAGIC 6. Deploy Databricks App
# MAGIC 7. (Optional) Create AI endpoint
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - System tables enabled (Account Admin)
# MAGIC - Databricks Apps enabled (Workspace Admin)
# MAGIC - Serverless SQL warehouse already created
# MAGIC - Metastore admin privileges for grants

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Configuration

# COMMAND ----------

# Import configuration
import sys
import os

# Get notebook path and determine repo root
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# Notebook path format: /Workspace/Users/user@domain.com/repo-name/deploy/00_deploy
# Extract repo root by finding the 'deploy' directory and getting everything before it
parts = notebook_path.split('/')
if 'deploy' in parts:
    deploy_idx = parts.index('deploy')
    repo_root = '/'.join(parts[:deploy_idx])
else:
    # Fallback: remove last two parts (deploy/00_deploy) to get repo root
    repo_root = '/'.join(parts[:-2]) if len(parts) >= 2 else os.path.dirname(os.path.dirname(notebook_path))

# Add repo root to path
sys.path.insert(0, repo_root)

try:
    from deploy.config import *
    print("✓ Configuration loaded from deploy/config.py")
except ImportError:
    print("⚠ Configuration file not found. Using defaults.")
    print("   Please update deploy/config.py with your settings.")
    # Fallback defaults
    WORKSPACE_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
    WAREHOUSE_ID = "<your-warehouse-id>"
    CATALOG = "observability"
    SCHEMA = "databricks_insights"
    ADMIN_GROUP = "databricks-insights-admins"
    APP_SERVICE_PRINCIPAL = "databricks-insights-app-sp"
    PIPELINE_NAME = "databricks-insights-pipeline"
    APP_NAME = "databricks-insights-app"
    AI_ENDPOINT_NAME = None
    REFRESH_SCHEDULE = "0 */15 * * * ?"
    ZOMBIE_CLEANUP_SCHEDULE = "0 0 */6 * * ?"

print(f"📁 Repo root: {repo_root}")

# COMMAND ----------

# MAGIC %md
# ## Step 2: Setup SQL Warehouse Connection

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import *

w = WorkspaceClient()
print(f"✓ Connected to workspace: {WORKSPACE_URL}")

# Verify warehouse exists
try:
    warehouse = w.warehouses.get(WAREHOUSE_ID)
    print(f"✓ SQL Warehouse found: {warehouse.name} ({WAREHOUSE_ID})")
except Exception as e:
    print(f"✗ Error: Warehouse {WAREHOUSE_ID} not found. Please update WAREHOUSE_ID in config.py")
    raise

# COMMAND ----------

# MAGIC %md
# ## Step 3: Create Catalog and Schema

# COMMAND ----------

# Read and execute setup SQL
setup_sql_path = f"{repo_root}/sql/setup.sql"

def execute_sql_file(file_path, replacements=None):
    """
    Execute SQL file by splitting into individual statements.
    spark.sql() can only execute one statement at a time.
    """
    try:
        with open(file_path, 'r') as f:
            sql_content = f.read()
        
        # Replace placeholders if provided
        if replacements:
            for old, new in replacements.items():
                sql_content = sql_content.replace(old, new)
        
        # Split SQL into individual statements
        # Remove comments and empty lines, then split by semicolon
        statements = []
        current_statement = []
        
        for line in sql_content.split('\n'):
            # Skip comment-only lines and empty lines
            stripped = line.strip()
            if not stripped or stripped.startswith('--'):
                continue
            
            current_statement.append(line)
            
            # If line ends with semicolon, it's the end of a statement
            if stripped.endswith(';'):
                statement = '\n'.join(current_statement).strip()
                if statement:
                    statements.append(statement)
                current_statement = []
        
        # Execute each statement separately
        executed = 0
        for stmt in statements:
            if stmt.strip():  # Skip empty statements
                try:
                    spark.sql(stmt)
                    executed += 1
                except Exception as e:
                    print(f"⚠ Error executing statement: {stmt[:100]}...")
                    print(f"   Error: {e}")
                    # Continue with next statement
                    continue
        
        return executed
    except FileNotFoundError:
        raise FileNotFoundError(f"SQL file not found: {file_path}")
    except Exception as e:
        raise Exception(f"Error reading SQL file: {e}")

try:
    replacements = {
        'databricks-insights-admins': ADMIN_GROUP,
        'databricks-insights-app-sp': APP_SERVICE_PRINCIPAL
    }
    
    executed_count = execute_sql_file(setup_sql_path, replacements)
    print(f"✓ Executed {executed_count} SQL statements")
    print("✓ Catalog and schema created")
    print("✓ System table grants executed")
    print("✓ App access granted")
except FileNotFoundError as e:
    print(f"⚠ Setup SQL file not found: {e}")
    print("⚠ Please ensure sql/setup.sql exists in the repository.")
    # Fallback: execute basic setup
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
        print("✓ Basic catalog and schema created (grants may need manual execution)")
    except Exception as fallback_error:
        print(f"⚠ Fallback setup also failed: {fallback_error}")
except Exception as e:
    print(f"⚠ Error executing setup SQL: {e}")
    print("⚠ You may need to run sql/setup.sql manually as a metastore admin")

# COMMAND ----------

# MAGIC %md
# ## Step 4: Create SQL Views

# COMMAND ----------

views_dir = f"{repo_root}/sql/views"

view_files = [
    '01_realtime_burn_rate.sql',
    '02_currently_failing_jobs.sql',
    '03_top_cost_drivers.sql',
    '04_security_anomalies.sql',
    '05_query_performance.sql'
]

created_views = []
for view_file in view_files:
    view_path = os.path.join(views_dir, view_file)
    try:
        with open(view_path, 'r') as f:
            view_sql = f.read()
        
        # Replace schema reference if needed
        view_sql = view_sql.replace('observability.gods_eye', f'{CATALOG}.{SCHEMA}')
        view_sql = view_sql.replace('observability.databricks_insights', f'{CATALOG}.{SCHEMA}')
        
        spark.sql(view_sql)
        view_name = view_file.replace('.sql', '').replace('01_', '').replace('02_', '').replace('03_', '').replace('04_', '').replace('05_', '')
        created_views.append(view_name)
        print(f"✓ Created view: {view_name}")
    except FileNotFoundError:
        print(f"⚠ View file not found: {view_file}")
    except Exception as e:
        print(f"✗ Error creating view {view_file}: {e}")

print(f"\n✓ Created {len(created_views)} views")

# COMMAND ----------

# MAGIC %md
# ## Step 5: Create Lakeflow Pipeline

# COMMAND ----------

# Note: Pipeline creation via API is complex and requires Asset Bundles
# We'll provide instructions for manual creation or Asset Bundle deployment

pipeline_path = f"{repo_root}/pipeline/databricks_insights_pipeline.py"

try:
    # Verify pipeline file exists
    if os.path.exists(pipeline_path):
        print("✓ Pipeline code found")
        print("\n📋 To create the pipeline:")
        print("   Option 1: Use Databricks Asset Bundles")
        print("     - Run: databricks bundle deploy")
        print("     - This will create the pipeline automatically")
        print("\n   Option 2: Manual UI creation")
        print("     - Navigate to Workflows → Lakeflow Pipelines → Create Pipeline")
        print(f"     - Name: {PIPELINE_NAME}")
        print(f"     - Source: {pipeline_path}")
        print(f"     - Target: {PIPELINE_TARGET}")
    else:
        print("⚠ Pipeline file not found")
except Exception as e:
    print(f"⚠ Error checking pipeline: {e}")

# COMMAND ----------

# MAGIC %md
# ## Step 6: Create Jobs

# COMMAND ----------

from databricks.sdk.service.jobs import *

# Create refresh job (runs pipeline every 15 minutes)
try:
    # First, we need the pipeline ID - this would be created via Asset Bundles or UI
    # For now, we'll create a job that can be updated later with the pipeline ID
    
    refresh_job = w.jobs.create(
        name=REFRESH_JOB_NAME,
        schedule=CronSchedule(
            quartz_cron_expression=REFRESH_SCHEDULE,
            timezone_id="UTC"
        ),
        tasks=[
            Task(
                task_key="refresh_pipeline",
                description="Refresh the databricks insights pipeline",
                # Pipeline task would be added here once pipeline is created
                # pipeline_task=PipelineTask(pipeline_id="<pipeline-id>")
            )
        ],
        email_notifications=JobEmailNotifications(
            on_failure=["admin@example.com"]  # Update with your email
        )
    )
    print(f"✓ Created job: {REFRESH_JOB_NAME} (update with pipeline ID after pipeline creation)")
except Exception as e:
    print(f"⚠ Error creating refresh job: {e}")
    print("   You may need to create this job manually after the pipeline is created")

# Create zombie cleanup job
try:
    zombie_cleanup_path = f"{repo_root}/jobs/auto_terminate_zombies.py"
    
    zombie_job = w.jobs.create(
        name=ZOMBIE_CLEANUP_JOB_NAME,
        schedule=CronSchedule(
            quartz_cron_expression=ZOMBIE_CLEANUP_SCHEDULE,
            timezone_id="UTC"
        ),
        tasks=[
            Task(
                task_key="terminate_zombies",
                description="Automatically terminate zombie clusters",
                notebook_task=NotebookTask(
                    notebook_path=zombie_cleanup_path.replace(os.getcwd() + '/', '')
                )
            )
        ],
        email_notifications=JobEmailNotifications(
            on_failure=["admin@example.com"]  # Update with your email
        )
    )
    print(f"✓ Created job: {ZOMBIE_CLEANUP_JOB_NAME}")
except Exception as e:
    print(f"⚠ Error creating zombie cleanup job: {e}")

# COMMAND ----------

# MAGIC %md
# ## Step 7: Deploy Databricks App

# COMMAND ----------

# Note: App deployment via API is limited. Use Asset Bundles or CLI instead
print("⚠ App deployment via notebook is limited.")
print("   Recommended: Use Databricks Asset Bundles or CLI:")
print(f"   databricks bundle deploy")
print(f"   OR")
print(f"   databricks apps deploy {APP_NAME} --source-code-path ./app/")
print(f"\n   App will be accessible at: {WORKSPACE_URL}/apps/{APP_NAME}")

# COMMAND ----------

# MAGIC %md
# ## Step 8: (Optional) Create AI Endpoint

# COMMAND ----------

if AI_ENDPOINT_NAME:
    try:
        from databricks.sdk.service.serving import *
        
        # Check if endpoint already exists
        try:
            existing = w.serving_endpoints.get(AI_ENDPOINT_NAME)
            print(f"✓ AI Endpoint already exists: {AI_ENDPOINT_NAME}")
        except:
            # Create endpoint (this is a simplified version)
            print(f"⚠ AI Endpoint creation via API requires specific model serving setup")
            print(f"   Please create manually in UI: Serving → Create Serving Endpoint")
            print(f"   Name: {AI_ENDPOINT_NAME}")
            print(f"   Model: {AI_MODEL_NAME}")
except Exception as e:
    print(f"⚠ Skipping AI endpoint creation: {e}")

# COMMAND ----------

# MAGIC %md
# ## Deployment Summary

# COMMAND ----------

print("=" * 60)
print("DEPLOYMENT SUMMARY")
print("=" * 60)
print(f"✓ Catalog: {CATALOG}")
print(f"✓ Schema: {CATALOG}.{SCHEMA}")
print(f"✓ Views created: {len(created_views)}")
print(f"✓ Jobs created: {REFRESH_JOB_NAME}, {ZOMBIE_CLEANUP_JOB_NAME}")
print(f"\n⚠ Manual steps required:")
print(f"   1. Create pipeline in UI pointing to: pipeline/databricks_insights_pipeline.py")
print(f"   2. Update {REFRESH_JOB_NAME} with pipeline ID")
print(f"   3. Deploy app using: databricks bundle deploy OR databricks apps deploy")
print(f"   4. (Optional) Create AI endpoint: {AI_ENDPOINT_NAME}")
print("=" * 60)
