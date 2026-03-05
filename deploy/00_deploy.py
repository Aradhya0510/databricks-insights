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
# MAGIC - Databricks Apps enabled (Workspace Admin) - optional
# MAGIC - Serverless SQL warehouse already created
# MAGIC - System table grants require metastore admin (one-time setup)
# MAGIC - Regular users can create schemas and views in 'main' catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Configuration

# COMMAND ----------

# Import configuration
import sys
import os

# Get notebook path and determine repo root
notebook_path_raw = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# Notebook path from dbutils may or may not include /Workspace prefix
# Ensure it starts with /Workspace
if not notebook_path_raw.startswith('/Workspace'):
    if notebook_path_raw.startswith('/'):
        # Path starts with / but not /Workspace, add it
        notebook_path = '/Workspace' + notebook_path_raw
    else:
        # Path doesn't start with /, add /Workspace/
        notebook_path = '/Workspace/' + notebook_path_raw
else:
    notebook_path = notebook_path_raw

# Notebook path format: /Workspace/Users/user@domain.com/repo-name/deploy/00_deploy
# Extract repo root by finding the 'deploy' directory and getting everything before it
parts = notebook_path.split('/')
# Filter out empty strings from split
parts = [p for p in parts if p]

if 'deploy' in parts:
    deploy_idx = parts.index('deploy')
    # Reconstruct path with leading slash (for /Workspace)
    repo_root = '/' + '/'.join(parts[:deploy_idx])
else:
    # Fallback: remove last two parts (deploy/00_deploy) to get repo root
    if len(parts) >= 2:
        repo_root = '/' + '/'.join(parts[:-2])
    else:
        repo_root = os.path.dirname(os.path.dirname(notebook_path))

print(f"📁 Notebook path: {notebook_path}")
print(f"📁 Repo root: {repo_root}")

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
    CATALOG = "main"
    SCHEMA = "databricks_insights"
    INSIGHTS_USER = None
    APP_USER = None
    PIPELINE_NAME = "databricks-insights-pipeline"
    APP_NAME = "databricks-insights-app"
    AI_ENDPOINT_NAME = None
    REFRESH_SCHEDULE = "0 */15 * * * ?"
    ZOMBIE_CLEANUP_SCHEDULE = "0 0 */6 * * ?"

# Get current user if not specified in config
try:
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
    print(f"✓ Current user: {current_user}")
except:
    current_user = None

# Use current user if not specified
if INSIGHTS_USER is None:
    INSIGHTS_USER = current_user
    print(f"✓ Using current user for insights: {INSIGHTS_USER}")

if APP_USER is None:
    APP_USER = current_user
    print(f"✓ Using current user for app: {APP_USER}")

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

def quote_identifier(identifier):
    """
    Quote SQL identifier if it contains special characters (hyphens, spaces, etc.)
    """
    # Check if identifier needs quoting (contains non-alphanumeric/underscore characters)
    import re
    if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
        return identifier  # No quoting needed
    else:
        return f'`{identifier}`'  # Quote with backticks

def execute_sql_file(file_path, replacements=None, skip_errors=None):
    """
    Execute SQL file by splitting into individual statements.
    spark.sql() can only execute one statement at a time.
    
    Args:
        file_path: Path to SQL file
        replacements: Dict of placeholder replacements
        skip_errors: List of error patterns to skip (e.g., ['PERMISSION_DENIED', 'PRINCIPAL_DOES_NOT_EXIST'])
    """
    if skip_errors is None:
        skip_errors = ['PERMISSION_DENIED', 'PRINCIPAL_DOES_NOT_EXIST']
    
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
        skipped = 0
        failed = 0
        for stmt in statements:
            if stmt.strip():  # Skip empty statements
                try:
                    spark.sql(stmt)
                    executed += 1
                except Exception as e:
                    error_str = str(e)
                    # Check if this is a skippable error
                    should_skip = any(err in error_str for err in skip_errors)
                    
                    if should_skip:
                        if 'PERMISSION_DENIED' in error_str and 'CREATE CATALOG' in stmt:
                            print(f"⚠ Skipped catalog creation (permission denied - catalog may already exist or require metastore admin)")
                            skipped += 1
                        elif 'PRINCIPAL_DOES_NOT_EXIST' in error_str or 'does not exist' in error_str.lower() or 'USER_DOES_NOT_EXIST' in error_str:
                            # Extract which user is missing from the statement
                            user_name = "unknown"
                            if INSIGHTS_USER in stmt or '{INSIGHTS_USER}' in stmt:
                                user_name = INSIGHTS_USER
                            elif APP_USER in stmt or '{APP_USER}' in stmt:
                                user_name = APP_USER
                            
                            print(f"⚠ Skipped grant to '{user_name}' (user not found - ensure user exists in workspace or update config.py)")
                            skipped += 1
                        else:
                            print(f"⚠ Skipped statement (expected error): {stmt[:80]}...")
                            skipped += 1
                    else:
                        print(f"✗ Error executing statement: {stmt[:100]}...")
                        print(f"   Error: {error_str[:200]}")
                        failed += 1
                        # Continue with next statement
                        continue
        
        return executed, skipped, failed
    except FileNotFoundError:
        raise FileNotFoundError(f"SQL file not found: {file_path}")
    except Exception as e:
        raise Exception(f"Error reading SQL file: {e}")

try:
    # Quote identifiers if they contain special characters
    quoted_catalog = quote_identifier(CATALOG)
    quoted_schema = quote_identifier(SCHEMA)
    
    replacements = {
        '{CATALOG}': quoted_catalog,
        '{SCHEMA}': quoted_schema,
        '{INSIGHTS_USER}': INSIGHTS_USER,
        '{APP_USER}': APP_USER
    }
    
    executed_count, skipped_count, failed_count = execute_sql_file(
        setup_sql_path, 
        replacements,
        skip_errors=['PERMISSION_DENIED', 'PRINCIPAL_DOES_NOT_EXIST']
    )
    print(f"\n{'='*60}")
    print(f"Setup SQL Summary:")
    print(f"  ✓ Executed: {executed_count} statements")
    if skipped_count > 0:
        print(f"  ⚠ Skipped: {skipped_count} statements (expected - see details above)")
    if failed_count > 0:
        print(f"  ✗ Failed: {failed_count} statements (unexpected errors)")
    print(f"{'='*60}")
    
    if skipped_count > 0:
        print(f"\n📋 Next Steps:")
        print(f"  1. If schema creation was skipped: Ensure you have CREATE SCHEMA privileges on catalog '{CATALOG}'")
        print(f"     (Using 'main' catalog requires no special privileges)")
        print(f"  2. If system table grants were skipped: These require metastore admin privileges")
        print(f"     Ask your workspace admin to run section 2 from sql/setup.sql ONCE")
        print(f"     After that, all workspace users can access system tables")
        print(f"  3. If user grants were skipped: Ensure these users exist in your workspace:")
        print(f"     - Insights user: {INSIGHTS_USER}")
        print(f"     - App user: {APP_USER}")
        print(f"     Update config.py with correct user emails if needed")
    
    if failed_count == 0:
        print("✓ Setup SQL execution completed")
    else:
        print("⚠ Setup SQL execution completed with some failures")
except FileNotFoundError as e:
    print(f"⚠ Setup SQL file not found: {e}")
    print("⚠ Please ensure sql/setup.sql exists in the repository.")
    # Fallback: execute basic setup (with quoted identifiers)
    try:
        quoted_catalog = quote_identifier(CATALOG)
        quoted_schema = quote_identifier(SCHEMA)
        # No need to create catalog - using 'main' which already exists
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quoted_catalog}.{quoted_schema}")
        print("✓ Basic schema created (grants may need manual execution)")
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
        
        # Replace catalog and schema placeholders (with quoted identifiers)
        quoted_catalog = quote_identifier(CATALOG)
        quoted_schema = quote_identifier(SCHEMA)
        view_sql = view_sql.replace('{CATALOG}', quoted_catalog)
        view_sql = view_sql.replace('{SCHEMA}', quoted_schema)
        # Also handle legacy hardcoded values (for backward compatibility)
        view_sql = view_sql.replace('observability.gods_eye', f'{quoted_catalog}.{quoted_schema}')
        view_sql = view_sql.replace('observability.databricks_insights', f'{quoted_catalog}.{quoted_schema}')
        
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
