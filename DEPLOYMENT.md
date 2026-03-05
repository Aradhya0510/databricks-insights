# Deployment Guide

## Quick Deployment (One-Shot)

### Prerequisites Checklist

- [ ] System Tables enabled (Account Admin)
- [ ] Databricks Apps enabled (Workspace Admin)  
- [ ] Serverless SQL warehouse created (note the warehouse ID)
- [ ] Metastore admin access (for system table grants)

### Step 1: Clone Repository

**Option A: Using Databricks Repos (Recommended)**
1. In Databricks, navigate to **Repos**
2. Click **Add Repo**
3. Enter your GitHub repository URL
4. Click **Create Repo**
5. The repo will be cloned into `/Repos/<your-username>/databricks-insights`

**Option B: Using Git in Workspace**
```bash
# In a Databricks notebook or terminal
%sh
cd /Workspace
git clone <your-github-repo-url> databricks-insights
```

### Step 2: Configure

1. Open `deploy/config.py` in the cloned repository
2. Update the following values:
   ```python
   WORKSPACE_URL = "https://your-workspace.cloud.databricks.com"
   WAREHOUSE_ID = "your-warehouse-id"  # Get from SQL Warehouses UI
   ADMIN_GROUP = "your-admin-group"  # Your admin group/service principal
   APP_SERVICE_PRINCIPAL = "your-app-sp"  # Your app service principal
   ```

### Step 3: Run Deployment Notebook

1. Open `deploy/00_deploy.py` notebook
2. Run all cells
3. The notebook will:
   - ✅ Create catalog and schema
   - ✅ Grant system table access
   - ✅ Create SQL views
   - ✅ Create scheduled jobs
   - 📋 Provide instructions for pipeline and app

### Step 4: Complete Pipeline Setup

**Option A: Using Asset Bundles (Recommended)**
```bash
# From your local machine or CI/CD
databricks bundle deploy
```

**Option B: Manual UI**
1. Navigate to **Workflows → Lakeflow Pipelines → Create Pipeline**
2. Name: `databricks-insights-pipeline`
3. Source: Point to `pipeline/databricks_insights_pipeline.py` in your repo
4. Target: `observability.databricks_insights`
5. Create and run initial update

### Step 5: Deploy App

**Option A: Using Asset Bundles**
```bash
databricks bundle deploy
```

**Option B: Using CLI**
```bash
databricks apps deploy databricks-insights-app \
  --source-code-path ./app/
```

**Option C: Manual UI**
1. Navigate to **Apps → Create App**
2. Upload the `app/` directory
3. Configure resources (SQL warehouse, AI endpoint)

### Step 6: (Optional) Create AI Endpoint

1. Navigate to **Serving → Create Serving Endpoint**
2. Name: `databricks-insights-ai-endpoint`
3. Select Foundation Model (e.g., `databricks-meta-llama-3-3-70b-instruct`)
4. Enable AI Gateway
5. Create endpoint

### Step 7: Access the App

Navigate to: `https://<your-workspace-url>/apps/databricks-insights-app`

## Verification

After deployment, verify:

1. **Catalog and Schema:**
   ```sql
   SHOW SCHEMAS IN observability;
   SHOW TABLES IN observability.databricks_insights;
   ```

2. **Views:**
   ```sql
   SHOW VIEWS IN observability.databricks_insights;
   ```

3. **Jobs:**
   - Check **Workflows → Jobs** for:
     - `databricks-insights-refresh`
     - `databricks-insights-zombie-cleanup`

4. **Pipeline:**
   - Check **Workflows → Lakeflow Pipelines** for:
     - `databricks-insights-pipeline`

5. **App:**
   - Check **Apps** for:
     - `databricks-insights-app`

## Troubleshooting

### System Tables Empty
- Ensure System Tables are enabled at account level
- Run `sql/setup.sql` as metastore admin
- Verify grants: `SHOW GRANTS ON SCHEMA system.billing;`

### App Connection Error
- Verify warehouse ID in `app/app.yaml`
- Ensure warehouse is running
- Check app logs in **Apps → databricks-insights-app → Logs**

### Pipeline Fails
- Grant CREATE TABLE permission to pipeline service principal:
  ```sql
  GRANT CREATE TABLE ON SCHEMA observability.databricks_insights TO `<pipeline-sp>`;
  ```

### Views Not Created
- Check error messages in deployment notebook
- Verify system table access
- Run view SQL files manually if needed

## Next Steps

- Configure alerts (see `phase5_alerts/` in implementation guide)
- Set up notification destinations
- Customize dashboards in the app
- Configure AI endpoint for natural language queries
