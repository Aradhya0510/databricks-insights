# Databricks Insights: Workspace Observability Platform

Complete one-shot deployment of the Databricks Insights observability platform for Databricks workspaces.

## Quick Start

### Prerequisites

1. **System Tables Enabled** (Account Admin)
   - Navigate to Account Console → Settings → Feature enablement
   - Enable System Tables

2. **Databricks Apps Enabled** (Workspace Admin)
   - Navigate to Workspace Settings → Previews
   - Enable Databricks Apps

3. **Serverless SQL Warehouse**
   - Create a serverless SQL warehouse in your workspace
   - Note the warehouse ID (you'll need it for configuration)

### Deployment Options

#### Option 1: GitHub Repo + Databricks Repos (Recommended)

1. **Clone the repo into your workspace:**
   - In Databricks, go to Repos
   - Click "Add Repo"
   - Enter your GitHub repo URL
   - Clone the repository

2. **Configure deployment:**
   - Open `deploy/config.py`
   - Update:
     - `WORKSPACE_URL`: Your workspace URL
     - `WAREHOUSE_ID`: Your serverless SQL warehouse ID
     - `ADMIN_GROUP`: Your admin group/service principal name
     - `APP_SERVICE_PRINCIPAL`: Your app service principal name

3. **Run deployment:**
   - Open `deploy/00_deploy.py` notebook
   - Run all cells
   - The deployment will:
     - Create catalog and schema
     - Grant system table access
     - Create SQL views
     - Create jobs
     - Provide instructions for pipeline and app deployment

4. **Complete manual steps:**
   - Create pipeline in UI pointing to `pipeline/databricks_insights_pipeline.py`
   - Deploy app using Asset Bundles (see Option 2) or CLI

#### Option 2: Asset Bundles (Full Automation)

1. **Configure:**
   ```bash
   # Update databricks.yml with your workspace URL
   # Set warehouse_id variable
   ```

2. **Deploy:**
   ```bash
   # Install Databricks CLI
   pip install databricks-cli databricks-bundles
   
   # Configure authentication
   databricks configure --token
   
   # Validate bundle
   databricks bundle validate
   
   # Deploy everything
   databricks bundle deploy
   ```

3. **Run SQL setup:**
   - Execute `sql/setup.sql` as a metastore admin
   - Execute all SQL files in `sql/views/` directory

4. **Access the app:**
   - Navigate to `https://<workspace-url>/apps/databricks-insights-app`

## Project Structure

```
databricks-insights/
├── deploy/
│   ├── 00_deploy.py          # Main deployment notebook
│   └── config.py             # Configuration file
├── sql/
│   ├── setup.sql              # Catalog, schema, and grants
│   └── views/                 # SQL views
│       ├── realtime_burn_rate.sql
│       ├── currently_failing_jobs.sql
│       ├── top_cost_drivers.sql
│       ├── security_anomalies.sql
│       └── query_performance.sql
├── pipeline/
│   └── databricks_insights_pipeline.py  # Lakeflow pipeline
├── app/                       # Databricks App (Dash frontend)
│   ├── app.yaml
│   ├── app.py
│   ├── requirements.txt
│   ├── pages/
│   ├── utils/
│   └── assets/
├── jobs/
│   └── auto_terminate_zombies.py
├── databricks.yml            # Asset Bundle configuration
└── README.md
```

## Features

- **Cost & Compute**: Real-time burn rate, cost by product, zombie cluster detection
- **Jobs & Pipelines**: Job health metrics, failure tracking, pipeline status
- **Governance**: Security anomaly detection, permission change tracking
- **Users**: User activity summary, cost attribution
- **AI Assistant**: Natural language querying of workspace data

## Configuration

### Environment Variables

The app uses the following environment variables (automatically injected by Databricks Apps):
- `DATABRICKS_HOST`: Workspace hostname
- `DATABRICKS_WAREHOUSE_ID`: SQL warehouse ID
- `DATABRICKS_SERVING_ENDPOINT`: AI endpoint URL (optional)
- `DATABRICKS_TOKEN`: Authentication token

### Service Principals

Update these in `deploy/config.py` and `sql/setup.sql`:
- `databricks-insights-admins`: Group or service principal with system table access
- `databricks-insights-app-sp`: Service principal for the Databricks App

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| System tables return empty | Tables not enabled or no grants | Run `sql/setup.sql` as metastore admin |
| App shows "connection error" | SQL warehouse stopped or wrong ID | Check warehouse status; update `app.yaml` |
| Pipeline fails on refresh | Schema permissions | Grant CREATE TABLE to pipeline service principal |
| AI endpoint returns errors | Endpoint not running or rate limited | Check serving endpoint status in UI |

## Documentation

See `databricks_insights-implementation-guide.md` for detailed implementation guide.
