# Deploying Databricks Insights App

## Option 1: Configure CLI for Your Workspace (Recommended)

### Step 1: Create a new CLI profile for your workspace

```bash
# Create a new profile called "fevm"
databricks configure --profile fevm --token

# When prompted, enter:
# - Databricks Host: https://fevm-serverless-stable-7xmub3.cloud.databricks.com
# - Token: [your personal access token]
```

### Step 2: Deploy using the profile

```bash
# Sync files to workspace
databricks sync --profile fevm --watch . /Workspace/Users/aradhya.chouhan@databricks.com/databricks-insights

# Deploy the app
databricks apps deploy --profile fevm databricks-insights-app \
  --source-code-path /Workspace/Users/aradhya.chouhan@databricks.com/databricks-insights/app
```

## Option 2: Use Environment Variables (Quick Fix)

Set environment variables to override the default profile:

```bash
export DATABRICKS_HOST="https://fevm-serverless-stable-7xmub3.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"

# Then deploy
cd /Users/aradhya.chouhan/Workspace/databricks_insights
databricks sync --watch . /Workspace/Users/aradhya.chouhan@databricks.com/databricks-insights
databricks apps deploy databricks-insights-app \
  --source-code-path /Workspace/Users/aradhya.chouhan@databricks.com/databricks-insights/app
```

## Option 3: Deploy Directly from Workspace (No CLI Needed)

If you've already cloned the repo into your workspace:

1. **Navigate to Apps in your workspace:**
   - Go to: https://fevm-serverless-stable-7xmub3.cloud.databricks.com/apps

2. **Create a new app:**
   - Click "Create App"
   - Name: `databricks-insights-app`
   - Source code path: `/Workspace/Users/aradhya.chouhan@databricks.com/databricks-insights/app`
   - Click "Create"

3. **Configure app resources:**
   - In the app settings, add:
     - SQL Warehouse: Your warehouse ID
     - Serving Endpoint (optional): `databricks-insights-ai-endpoint`

## Option 4: Update Default Profile

If you want to change your default workspace:

```bash
# Reconfigure default profile
databricks configure --token

# Enter:
# - Databricks Host: https://fevm-serverless-stable-7xmub3.cloud.databricks.com
# - Token: [your personal access token]
```

## Getting Your Personal Access Token

1. Go to: https://fevm-serverless-stable-7xmub3.cloud.databricks.com/settings/account
2. Navigate to: Developer → Access Tokens
3. Click "Generate new token"
4. Copy the token (you'll only see it once)

## Troubleshooting

### If sync fails:
- Make sure the target path exists in your workspace
- Check that you have write permissions
- Try without `--watch` flag first: `databricks sync . /Workspace/Users/aradhya.chouhan@databricks.com/databricks-insights`

### If app deploy fails:
- Verify the source code path is correct
- Check that `app/app.yaml` exists
- Ensure you have app deployment permissions (Workspace Admin)

### Check current configuration:
```bash
databricks auth env  # Shows current auth settings
databricks --version  # Shows CLI version
```
