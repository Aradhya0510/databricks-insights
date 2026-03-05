"""
Sync configuration from deploy/config.py to app/app.yaml
This ensures the app uses the same configuration as the deployment
"""
import os
import yaml
import sys

# Add parent directory to path to import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from deploy.config import WAREHOUSE_ID, AI_ENDPOINT_NAME, APP_NAME
except ImportError:
    print("Error: Could not import from deploy/config.py")
    print("Make sure deploy/config.py exists and has the required variables")
    sys.exit(1)

# Read current app.yaml
app_yaml_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "app", "app.yaml")

try:
    with open(app_yaml_path, 'r') as f:
        app_config = yaml.safe_load(f)
except FileNotFoundError:
    print(f"Error: {app_yaml_path} not found")
    sys.exit(1)

# Update warehouse ID in resources
if 'resources' in app_config:
    for resource in app_config['resources']:
        if 'sql_warehouse' in resource:
            resource['sql_warehouse']['id'] = WAREHOUSE_ID
            print(f"✓ Updated warehouse ID to: {WAREHOUSE_ID}")

# Update serving endpoint if configured
if AI_ENDPOINT_NAME:
    # Check if serving endpoint resource exists
    serving_endpoint_exists = any(
        'serving_endpoint' in r for r in app_config.get('resources', [])
    )
    
    if not serving_endpoint_exists:
        # Add serving endpoint resource
        if 'resources' not in app_config:
            app_config['resources'] = []
        
        app_config['resources'].append({
            'name': 'serving-endpoint',
            'type': 'serving_endpoint',
            'serving_endpoint': {
                'name': AI_ENDPOINT_NAME,
                'permission': 'CAN_QUERY'
            }
        })
        print(f"✓ Added serving endpoint resource: {AI_ENDPOINT_NAME}")
    else:
        # Update existing serving endpoint
        for resource in app_config['resources']:
            if 'serving_endpoint' in resource:
                resource['serving_endpoint']['name'] = AI_ENDPOINT_NAME
                print(f"✓ Updated serving endpoint name to: {AI_ENDPOINT_NAME}")
else:
    # Remove serving endpoint if not configured
    if 'resources' in app_config:
        app_config['resources'] = [
            r for r in app_config['resources']
            if 'serving_endpoint' not in r
        ]
        print("✓ Removed serving endpoint resource (AI_ENDPOINT_NAME not set)")

# Write updated app.yaml
with open(app_yaml_path, 'w') as f:
    yaml.dump(app_config, f, default_flow_style=False, sort_keys=False)
    print(f"✓ Updated {app_yaml_path}")

print("\n✓ App configuration synced from deploy/config.py")
print(f"  Warehouse ID: {WAREHOUSE_ID}")
print(f"  AI Endpoint: {AI_ENDPOINT_NAME or 'Not configured'}")
