# Phase 5: Automated Zombie Cluster Termination
# Notebook: databricks_insights_auto_terminate_zombies
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Query zombie clusters
zombies = spark.sql("""
    SELECT cluster_id, cluster_name, owner
    FROM observability.databricks_insights.gold_compute_inventory
    WHERE health_status = 'ZOMBIE'
""").collect()

terminated = []
for z in zombies:
    try:
        w.clusters.delete(cluster_id=z.cluster_id)
        terminated.append(z.cluster_name)
        print(f"Terminated zombie cluster: {z.cluster_name} ({z.cluster_id})")
    except Exception as e:
        print(f"Failed to terminate {z.cluster_name}: {e}")

if terminated:
    # Log to audit table (create this table if it doesn't exist)
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS observability.databricks_insights.termination_log (
                termination_time TIMESTAMP,
                cluster_names ARRAY<STRING>
            )
        """)
        
        spark.sql(f"""
            INSERT INTO observability.databricks_insights.termination_log
            VALUES (CURRENT_TIMESTAMP(), ARRAY({','.join(f"'{t}'" for t in terminated)}))
        """)
    except Exception as e:
        print(f"Failed to log terminations: {e}")
