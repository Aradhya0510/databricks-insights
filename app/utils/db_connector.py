import os
from databricks import sql as dbsql


def get_connection():
    """
    Create a connection to the SQL warehouse.
    Credentials are automatically injected by Databricks Apps.
    """
    return dbsql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
        # Auth is handled automatically by the app's service principal
        credentials_provider=lambda: {
            "Authorization": f"Bearer {os.environ.get('DATABRICKS_TOKEN', '')}"
        },
    )


def run_query(query: str, params: dict = None) -> list:
    """Execute a query and return results as list of dicts."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
