import dash
from dash import html, dcc, callback, Output, Input, State
import dash_bootstrap_components as dbc
import os
import json
import requests
from utils.db_connector import run_query
from utils.config import SCHEMA_PATH

dash.register_page(__name__, path="/ai", name="AI Assistant")

# System prompt with schema context
def get_system_prompt():
    return f"""You are Databricks Insights AI, an expert Databricks workspace analyst.
You help admins understand their workspace costs, job health, security posture,
and user activity.

You have access to the following tables in the `{SCHEMA_PATH}` schema:

1. gold_cost_daily: columns [date, workspace_id, product, sku_name,
   total_dbus, estimated_cost_usd, unique_users]
2. gold_job_health: columns [workspace_id, job_id, job_name,
   total_runs_30d, success_count, failure_count, success_rate_pct,
   avg_duration_seconds, p95_duration_seconds, last_run_time, job_owner]
3. gold_compute_inventory: columns [workspace_id, cluster_id,
   cluster_name, owner, health_status, dbus_last_7d, last_active_time]
4. gold_user_activity: columns [user_email, login_count_30d,
   last_login, active_days, total_dbus_30d, estimated_cost_30d]
5. gold_governance_posture: columns [workspace_id,
   permission_changes_24h, ip_denials_24h, destructive_ops_24h,
   admin_group_changes_24h]
6. gold_pipeline_health: columns [workspace_id, pipeline_id, name,
   total_updates_7d, success_count, failure_count, success_rate_pct]

When asked a question:
1. Generate a SQL query against these tables
2. Return ONLY the SQL wrapped in ```sql``` code blocks
3. Keep queries simple and efficient
"""

SYSTEM_PROMPT = get_system_prompt()


def query_ai_endpoint(user_message: str) -> str:
    """Send a message to the model serving endpoint."""
    from utils.config import AI_ENDPOINT_URL, AI_ENDPOINT_NAME
    
    # Try to get endpoint URL from environment (set by app.yaml) or config
    endpoint_url = os.environ.get("DATABRICKS_SERVING_ENDPOINT") or AI_ENDPOINT_URL
    token = os.environ.get("DATABRICKS_TOKEN")

    if not endpoint_url:
        if AI_ENDPOINT_NAME:
            return f"AI endpoint '{AI_ENDPOINT_NAME}' is configured but not available. The endpoint may need to be created or the app needs to be redeployed with the serving endpoint resource."
        else:
            return "AI endpoint is not configured. Set AI_ENDPOINT_NAME in deploy/config.py and redeploy the app, or set DATABRICKS_SERVING_ENDPOINT environment variable."
    
    if not token:
        return "Error: DATABRICKS_TOKEN not available. This should be automatically provided by Databricks Apps."

    try:
        response = requests.post(
            endpoint_url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_message},
                ],
                "max_tokens": 1024,
                "temperature": 0.1,
            },
        )
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        return f"Error querying AI endpoint: {str(e)}"


def extract_sql(ai_response: str) -> str:
    """Extract SQL from markdown code blocks."""
    if "```sql" in ai_response:
        return ai_response.split("```sql")[1].split("```")[0].strip()
    elif "```" in ai_response:
        return ai_response.split("```")[1].split("```")[0].strip()
    return None


def layout():
    return dbc.Container([
        html.H3("AI Workspace Assistant", className="mt-3 mb-4"),
        html.P("Ask questions about your workspace in natural language.",
               className="text-muted"),

        # Chat history
        html.Div(id="chat-history", className="mb-3",
                 style={"maxHeight": "500px", "overflowY": "auto"}),

        # Input
        dbc.InputGroup([
            dbc.Input(
                id="user-input",
                placeholder="e.g., 'Why did costs spike last Tuesday?'",
                type="text",
            ),
            dbc.Button("Ask", id="send-btn", color="primary"),
        ], className="mb-3"),

        # Results area
        html.Div(id="ai-results"),

        # Store chat history
        dcc.Store(id="chat-store", data=[]),
    ])


@callback(
    Output("chat-history", "children"),
    Output("ai-results", "children"),
    Output("chat-store", "data"),
    Output("user-input", "value"),
    Input("send-btn", "n_clicks"),
    State("user-input", "value"),
    State("chat-store", "data"),
    prevent_initial_call=True,
)
def handle_chat(n_clicks, user_input, chat_history):
    if not user_input:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update

    # Get AI response
    ai_response = query_ai_endpoint(user_input)

    # Try to extract and run SQL
    sql_query = extract_sql(ai_response)
    result_component = html.Div()

    if sql_query:
        try:
            results = run_query(sql_query)
            import pandas as pd
            df = pd.DataFrame(results)
            result_component = html.Div([
                html.H6("Generated SQL:", className="text-muted mt-2"),
                dcc.Markdown(f"```sql\n{sql_query}\n```"),
                html.H6("Results:", className="text-muted mt-2"),
                dbc.Table.from_dataframe(
                    df, striped=True, bordered=True, hover=True,
                    dark=True, responsive=True
                ) if not df.empty else html.P("Query returned no results.")
            ])
        except Exception as e:
            result_component = html.Div([
                html.P(f"Query execution error: {str(e)}", className="text-danger"),
                dcc.Markdown(f"```sql\n{sql_query}\n```"),
            ])
    else:
        result_component = dcc.Markdown(ai_response)

    # Update chat history
    chat_history.append({"role": "user", "content": user_input})
    chat_history.append({"role": "assistant", "content": ai_response})

    chat_display = []
    for msg in chat_history:
        if msg["role"] == "user":
            chat_display.append(
                dbc.Alert(msg["content"], color="primary", className="text-end")
            )
        else:
            chat_display.append(
                dbc.Alert(msg["content"][:200] + "...", color="secondary")
            )

    return chat_display, result_component, chat_history, ""
