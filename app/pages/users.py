import dash
from dash import html, dcc, callback, Output, Input
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from utils.db_connector import run_query
from utils.config import SCHEMA_PATH

dash.register_page(__name__, path="/users", name="Users")


def layout():
    return dbc.Container([
        html.H3("User Activity Overview", className="mt-3 mb-4"),

        # KPI Row
        dbc.Row(id="users-kpis", className="mb-4"),

        # Top users by cost
        dbc.Row([
            dbc.Col(dbc.Card([
                dbc.CardHeader("Top Users by Cost (Last 30 Days)"),
                dbc.CardBody(dcc.Graph(id="top-users-cost-chart")),
            ]), width=8),
            dbc.Col(dbc.Card([
                dbc.CardHeader("User Activity Summary"),
                dbc.CardBody(id="user-activity-table"),
            ]), width=4),
        ], className="mb-4"),

        dcc.Interval(id="users-refresh", interval=300_000, n_intervals=0),
    ])


@callback(
    Output("users-kpis", "children"),
    Output("top-users-cost-chart", "figure"),
    Output("user-activity-table", "children"),
    Input("users-refresh", "n_intervals"),
)
def update_users_panel(_):
    # Get user stats
    stats = run_query("""
        SELECT 
          COUNT(DISTINCT user_email) AS total_users,
          SUM(login_count_30d) AS total_logins,
          SUM(estimated_cost_30d) AS total_user_cost
        FROM {SCHEMA_PATH}.gold_user_activity
    """)

    kpis = dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Active Users (30d)", className="text-muted"),
            html.H3(f"{stats[0]['total_users'] or 0}")
        ])), width=4),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Total Logins (30d)", className="text-muted"),
            html.H3(f"{stats[0]['total_logins'] or 0}")
        ])), width=4),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("User Cost (30d)", className="text-muted"),
            html.H3(f"${stats[0]['total_user_cost'] or 0:,.2f}", className="text-info")
        ])), width=4),
    ])

    # Top users by cost chart
    top_users = run_query("""
        SELECT 
          user_email,
          estimated_cost_30d,
          login_count_30d,
          active_days
        FROM {SCHEMA_PATH}.gold_user_activity
        WHERE estimated_cost_30d IS NOT NULL
        ORDER BY estimated_cost_30d DESC
        LIMIT 20
    """)
    top_users_df = pd.DataFrame(top_users)
    if not top_users_df.empty:
        users_fig = px.bar(
            top_users_df,
            x="user_email",
            y="estimated_cost_30d",
            template="plotly_dark",
            labels={"estimated_cost_30d": "Cost (USD)", "user_email": "User"}
        )
        users_fig.update_layout(height=400, xaxis_tickangle=-45)
    else:
        users_fig = px.bar(template="plotly_dark")

    # User activity table
    activity = run_query("""
        SELECT 
          user_email,
          login_count_30d,
          active_days,
          last_login,
          estimated_cost_30d
        FROM {SCHEMA_PATH}.gold_user_activity
        ORDER BY estimated_cost_30d DESC NULLS LAST
        LIMIT 25
    """)
    activity_table = dbc.Table.from_dataframe(
        pd.DataFrame(activity),
        striped=True, bordered=True, hover=True, dark=True, responsive=True,
    ) if activity else html.P("No user activity data available")

    return kpis, users_fig, activity_table
