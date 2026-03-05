import dash
from dash import html, dcc, callback, Output, Input
import dash_bootstrap_components as dbc
import pandas as pd
from utils.db_connector import run_query
from utils.config import SCHEMA_PATH

dash.register_page(__name__, path="/governance", name="Governance")


def layout():
    return dbc.Container([
        html.H3("Security & Governance", className="mt-3 mb-4"),

        # KPI Row
        dbc.Row(id="governance-kpis", className="mb-4"),

        # Security anomalies
        dbc.Row([
            dbc.Col(dbc.Card([
                dbc.CardHeader("Security Anomalies (Last 24 Hours)"),
                dbc.CardBody(id="security-anomalies-table"),
            ]), width=12),
        ], className="mb-4"),

        # Governance posture
        dbc.Row([
            dbc.Col(dbc.Card([
                dbc.CardHeader("Governance Posture Indicators"),
                dbc.CardBody(id="governance-posture-table"),
            ]), width=12),
        ]),

        dcc.Interval(id="governance-refresh", interval=300_000, n_intervals=0),
    ])


@callback(
    Output("governance-kpis", "children"),
    Output("security-anomalies-table", "children"),
    Output("governance-posture-table", "children"),
    Input("governance-refresh", "n_intervals"),
)
def update_governance_panel(_):
    # Get governance posture
    posture = run_query("""
        SELECT 
          SUM(permission_changes_24h) AS total_perm_changes,
          SUM(ip_denials_24h) AS total_ip_denials,
          SUM(destructive_ops_24h) AS total_destructive,
          SUM(admin_group_changes_24h) AS total_admin_changes
        FROM {SCHEMA_PATH}.gold_governance_posture
    """)

    # Get anomaly count
    anomalies = run_query("""
        SELECT COUNT(*) AS cnt FROM {SCHEMA_PATH}.v_security_anomalies
    """)

    kpis = dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Security Anomalies (24h)", className="text-muted"),
            html.H3(
                f"{anomalies[0]['cnt']}",
                className="text-danger" if anomalies[0]['cnt'] > 0 else "text-success"
            ),
        ])), width=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Permission Changes (24h)", className="text-muted"),
            html.H3(f"{posture[0]['total_perm_changes'] or 0}")
        ])), width=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("IP Access Denials (24h)", className="text-muted"),
            html.H3(
                f"{posture[0]['total_ip_denials'] or 0}",
                className="text-warning" if posture[0]['total_ip_denials'] > 0 else "text-success"
            ),
        ])), width=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Destructive Ops (24h)", className="text-muted"),
            html.H3(
                f"{posture[0]['total_destructive'] or 0}",
                className="text-danger" if posture[0]['total_destructive'] > 0 else "text-success"
            ),
        ])), width=3),
    ])

    # Security anomalies table
    anomalies_data = run_query("""
        SELECT * FROM {SCHEMA_PATH}.v_security_anomalies
        ORDER BY event_time DESC
        LIMIT 50
    """)
    anomalies_table = dbc.Table.from_dataframe(
        pd.DataFrame(anomalies_data),
        striped=True, bordered=True, hover=True, dark=True, responsive=True,
    ) if anomalies_data else html.P("No security anomalies detected.", className="text-success")

    # Governance posture table
    posture_data = run_query("""
        SELECT 
          workspace_id,
          permission_changes_24h,
          ip_denials_24h,
          destructive_ops_24h,
          admin_group_changes_24h
        FROM {SCHEMA_PATH}.gold_governance_posture
    """)
    posture_table = dbc.Table.from_dataframe(
        pd.DataFrame(posture_data),
        striped=True, bordered=True, hover=True, dark=True, responsive=True,
    ) if posture_data else html.P("No governance data available")

    return kpis, anomalies_table, posture_table
