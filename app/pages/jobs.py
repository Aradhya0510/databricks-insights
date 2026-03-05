import dash
from dash import html, dcc, callback, Output, Input
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from utils.db_connector import run_query
from utils.config import SCHEMA_PATH

dash.register_page(__name__, path="/jobs", name="Jobs & Pipelines")


def layout():
    return dbc.Container([
        html.H3("Jobs & Pipeline Health", className="mt-3 mb-4"),

        # KPI Row
        dbc.Row(id="jobs-kpis", className="mb-4"),

        # Failing jobs table
        dbc.Row([
            dbc.Col(dbc.Card([
                dbc.CardHeader("Currently Failing Jobs"),
                dbc.CardBody(id="failing-jobs-table"),
            ]), width=12),
        ], className="mb-4"),

        # Job success rate heatmap
        dbc.Row([
            dbc.Col(dbc.Card([
                dbc.CardHeader("Job Success Rates (Last 30 Days)"),
                dbc.CardBody(dcc.Graph(id="job-success-chart")),
            ]), width=8),
            dbc.Col(dbc.Card([
                dbc.CardHeader("Pipeline Health (7d)"),
                dbc.CardBody(id="pipeline-health-table"),
            ]), width=4),
        ]),

        dcc.Interval(id="jobs-refresh", interval=300_000, n_intervals=0),
    ])


@callback(
    Output("jobs-kpis", "children"),
    Output("failing-jobs-table", "children"),
    Output("job-success-chart", "figure"),
    Output("pipeline-health-table", "children"),
    Input("jobs-refresh", "n_intervals"),
)
def update_jobs_panel(_):
    # KPIs
    stats = run_query("""
        SELECT
          COUNT(*) AS total_jobs,
          AVG(success_rate_pct) AS avg_success_rate,
          SUM(failure_count) AS total_failures_30d
        FROM {SCHEMA_PATH}.gold_job_health
    """)

    kpis = dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Total Jobs", className="text-muted"),
            html.H3(f"{stats[0]['total_jobs']}")
        ])), width=4),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Avg Success Rate", className="text-muted"),
            html.H3(f"{stats[0]['avg_success_rate']:.1f}%", className="text-success")
        ])), width=4),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("Total Failures (30d)", className="text-muted"),
            html.H3(f"{stats[0]['total_failures_30d']}", className="text-danger")
        ])), width=4),
    ])

    # Failing jobs
    failing = run_query("""
        SELECT * FROM {SCHEMA_PATH}.v_currently_failing_jobs LIMIT 25
    """)
    failing_table = dbc.Table.from_dataframe(
        pd.DataFrame(failing),
        striped=True, bordered=True, hover=True, dark=True, responsive=True,
    ) if failing else html.P("All jobs healthy!", className="text-success")

    # Job success chart — top 30 jobs by run count
    job_data = run_query("""
        SELECT job_name, success_rate_pct, total_runs_30d, failure_count
        FROM {SCHEMA_PATH}.gold_job_health
        ORDER BY total_runs_30d DESC LIMIT 30
    """)
    job_df = pd.DataFrame(job_data)
    job_fig = px.bar(
        job_df, x="job_name", y="success_rate_pct",
        color="success_rate_pct",
        color_continuous_scale=["red", "yellow", "green"],
        range_color=[0, 100],
        template="plotly_dark",
    )
    job_fig.update_layout(height=400, xaxis_tickangle=-45)

    # Pipeline health
    pipelines = run_query("""
        SELECT name, total_updates_7d, success_count, failure_count, success_rate_pct
        FROM {SCHEMA_PATH}.gold_pipeline_health
        ORDER BY failure_count DESC
    """)
    pipeline_table = dbc.Table.from_dataframe(
        pd.DataFrame(pipelines),
        striped=True, bordered=True, hover=True, dark=True, responsive=True,
    ) if pipelines else html.P("No pipeline data")

    return kpis, failing_table, job_fig, pipeline_table
