import dash
from dash import html, dcc, callback, Output, Input
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from utils.db_connector import run_query
from utils.config import SCHEMA_PATH

dash.register_page(__name__, path="/", name="Cost & Compute")


def layout():
    return dbc.Container([
        html.H3("Cost & Compute Overview", className="mt-3 mb-4"),

        # KPI Cards Row
        dbc.Row(id="cost-kpi-cards", className="mb-4"),

        # Burn rate chart
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Hourly Cost Burn Rate (Last 7 Days)"),
                    dbc.CardBody(dcc.Graph(id="burn-rate-chart")),
                ])
            ], width=8),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Cost by Product (30d)"),
                    dbc.CardBody(dcc.Graph(id="cost-by-product-chart")),
                ])
            ], width=4),
        ], className="mb-4"),

        # Top cost drivers table
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Top 20 Cost Drivers (Last 30 Days)"),
                    dbc.CardBody(id="cost-drivers-table"),
                ])
            ], width=12),
        ], className="mb-4"),

        # Zombie clusters
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.Span([
                        "Zombie Clusters ",
                        dbc.Badge("Action Required", color="danger", className="ms-2")
                    ])),
                    dbc.CardBody(id="zombie-clusters-table"),
                ])
            ], width=12),
        ]),

        # Auto-refresh every 5 minutes
        dcc.Interval(id="cost-refresh", interval=300_000, n_intervals=0),
    ])


@callback(
    Output("cost-kpi-cards", "children"),
    Output("burn-rate-chart", "figure"),
    Output("cost-by-product-chart", "figure"),
    Output("cost-drivers-table", "children"),
    Output("zombie-clusters-table", "children"),
    Input("cost-refresh", "n_intervals"),
)
def update_cost_panel(_):
    # KPI queries
    today_cost = run_query("""
        SELECT SUM(u.usage_quantity * lp.pricing.effective_list.default) AS cost
        FROM system.billing.usage u
        JOIN system.billing.list_prices lp
          ON lp.sku_name = u.sku_name
          AND u.usage_end_time >= lp.price_start_time
          AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
        WHERE u.usage_date = CURRENT_DATE
    """)

    mtd_cost = run_query("""
        SELECT SUM(u.usage_quantity * lp.pricing.effective_list.default) AS cost
        FROM system.billing.usage u
        JOIN system.billing.list_prices lp
          ON lp.sku_name = u.sku_name
          AND u.usage_end_time >= lp.price_start_time
          AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
        WHERE u.usage_date >= DATE_TRUNC('MONTH', CURRENT_DATE)
    """)

    active_clusters = run_query(f"""
        SELECT COUNT(DISTINCT cluster_id) AS cnt
        FROM {SCHEMA_PATH}.gold_compute_inventory
        WHERE health_status = 'ACTIVE'
    """)

    zombie_count = run_query(f"""
        SELECT COUNT(DISTINCT cluster_id) AS cnt
        FROM {SCHEMA_PATH}.gold_compute_inventory
        WHERE health_status = 'IDLE'
    """)

    kpi_cards = dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H6("Today's Spend", className="text-muted"),
                html.H3(f"${today_cost[0]['cost'] or 0:,.2f}", className="text-success"),
            ])
        ]), width=3),
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H6("Month-to-Date", className="text-muted"),
                html.H3(f"${mtd_cost[0]['cost'] or 0:,.2f}", className="text-info"),
            ])
        ]), width=3),
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H6("Active Clusters", className="text-muted"),
                html.H3(f"{active_clusters[0]['cnt']}", className="text-warning"),
            ])
        ]), width=3),
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H6("Zombie Clusters", className="text-muted"),
                html.H3(
                    f"{zombie_count[0]['cnt']}",
                    className="text-danger" if zombie_count[0]['cnt'] > 0 else "text-success"
                ),
            ])
        ]), width=3),
    ])

    # Burn rate chart
    burn_data = run_query(f"""
        SELECT hour, cost_usd, rolling_24h_avg
        FROM {SCHEMA_PATH}.v_realtime_burn_rate
        ORDER BY hour
    """)
    burn_df = pd.DataFrame(burn_data)
    burn_fig = go.Figure()
    if not burn_df.empty:
        burn_fig.add_trace(go.Bar(x=burn_df["hour"], y=burn_df["cost_usd"],
                                   name="Hourly Cost", opacity=0.6))
        burn_fig.add_trace(go.Scatter(x=burn_df["hour"], y=burn_df["rolling_24h_avg"],
                                       name="24h Rolling Avg", line=dict(color="red", width=2)))
    burn_fig.update_layout(template="plotly_dark", height=400, margin=dict(l=40, r=20, t=20, b=40))

    # Cost by product
    product_data = run_query(f"""
        SELECT product, SUM(estimated_cost_usd) AS cost
        FROM {SCHEMA_PATH}.gold_cost_daily
        WHERE date >= CURRENT_DATE - INTERVAL 30 DAYS
        GROUP BY product
        ORDER BY cost DESC
    """)
    product_df = pd.DataFrame(product_data)
    product_fig = px.pie(product_df, values="cost", names="product",
                         template="plotly_dark", hole=0.4)
    product_fig.update_layout(height=400, margin=dict(l=20, r=20, t=20, b=20))

    # Top cost drivers
    drivers = run_query(f"SELECT * FROM {SCHEMA_PATH}.v_top_cost_drivers LIMIT 20")
    drivers_table = dbc.Table.from_dataframe(
        pd.DataFrame(drivers),
        striped=True, bordered=True, hover=True, dark=True, responsive=True,
    ) if drivers else html.P("No data available")

    # Zombie clusters
    zombies = run_query(f"""
        SELECT cluster_id, cluster_name, owner, dbus_last_7d, last_active_time
        FROM {SCHEMA_PATH}.gold_compute_inventory
        WHERE health_status = 'IDLE'
    """)
    zombies_table = dbc.Table.from_dataframe(
        pd.DataFrame(zombies),
        striped=True, bordered=True, hover=True, dark=True, responsive=True,
    ) if zombies else html.P("No zombie clusters detected.", className="text-success")

    return kpi_cards, burn_fig, product_fig, drivers_table, zombies_table
