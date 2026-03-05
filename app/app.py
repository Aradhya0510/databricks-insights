import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
)

# Navigation sidebar
sidebar = dbc.Nav(
    [
        dbc.NavLink(
            [html.I(className="bi bi-currency-dollar me-2"), "Cost & Compute"],
            href="/", active="exact"
        ),
        dbc.NavLink(
            [html.I(className="bi bi-gear me-2"), "Jobs & Pipelines"],
            href="/jobs", active="exact"
        ),
        dbc.NavLink(
            [html.I(className="bi bi-shield-check me-2"), "Governance"],
            href="/governance", active="exact"
        ),
        dbc.NavLink(
            [html.I(className="bi bi-people me-2"), "Users"],
            href="/users", active="exact"
        ),
        dbc.NavLink(
            [html.I(className="bi bi-robot me-2"), "AI Assistant"],
            href="/ai", active="exact"
        ),
    ],
    vertical=True,
    pills=True,
    className="bg-dark",
)

app.layout = dbc.Container(
    [
        dbc.Row([
            dbc.Col([
                html.H2("Databricks Insights", className="text-primary mb-3 mt-3"),
                html.P("Workspace Observability", className="text-muted"),
                html.Hr(),
                sidebar,
            ], width=2, className="bg-dark vh-100 position-fixed"),
            dbc.Col([
                dash.page_container,
            ], width=10, className="ms-auto"),
        ]),
    ],
    fluid=True,
)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
