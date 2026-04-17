# -*- coding: utf-8 -*-
# mypy: ignore-errors
import os

import flask
import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, callback, dcc, html
from databricks import sql

from posit.connect.external.databricks import (
    ConnectStrategy,
    databricks_config,
    sql_credentials,
)

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_HOST_URL = f"https://{DATABRICKS_HOST}"
SQL_HTTP_PATH = os.getenv("DATABRICKS_PATH", "")

QUERY = """
    SELECT
        t.dateTime,
        t.product,
        t.quantity,
        t.totalPrice,
        c.continent,
        c.country,
        f.name AS franchise_name
    FROM samples.bakehouse.sales_transactions t
    JOIN samples.bakehouse.sales_customers c
        ON t.customerID = c.customerID
    JOIN samples.bakehouse.sales_franchises f
        ON t.franchiseID = f.franchiseID
"""

app = Dash(__name__)

app.layout = html.Div(
    [
        html.H1("Bakehouse Franchise Dashboard", style={"textAlign": "center"}),
        html.Div(id="greeting", style={"textAlign": "center", "marginBottom": "10px"}),
        html.Div(
            [
                html.Div(
                    [
                        html.Label("Continent"),
                        dcc.Dropdown(id="continent-filter", value="All"),
                    ],
                    style={"width": "30%", "display": "inline-block", "padding": "10px"},
                ),
                html.Div(
                    [
                        html.Label("Franchise"),
                        dcc.Dropdown(id="franchise-filter", value="All"),
                    ],
                    style={"width": "30%", "display": "inline-block", "padding": "10px"},
                ),
            ],
            style={"textAlign": "center"},
        ),
        html.Div(
            id="value-boxes",
            style={
                "display": "flex",
                "justifyContent": "center",
                "gap": "20px",
                "margin": "20px 0",
            },
        ),
        html.Div(
            [
                html.Div(
                    dcc.Graph(id="chart-franchise"),
                    style={"width": "50%", "display": "inline-block"},
                ),
                html.Div(
                    dcc.Graph(id="chart-continent"),
                    style={"width": "50%", "display": "inline-block"},
                ),
            ]
        ),
        html.Div(
            [
                html.Div(
                    dcc.Graph(id="chart-products"),
                    style={"width": "50%", "display": "inline-block"},
                ),
                html.Div(
                    dcc.Graph(id="chart-trend"),
                    style={"width": "50%", "display": "inline-block"},
                ),
            ]
        ),
        dcc.Store(id="raw-data"),
    ]
)


def value_box(title, value, color):
    return html.Div(
        [
            html.Div(title, style={"fontSize": "0.85rem", "color": "#6c757d"}),
            html.Div(value, style={"fontSize": "1.5rem", "fontWeight": "bold"}),
        ],
        style={
            "backgroundColor": color,
            "color": "white",
            "padding": "15px 25px",
            "borderRadius": "8px",
            "textAlign": "center",
            "minWidth": "150px",
        },
    )


@callback(
    Output("raw-data", "data"),
    Output("continent-filter", "options"),
    Output("franchise-filter", "options"),
    Output("greeting", "children"),
    Input("continent-filter", "value"),  # triggers on first load
)
def load_data(_):
    session_token = flask.request.headers.get("Posit-Connect-User-Session-Token")
    cfg = databricks_config(
        posit_connect_strategy=ConnectStrategy(user_session_token=session_token),
    )

    with sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=SQL_HTTP_PATH,
        credentials_provider=sql_credentials(cfg),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(QUERY)
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(rows, columns=cols)
    df.columns = df.columns.str.lower()

    continents = ["All"] + sorted(df["continent"].dropna().unique().tolist())
    franchises = ["All"] + sorted(df["franchise_name"].dropna().unique().tolist())

    return (
        df.to_json(date_format="iso", orient="split"),
        [{"label": c, "value": c} for c in continents],
        [{"label": f, "value": f} for f in franchises],
        f"Loaded {len(df)} rows from Databricks",
    )


def get_filtered(raw_json, continent, franchise):
    if raw_json is None:
        return pd.DataFrame()
    df = pd.read_json(raw_json, orient="split")
    if continent and continent != "All":
        df = df[df["continent"] == continent]
    if franchise and franchise != "All":
        df = df[df["franchise_name"] == franchise]
    return df


@callback(
    Output("value-boxes", "children"),
    Input("raw-data", "data"),
    Input("continent-filter", "value"),
    Input("franchise-filter", "value"),
)
def update_value_boxes(raw_json, continent, franchise):
    df = get_filtered(raw_json, continent, franchise)
    if df.empty:
        return [
            value_box("Total Revenue", "--", "#0d6efd"),
            value_box("Total Orders", "--", "#0dcaf0"),
            value_box("Avg Order", "--", "#198754"),
            value_box("Franchises", "--", "#ffc107"),
        ]
    return [
        value_box("Total Revenue", f"${df['totalprice'].sum():,.2f}", "#0d6efd"),
        value_box("Total Orders", f"{len(df):,}", "#0dcaf0"),
        value_box("Avg Order", f"${df['totalprice'].mean():,.2f}", "#198754"),
        value_box("Franchises", str(df["franchise_name"].nunique()), "#ffc107"),
    ]


@callback(
    Output("chart-franchise", "figure"),
    Input("raw-data", "data"),
    Input("continent-filter", "value"),
    Input("franchise-filter", "value"),
)
def update_franchise_chart(raw_json, continent, franchise):
    df = get_filtered(raw_json, continent, franchise)
    if df.empty:
        return px.bar(title="Revenue by Franchise")
    agg = (
        df.groupby("franchise_name", as_index=False)["totalprice"]
        .sum()
        .sort_values("totalprice", ascending=True)
    )
    return px.bar(
        agg,
        x="totalprice",
        y="franchise_name",
        orientation="h",
        title="Revenue by Franchise",
        labels={"totalprice": "Revenue ($)", "franchise_name": "Franchise"},
    )


@callback(
    Output("chart-continent", "figure"),
    Input("raw-data", "data"),
    Input("continent-filter", "value"),
    Input("franchise-filter", "value"),
)
def update_continent_chart(raw_json, continent, franchise):
    df = get_filtered(raw_json, continent, franchise)
    if df.empty:
        return px.pie(title="Revenue by Continent")
    agg = df.groupby("continent", as_index=False)["totalprice"].sum()
    return px.pie(
        agg,
        names="continent",
        values="totalprice",
        title="Revenue by Continent",
    )


@callback(
    Output("chart-products", "figure"),
    Input("raw-data", "data"),
    Input("continent-filter", "value"),
    Input("franchise-filter", "value"),
)
def update_products_chart(raw_json, continent, franchise):
    df = get_filtered(raw_json, continent, franchise)
    if df.empty:
        return px.bar(title="Top Products by Revenue")
    agg = (
        df.groupby("product", as_index=False)["totalprice"]
        .sum()
        .sort_values("totalprice", ascending=False)
        .head(15)
    )
    return px.bar(
        agg,
        x="product",
        y="totalprice",
        title="Top Products by Revenue",
        labels={"totalprice": "Revenue ($)", "product": "Product"},
    )


@callback(
    Output("chart-trend", "figure"),
    Input("raw-data", "data"),
    Input("continent-filter", "value"),
    Input("franchise-filter", "value"),
)
def update_trend_chart(raw_json, continent, franchise):
    df = get_filtered(raw_json, continent, franchise)
    if df.empty:
        return px.line(title="Monthly Revenue Trend")
    df["month"] = pd.to_datetime(df["datetime"]).dt.to_period("M").astype(str)
    agg = df.groupby("month", as_index=False)["totalprice"].sum().sort_values("month")
    return px.line(
        agg,
        x="month",
        y="totalprice",
        markers=True,
        title="Monthly Revenue Trend",
        labels={"totalprice": "Revenue ($)", "month": "Month"},
    )


if __name__ == "__main__":
    app.run(debug=True)
