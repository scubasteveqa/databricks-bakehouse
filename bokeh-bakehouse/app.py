# -*- coding: utf-8 -*-
# mypy: ignore-errors
import os
import math

import pandas as pd
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, Select, Div
from bokeh.palettes import Category10
from bokeh.plotting import curdoc, figure
from bokeh.transform import cumsum
from databricks import sql

from posit.connect.external.databricks import (
    ConnectStrategy,
    databricks_config,
    sql_credentials,
)

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
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

# Get session token from request headers
request = curdoc().session_context.request
session_token = request.headers.get("Posit-Connect-User-Session-Token", "")

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

raw_df = pd.DataFrame(rows, columns=cols)
raw_df.columns = raw_df.columns.str.lower()
raw_df["datetime"] = pd.to_datetime(raw_df["datetime"])
raw_df["month"] = raw_df["datetime"].dt.to_period("M").astype(str)

# Filters
continents = ["All"] + sorted(raw_df["continent"].dropna().unique().tolist())
franchises = ["All"] + sorted(raw_df["franchise_name"].dropna().unique().tolist())

continent_select = Select(title="Continent", value="All", options=continents)
franchise_select = Select(title="Franchise", value="All", options=franchises)

# Value box displays
revenue_div = Div(text="", styles={"font-size": "18px", "font-weight": "bold"})
orders_div = Div(text="", styles={"font-size": "18px", "font-weight": "bold"})
avg_div = Div(text="", styles={"font-size": "18px", "font-weight": "bold"})
franchise_div = Div(text="", styles={"font-size": "18px", "font-weight": "bold"})

# Data sources
franchise_source = ColumnDataSource(data=dict(franchise_name=[], totalprice=[]))
continent_source = ColumnDataSource(
    data=dict(continent=[], totalprice=[], angle=[], color=[])
)
product_source = ColumnDataSource(data=dict(product=[], totalprice=[]))
trend_source = ColumnDataSource(data=dict(month=[], totalprice=[]))

# Franchise bar chart
franchise_fig = figure(
    y_range=[],
    title="Revenue by Franchise",
    x_axis_label="Revenue ($)",
    height=350,
    width=500,
)
franchise_fig.hbar(y="franchise_name", right="totalprice", height=0.7, source=franchise_source)

# Continent pie chart
continent_fig = figure(
    title="Revenue by Continent",
    height=350,
    width=500,
    toolbar_location=None,
    x_range=(-0.5, 1.0),
)
continent_fig.wedge(
    x=0,
    y=1,
    radius=0.4,
    start_angle=cumsum("angle", include_zero=True),
    end_angle=cumsum("angle"),
    line_color="white",
    fill_color="color",
    legend_field="continent",
    source=continent_source,
)
continent_fig.axis.axis_label = None
continent_fig.axis.visible = False
continent_fig.grid.grid_line_color = None

# Products bar chart
product_fig = figure(
    x_range=[],
    title="Top Products by Revenue",
    y_axis_label="Revenue ($)",
    height=350,
    width=500,
)
product_fig.vbar(x="product", top="totalprice", width=0.7, source=product_source)
product_fig.xaxis.major_label_orientation = 0.8

# Monthly trend
trend_fig = figure(
    x_range=[],
    title="Monthly Revenue Trend",
    y_axis_label="Revenue ($)",
    height=350,
    width=500,
)
trend_fig.line(x="month", y="totalprice", line_width=2, source=trend_source)
trend_fig.scatter(x="month", y="totalprice", size=6, source=trend_source)
trend_fig.xaxis.major_label_orientation = 0.8


def update(attr, old, new):
    df = raw_df.copy()
    if continent_select.value != "All":
        df = df[df["continent"] == continent_select.value]
    if franchise_select.value != "All":
        df = df[df["franchise_name"] == franchise_select.value]

    # Value boxes
    total_rev = df["totalprice"].sum()
    total_orders = len(df)
    avg_order = df["totalprice"].mean() if total_orders > 0 else 0
    n_franchises = df["franchise_name"].nunique()

    revenue_div.text = f"Total Revenue: <b>${total_rev:,.2f}</b>"
    orders_div.text = f"Total Orders: <b>{total_orders:,}</b>"
    avg_div.text = f"Avg Order: <b>${avg_order:,.2f}</b>"
    franchise_div.text = f"Franchises: <b>{n_franchises}</b>"

    # Franchise chart
    agg_f = (
        df.groupby("franchise_name", as_index=False)["totalprice"]
        .sum()
        .sort_values("totalprice", ascending=True)
    )
    franchise_source.data = dict(
        franchise_name=agg_f["franchise_name"].tolist(),
        totalprice=agg_f["totalprice"].tolist(),
    )
    franchise_fig.y_range.factors = agg_f["franchise_name"].tolist()

    # Continent pie chart
    agg_c = df.groupby("continent", as_index=False)["totalprice"].sum()
    total = agg_c["totalprice"].sum()
    agg_c["angle"] = agg_c["totalprice"] / total * 2 * math.pi if total > 0 else 0
    n_colors = max(len(agg_c), 3)
    palette = Category10[n_colors] if n_colors <= 10 else Category10[10]
    agg_c["color"] = palette[: len(agg_c)]
    continent_source.data = dict(
        continent=agg_c["continent"].tolist(),
        totalprice=agg_c["totalprice"].tolist(),
        angle=agg_c["angle"].tolist(),
        color=agg_c["color"].tolist(),
    )

    # Products chart
    agg_p = (
        df.groupby("product", as_index=False)["totalprice"]
        .sum()
        .sort_values("totalprice", ascending=False)
        .head(15)
    )
    product_source.data = dict(
        product=agg_p["product"].tolist(),
        totalprice=agg_p["totalprice"].tolist(),
    )
    product_fig.x_range.factors = agg_p["product"].tolist()

    # Trend chart
    agg_t = (
        df.groupby("month", as_index=False)["totalprice"].sum().sort_values("month")
    )
    trend_source.data = dict(
        month=agg_t["month"].tolist(),
        totalprice=agg_t["totalprice"].tolist(),
    )
    trend_fig.x_range.factors = agg_t["month"].tolist()


continent_select.on_change("value", update)
franchise_select.on_change("value", update)

# Initial load
update(None, None, None)

# Layout
filters = row(continent_select, franchise_select)
value_boxes = row(revenue_div, orders_div, avg_div, franchise_div)
charts_row1 = row(franchise_fig, continent_fig)
charts_row2 = row(product_fig, trend_fig)

layout = column(
    Div(text="<h1>Bakehouse Franchise Dashboard</h1>"),
    filters,
    value_boxes,
    charts_row1,
    charts_row2,
)

curdoc().add_root(layout)
curdoc().title = "Bakehouse Franchise Dashboard"
