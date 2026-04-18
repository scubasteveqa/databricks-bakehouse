"""
Shiny for Python - Bakehouse Franchise Dashboard
=================================================
Connects to Databricks via databricks-sql-connector and queries
the samples.bakehouse dataset using OAuth credentials from
a Posit Connect integration.

Environment variables:
    DATABRICKS_HOST      - e.g. <placeholder>.cloud.databricks.com
    DATABRICKS_HTTP_PATH - e.g. /sql/1.0/warehouses/abc123
"""

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from posit import connect
from shiny import App, Inputs, Outputs, Session, reactive, render, ui
from shinywidgets import output_widget, render_widget


def get_connection(access_token: str):
    from databricks import sql as dbsql

    return dbsql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_PATH"],
        access_token=access_token,
    )


def run_query(access_token: str, sql: str) -> pd.DataFrame:
    conn = get_connection(access_token)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


app_ui = ui.page_sidebar(
    ui.sidebar(
        ui.tags.div(
            ui.tags.h3("Bakehouse Dashboard"),
            ui.tags.p(
                "Franchise sales analytics powered by ",
                ui.tags.code("samples.bakehouse"),
                style="color: #6c757d; font-size: 0.85rem; margin: 0;",
            ),
        ),
        ui.input_action_button("load_data", "Refresh Data", class_="btn-primary w-100"),
        ui.tags.script(
            "setTimeout(function() { document.getElementById('load_data').click(); }, 500);"
        ),
        ui.input_select(
            "continent",
            "Continent",
            choices=["All"],
            selected="All",
        ),
        ui.input_select(
            "franchise",
            "Franchise",
            choices=["All"],
            selected="All",
        ),
        width=260,
    ),
    ui.layout_columns(
        ui.value_box("Total Revenue", ui.output_text("total_revenue"), theme="primary"),
        ui.value_box("Total Orders", ui.output_text("total_orders"), theme="info"),
        ui.value_box("Avg Order Value", ui.output_text("avg_order"), theme="success"),
        ui.value_box("Franchises", ui.output_text("franchise_count"), theme="warning"),
        col_widths=[3, 3, 3, 3],
    ),
    ui.layout_columns(
        ui.card(
            ui.card_header("Revenue by Franchise"),
            output_widget("chart_franchise_revenue"),
        ),
        ui.card(
            ui.card_header("Revenue by Continent"),
            output_widget("chart_continent"),
        ),
        col_widths=[6, 6],
    ),
    ui.layout_columns(
        ui.card(
            ui.card_header("Top Products by Revenue"),
            output_widget("chart_products"),
        ),
        ui.card(
            ui.card_header("Monthly Revenue Trend"),
            output_widget("chart_trend"),
        ),
        col_widths=[6, 6],
    ),
    ui.card(
        ui.card_header("Transaction Data"),
        ui.output_data_frame("sales_table"),
    ),
    title="Bakehouse Franchise Dashboard",
)


def server(i: Inputs, o: Outputs, session: Session):
    data = reactive.Value(None)

    @reactive.effect
    @reactive.event(i.load_data)
    def fetch_data():
        try:
            session_token = session.http_conn.headers.get(
                "Posit-Connect-User-Session-Token"
            )
            if not session_token:
                ui.notification_show(
                    "No session token found. Deploy this app on Posit Connect.",
                    type="error",
                )
                return

            client = connect.Client()
            credentials = client.oauth.get_credentials(session_token)
            access_token = credentials["access_token"]

            df = run_query(
                access_token,
                """
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
                """,
            )

            df.columns = df.columns.str.lower()
            df["datetime"] = pd.to_datetime(df["datetime"])
            df["month"] = df["datetime"].dt.to_period("M").astype(str)
            data.set(df)

            # Update filter choices
            continents = ["All"] + sorted(df["continent"].dropna().unique().tolist())
            franchises = ["All"] + sorted(df["franchise_name"].dropna().unique().tolist())
            ui.update_select("continent", choices=continents, selected="All")
            ui.update_select("franchise", choices=franchises, selected="All")

            ui.notification_show(
                f"Loaded {len(df)} rows from Databricks", type="message"
            )
        except Exception as e:
            ui.notification_show(
                f"Error: {type(e).__name__}: {e}", type="error", duration=10
            )

    @reactive.calc
    def filtered_data():
        df = data()
        if df is None:
            return None
        if i.continent() != "All":
            df = df[df["continent"] == i.continent()]
        if i.franchise() != "All":
            df = df[df["franchise_name"] == i.franchise()]
        return df

    @render.text
    def total_revenue():
        df = filtered_data()
        if df is None:
            return "--"
        return f"${df['totalprice'].sum():,.2f}"

    @render.text
    def total_orders():
        df = filtered_data()
        if df is None:
            return "--"
        return f"{len(df):,}"

    @render.text
    def avg_order():
        df = filtered_data()
        if df is None or len(df) == 0:
            return "--"
        return f"${df['totalprice'].mean():,.2f}"

    @render.text
    def franchise_count():
        df = filtered_data()
        if df is None:
            return "--"
        return str(df["franchise_name"].nunique())

    @render_widget
    def chart_franchise_revenue():
        df = filtered_data()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading...", template="plotly_white", height=400)
            return fig
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
            labels={"totalprice": "Revenue ($)", "franchise_name": "Franchise"},
        )

    @render_widget
    def chart_continent():
        df = filtered_data()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading...", template="plotly_white", height=400)
            return fig
        agg = df.groupby("continent", as_index=False)["totalprice"].sum()
        return px.pie(
            agg,
            names="continent",
            values="totalprice",
            labels={"totalprice": "Revenue ($)", "continent": "Continent"},
        )

    @render_widget
    def chart_products():
        df = filtered_data()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading...", template="plotly_white", height=400)
            return fig
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
            labels={"totalprice": "Revenue ($)", "product": "Product"},
        )

    @render_widget
    def chart_trend():
        df = filtered_data()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading...", template="plotly_white", height=400)
            return fig
        agg = df.groupby("month", as_index=False)["totalprice"].sum()
        return px.line(
            agg,
            x="month",
            y="totalprice",
            markers=True,
            labels={"totalprice": "Revenue ($)", "month": "Month"},
        )

    @render.data_frame
    def sales_table():
        df = filtered_data()
        if df is None:
            return render.DataGrid(pd.DataFrame())
        display = df[
            ["datetime", "franchise_name", "product", "quantity", "totalprice", "continent", "country"]
        ].copy()
        display["datetime"] = display["datetime"].dt.strftime("%Y-%m-%d %H:%M")
        display.columns = ["Date", "Franchise", "Product", "Qty", "Total", "Continent", "Country"]
        return render.DataGrid(display, filters=True)


app = App(app_ui, server)
