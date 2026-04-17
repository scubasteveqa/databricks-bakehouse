# -*- coding: utf-8 -*-
# mypy: ignore-errors
import os

import pandas as pd
import plotly.express as px
import streamlit as st
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

st.set_page_config(page_title="Bakehouse Franchise Dashboard", layout="wide")
st.title("Bakehouse Franchise Dashboard")


@st.cache_data(ttl=300)
def load_data(session_token):
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
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["month"] = df["datetime"].dt.to_period("M").astype(str)
    return df


session_token = st.context.headers.get("Posit-Connect-User-Session-Token")

try:
    df = load_data(session_token)
    st.success(f"Loaded {len(df)} rows from Databricks")
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# Sidebar filters
with st.sidebar:
    st.header("Filters")
    continents = ["All"] + sorted(df["continent"].dropna().unique().tolist())
    continent = st.selectbox("Continent", continents)

    franchises = ["All"] + sorted(df["franchise_name"].dropna().unique().tolist())
    franchise = st.selectbox("Franchise", franchises)

# Apply filters
filtered = df.copy()
if continent != "All":
    filtered = filtered[filtered["continent"] == continent]
if franchise != "All":
    filtered = filtered[filtered["franchise_name"] == franchise]

# Value boxes
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Revenue", f"${filtered['totalprice'].sum():,.2f}")
col2.metric("Total Orders", f"{len(filtered):,}")
col3.metric("Avg Order", f"${filtered['totalprice'].mean():,.2f}" if len(filtered) > 0 else "--")
col4.metric("Franchises", str(filtered["franchise_name"].nunique()))

# Charts row 1
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("Revenue by Franchise")
    agg = (
        filtered.groupby("franchise_name", as_index=False)["totalprice"]
        .sum()
        .sort_values("totalprice", ascending=True)
    )
    fig = px.bar(
        agg,
        x="totalprice",
        y="franchise_name",
        orientation="h",
        labels={"totalprice": "Revenue ($)", "franchise_name": "Franchise"},
    )
    st.plotly_chart(fig, use_container_width=True)

with chart_col2:
    st.subheader("Revenue by Continent")
    agg = filtered.groupby("continent", as_index=False)["totalprice"].sum()
    fig = px.pie(agg, names="continent", values="totalprice")
    st.plotly_chart(fig, use_container_width=True)

# Charts row 2
chart_col3, chart_col4 = st.columns(2)

with chart_col3:
    st.subheader("Top Products by Revenue")
    agg = (
        filtered.groupby("product", as_index=False)["totalprice"]
        .sum()
        .sort_values("totalprice", ascending=False)
        .head(15)
    )
    fig = px.bar(
        agg,
        x="product",
        y="totalprice",
        labels={"totalprice": "Revenue ($)", "product": "Product"},
    )
    st.plotly_chart(fig, use_container_width=True)

with chart_col4:
    st.subheader("Monthly Revenue Trend")
    agg = (
        filtered.groupby("month", as_index=False)["totalprice"]
        .sum()
        .sort_values("month")
    )
    fig = px.line(
        agg,
        x="month",
        y="totalprice",
        markers=True,
        labels={"totalprice": "Revenue ($)", "month": "Month"},
    )
    st.plotly_chart(fig, use_container_width=True)
