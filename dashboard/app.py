from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = Path("data/warehouse.duckdb")

BASE_QUERY = """
select
    f.observation_ts_utc,
    d.date_day,
    l.location_name,
    s.source_name,
    f.temperature_c,
    f.apparent_temperature_c,
    f.precipitation_mm,
    f.wind_speed_kmh
from analytics_gold.fact_observation as f
inner join analytics_gold.dim_date as d
    on f.date_key = d.date_key
inner join analytics_gold.dim_location as l
    on f.location_key = l.location_key
inner join analytics_gold.dim_source as s
    on f.source_key = s.source_key
"""


@st.cache_data(show_spinner=False)
def load_gold_data(db_path: str) -> pd.DataFrame:
    with duckdb.connect(db_path, read_only=True) as conn:
        df = conn.execute(BASE_QUERY).fetch_df()
    df["observation_ts_utc"] = pd.to_datetime(df["observation_ts_utc"], utc=True)
    df["date_day"] = pd.to_datetime(df["date_day"]).dt.date
    return df


st.set_page_config(page_title="Mini Cloud Data Platform Demo", layout="wide")
st.title("Mini Cloud Data Platform Demo")
st.caption("Gold-layer weather observations (DuckDB + dbt)")

if not DB_PATH.exists():
    st.error("DuckDB warehouse not found. Run `make ingest-local && make dbt-run` first.")
    st.stop()

try:
    data = load_gold_data(str(DB_PATH))
except Exception as exc:  # noqa: BLE001
    st.error(f"Could not load gold tables: {exc}")
    st.stop()

if data.empty:
    st.warning("Gold dataset is empty. Run `make ingest-local && make dbt-run`.")
    st.stop()

min_date = min(data["date_day"])
max_date = max(data["date_day"])
locations = sorted(data["location_name"].unique())
sources = sorted(data["source_name"].unique())

col_left, col_right = st.columns(2)
with col_left:
    selected_dates = st.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
with col_right:
    selected_location = st.selectbox("Location", options=["All"] + locations, index=0)
selected_source = st.selectbox("Source", options=["All"] + sources, index=0)

if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
    start_date, end_date = selected_dates
else:
    start_date = min_date
    end_date = max_date

filtered = data[(data["date_day"] >= start_date) & (data["date_day"] <= end_date)].copy()
if selected_location != "All":
    filtered = filtered[filtered["location_name"] == selected_location]
if selected_source != "All":
    filtered = filtered[filtered["source_name"] == selected_source]

if filtered.empty:
    st.warning("No rows for selected filters.")
    st.stop()

kpi_1, kpi_2, kpi_3 = st.columns(3)
kpi_1.metric("Observations", f"{len(filtered):,}")
kpi_2.metric("Avg Temperature (C)", f"{filtered['temperature_c'].mean():.2f}")
kpi_3.metric("Total Precipitation (mm)", f"{filtered['precipitation_mm'].sum():.2f}")

ts_data = (
    filtered.groupby("observation_ts_utc", as_index=False)
    .agg(avg_temp_c=("temperature_c", "mean"), total_precip_mm=("precipitation_mm", "sum"))
    .sort_values("observation_ts_utc")
)

chart = px.line(
    ts_data,
    x="observation_ts_utc",
    y="avg_temp_c",
    title="Average Temperature Over Time",
    labels={"observation_ts_utc": "Timestamp (UTC)", "avg_temp_c": "Avg Temp (C)"},
)
st.plotly_chart(chart, use_container_width=True)

st.subheader("Filtered Fact Preview")
st.dataframe(
    filtered.sort_values("observation_ts_utc", ascending=False).head(50),
    use_container_width=True,
)
