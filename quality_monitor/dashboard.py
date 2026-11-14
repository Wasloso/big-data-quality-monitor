"""Small read-only view of stored quality windows and alert evaluations."""

import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st


DATABASE = Path(os.environ.get("QUALITY_DATABASE", "/data/serving/quality-v2.sqlite"))
st.set_page_config(page_title="Quality monitor", layout="wide")
st.title("Production quality monitor")
st.caption("Predicted rejects from stored inspection windows. Fixture models use synthetic scores.")


@st.fragment(run_every="10s")
def show_dashboard():
    st.button("Refresh")
    if not DATABASE.is_file():
        st.info("No serving database yet. Start the window writer with --database /data/serving/quality-v2.sqlite.")
        return
    try:
        with closing(sqlite3.connect(DATABASE.resolve().as_uri() + "?mode=ro", timeout=5)) as connection:
            connection.row_factory = sqlite3.Row
            # Read both tables from one committed snapshot while Spark writes.
            connection.execute("BEGIN")
            windows = pd.DataFrame([dict(row) for row in connection.execute(
                "SELECT * FROM windows ORDER BY updated_at DESC, window_start DESC"
            )])
            alerts = pd.DataFrame([dict(row) for row in connection.execute("SELECT * FROM alerts")])
    except sqlite3.Error as error:
        st.error(f"Cannot read the serving database: {error}")
        return
    if windows.empty:
        st.info("The database has no quality windows yet.")
        return

    run_id = st.selectbox("Run", windows["run_id"].drop_duplicates().tolist())
    selected = windows[windows["run_id"] == run_id]
    versions = ["category", "model_version", "threshold_version"]
    detector = st.selectbox(
        "Detector", list(selected[versions].drop_duplicates().itertuples(index=False, name=None)),
        format_func=lambda values: " / ".join(values),
    )
    for column, value in zip(versions, detector):
        selected = selected[selected[column] == value]
    line = st.selectbox("Line", [None, *sorted(selected["line_id"].unique())],
                        format_func=lambda value: "All lines" if value is None else value)
    if line is not None:
        selected = selected[selected["line_id"] == line]

    last_update = pd.to_datetime(selected["updated_at"], utc=True).max().to_pydatetime()
    st.caption(f"Last stored update (UTC): {last_update.isoformat()}. Refreshes every 10 seconds.")
    if (datetime.now(timezone.utc) - last_update).total_seconds() > 300:
        st.warning("No window updates for over 5 minutes. The source may be idle or processing may have stopped.")
    inspections = int(selected["inspections"].sum())
    rejects = int(selected["rejects"].sum())
    totals, rejected, rate = st.columns(3)
    totals.metric("Inspections", inspections)
    rejected.metric("Predicted rejects", rejects)
    rate.metric("Predicted reject rate", f"{rejects / inspections:.1%}" if inspections else "—")
    st.caption("Provisional windows can change. Final windows have passed the event-time watermark.")
    trend = selected.sort_values("window_start").copy()
    trend["window_start"] = pd.to_datetime(trend["window_start"], utc=True)
    st.line_chart(trend, x="window_start", y="reject_rate", color="line_id")
    selected = selected.assign(state=selected["is_final"].map({0: "provisional", 1: "final"}))
    st.dataframe(selected[["window_start", "window_end", "line_id", "inspections", "rejects",
                           "reject_rate", "state"]], hide_index=True)

    st.subheader("Alert evaluations")
    if alerts.empty:
        st.info("No alert evaluations yet.")
        return
    evaluations = alerts.merge(selected[["window_key", "line_id", "window_start"]], on="window_key")
    st.write(f"Active rule results: {int((evaluations['status'] == 'active').sum())}")
    st.dataframe(evaluations[["line_id", "window_start", "status", "threshold",
                              "minimum_inspections", "is_provisional"]], hide_index=True)


show_dashboard()
