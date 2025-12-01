"""
data_quality_checks.py
Runtime data quality monitors for production.
"""

import pandas as pd

def run_data_quality_monitors(df):
    alerts = []

    # Missing timestamp anomalies
    if df.timestamp.isna().sum() > 0:
        alerts.append(f"Missing timestamps: {df.timestamp.isna().sum()} rows.")

    # Unexpected event names
    allowed = {"page_viewed", "product_added_to_cart", "checkout_started", "purchase"}
    unknowns = df[~df.event_name.isin(allowed)]["event_name"].unique()
    if len(unknowns) > 0:
        alerts.append(f"Unknown event types detected: {unknowns.tolist()}")

    # High duplicate user events
    dup_count = df.duplicated(subset=["event_id"]).sum()
    if dup_count > 0:
        alerts.append(f"Duplicate event_ids detected: {dup_count}")

    # UTM missing rate
    utm_missing = df[df.utm_source.isna()].shape[0]
    if utm_missing > 100:
        alerts.append(f"High UTM missing count: {utm_missing}")

    return alerts
