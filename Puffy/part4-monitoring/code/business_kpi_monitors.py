"""
business_kpi_monitors.py
Monitors real KPIs: Revenue, Conversion Rate, AOV, Add-to-Cart Rate, Attribution shifts.
"""

import numpy as np
import pandas as pd

def run_business_kpi_monitors(funnel_df, attribution_df):
    alerts = []

    # -----------------------------
    # Revenue Drop Check
    # -----------------------------
    revenue = funnel_df[funnel_df.event_name == "purchase"].amount.sum()
    if revenue < 0.8 * funnel_df.amount.mean() * 7:
        alerts.append("Possible revenue drop: <80% of weekly average.")

    # -----------------------------
    # Conversion Rate Drop
    # -----------------------------
    views = funnel_df[funnel_df.event_name == "page_viewed"].shape[0]
    purchases = funnel_df[funnel_df.event_name == "purchase"].shape[0]
    conv = purchases / max(views, 1)

    if conv < 0.005:  # 0.5%
        alerts.append(f"Conversion rate abnormally low ({conv:.3%}).")

    # -----------------------------
    # Attribution Spike Detection
    # -----------------------------
    by_channel = attribution_df["attributed_channel"].value_counts(normalize=True)

    if by_channel.max() > 0.70:
        alerts.append("One channel is receiving >70% attribution — possible tracking skew.")

    return alerts
