"""
Main Transformation Engine:
1. Sessionization
2. Funnel Construction
3. Attribution Modeling
4. Dimensional Tables
"""

import pandas as pd
from sessionization import build_sessions
from funnel_builder import build_funnel
from attribution import build_attribution

def run_transformations(raw_df):
    print("▶ Building sessions...")
    sessionized = build_sessions(raw_df)

    print("▶ Building funnel metrics...")
    funnel = build_funnel(sessionized)

    print("▶ Building first-click & last-click attribution...")
    attribution = build_attribution(sessionized)

    print("▶ Building dimensions (users & devices)...")
    dim_users = (
        sessionized.groupby("client_id")
        .agg(first_seen=("timestamp","min"), last_seen=("timestamp","max"))
        .reset_index()
    )

    dim_devices = (
        sessionized.groupby(["client_id"])
        .device_type.first().reset_index()
    )

    return {
        "fact_events": sessionized,
        "fact_funnel": funnel,
        "fact_attribution": attribution,
        "dim_users": dim_users,
        "dim_devices": dim_devices
    }
