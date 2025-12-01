"""
Attribution Framework
Supports:
- First-click attribution (FC)
- Last-click attribution (LC)
- 7-day lookback window
"""

import pandas as pd

def build_attribution(df):

    # Filter only events with UTM or purchase
    purchases = df[df["event_name"]=="purchase"]
    attributions = []

    for _, purchase in purchases.iterrows():
        cid = purchase["client_id"]
        purchase_time = purchase["timestamp"]

        history = df[
            (df.client_id==cid) &
            (df.timestamp <= purchase_time) &
            (df.timestamp >= purchase_time - pd.Timedelta(days=7))
        ].sort_values("timestamp")

        # Only events with UTMs considered marketing touches
        touches = history[history["utm_source"].notna()]

        if touches.empty:
            # fallback to referrer
            channel = "direct" if pd.isna(purchase["referrer"]) else "referral"
            attributions.append({
                "client_id": cid,
                "purchase_timestamp": purchase_time,
                "attribution_fc": channel,
                "attribution_lc": channel
            })
            continue

        # First click
        fc = touches.iloc[0]["utm_source"]
        # Last click
        lc = touches.iloc[-1]["utm_source"]

        attributions.append({
            "client_id": cid,
            "purchase_timestamp": purchase_time,
            "attribution_fc": fc,
            "attribution_lc": lc
        })

    return pd.DataFrame(attributions)
