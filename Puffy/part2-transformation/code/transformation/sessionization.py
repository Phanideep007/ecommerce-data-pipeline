"""
Sessionization Logic

Rules (from README + industry standard):
- Session boundary: 30 minutes of inactivity
- New session if referrer changes (external → internal)
- New session when campaign parameters change (utm_*)
- New session when device changes (mobile → desktop)
"""

import pandas as pd
from urllib.parse import urlparse, parse_qs

def extract_utm(url):
    try:
        parsed = urlparse(url)
        q = parse_qs(parsed.query)
        return {
            "utm_source": q.get("utm_source", [None])[0],
            "utm_medium": q.get("utm_medium", [None])[0],
            "utm_campaign": q.get("utm_campaign", [None])[0]
        }
    except:
        return {"utm_source": None, "utm_medium": None, "utm_campaign": None}


def build_sessions(df):
    df = df.sort_values(["client_id", "timestamp"])

    # Extract UTM for attribution
    utm_cols = df["page_url"].apply(extract_utm).apply(pd.Series)
    df = pd.concat([df, utm_cols], axis=1)

    # Device grouping (simplified)
    df["device_type"] = df["user_agent"].apply(
        lambda ua: "mobile" if "Mobile" in ua else "desktop"
    )

    # Identify session boundaries
    df["prev_timestamp"] = df.groupby("client_id")["timestamp"].shift(1)
    df["time_diff"] = (df["timestamp"] - df["prev_timestamp"]).dt.total_seconds().fillna(0)

    df["prev_utm_source"] = df.groupby("client_id")["utm_source"].shift(1)
    df["utm_changed"] = df["utm_source"] != df["prev_utm_source"]

    df["prev_device"] = df.groupby("client_id")["device_type"].shift(1)
    df["device_changed"] = df["device_type"] != df["prev_device"]

    # Session break conditions:
    df["new_session"] = (
        (df["time_diff"] > 1800) |
        (df["utm_changed"] & df["utm_source"].notna()) |
        (df["device_changed"])
    )

    # Assign session_id incrementally
    df["session_id"] = df.groupby("client_id")["new_session"].cumsum()

    return df
