import pandas as pd

def run_anomaly_checks(df, log):

    df["date"] = df["timestamp"].dt.date
    daily_counts = df.groupby("date").size().reset_index(name="count")
    daily_counts["rolling_mean"] = daily_counts["count"].rolling(7).mean()
    daily_counts["rolling_std"] = daily_counts["count"].rolling(7).std()

    anomalies = daily_counts[
        (daily_counts["count"] > daily_counts["rolling_mean"] + 3*daily_counts["rolling_std"]) |
        (daily_counts["count"] < daily_counts["rolling_mean"] - 3*daily_counts["rolling_std"])
    ]

    log("anomaly.event_volume_outlier", anomalies.empty,
        detail=f"Detected anomalies: {len(anomalies)}")
