import pandas as pd

def run_consistency_checks(df, log, quarantine):

    # 1. Timestamp monotonicity (per client)
    inconsistent = []
    for cid, grp in df.groupby("client_id"):
        if not grp["timestamp"].is_monotonic_increasing:
            inconsistent.append(cid)
            for _, row in grp.sort_values("timestamp").iterrows():
                quarantine(row, "timestamp_out_of_order")
    log("consistency.event_sequence_monotonic", len(inconsistent)==0)

    # 2. Page URLs that do NOT match event_name expectations
    mismatches = df[
        (df.event_name=="product_added_to_cart") & (~df.page_url.str.contains("/product"))
    ]
    for _, row in mismatches.iterrows():
        quarantine(row, "event-url-semantic-mismatch")
    log("consistency.semantic_event_url_alignment", mismatches.empty)

    # 3. Duplicated events (same client, same timestamp, same event_name)
    dupes = df[df.duplicated(subset=["client_id","timestamp","event_name"], keep=False)]
    for _, row in dupes.iterrows():
        quarantine(row, "duplicate_event")
    log("consistency.duplicate_events", dupes.empty)
