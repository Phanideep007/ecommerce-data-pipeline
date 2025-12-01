import pandas as pd

REQUIRED_COLUMNS = [
    "client_id", "page_url", "referrer", "timestamp",
    "event_name", "event_data", "user_agent"
]

def run_schema_checks(df, log, quarantine):
    # Column completeness
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        log("schema.required_columns", False, f"Missing: {missing}")
    else:
        log("schema.required_columns", True)

    # Data types
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        log("schema.timestamp_type", False, "timestamp must be ISO8601/UTC")
    else:
        log("schema.timestamp_type", True)
