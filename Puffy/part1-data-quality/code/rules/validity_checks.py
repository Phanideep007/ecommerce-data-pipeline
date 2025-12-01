import json
import pandas as pd
import urllib.parse as urlparse

def run_validity_checks(df, log, quarantine):

    # 1. Client ID validity
    invalid = df[df["client_id"].isna()]
    for _, row in invalid.iterrows():
        quarantine(row, "client_id_missing")
    log("validity.client_id_present", invalid.empty)

    # 2. Timestamp validity
    invalid_ts = df[df["timestamp"].isna()]
    for _, row in invalid_ts.iterrows():
        quarantine(row, "timestamp_missing")
    log("validity.timestamp_present", invalid_ts.empty)

    # 3. Event name validity
    allowed = {
        "page_viewed","email_filled_on_popup",
        "product_added_to_cart","checkout_started",
        "purchase"
    }
    invalid_event = df[~df.event_name.isin(allowed)]
    for _, r in invalid_event.iterrows():
        quarantine(r, "invalid_event_name")
    log("validity.event_name_valid", invalid_event.empty)

    # 4. JSON validity in event_data
    bad_json = []
    for i, row in df.iterrows():
        try:
            json.loads(row["event_data"])
        except:
            bad_json.append(row)
            quarantine(row, "malformed_event_data_json")
    log("validity.event_data_is_valid_json", len(bad_json)==0)

    # 5. URL parse validity
    bad_url = []
    for i, row in df.iterrows():
        try:
            urlparse.urlparse(row["page_url"])
        except:
            bad_url.append(row)
            quarantine(row, "invalid_page_url")
    log("validity.page_url_parseable", len(bad_url)==0)
