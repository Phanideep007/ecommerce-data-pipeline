"""
Monitor attribution stability and marketing signal integrity.
"""

import pandas as pd

def detect_direct_spike(attr):
    daily = (
        attr.assign(is_direct=attr.attribution_lc=="direct")
            .groupby(attr.purchase_timestamp.dt.date)["is_direct"]
            .mean()
            .reset_index(name="direct_share")
    )
    return daily[daily.direct_share > 0.45]


def detect_paid_drop(attr):
    daily = (
        attr.assign(is_paid=attr.attribution_lc.notna())
            .groupby(attr.purchase_timestamp.dt.date)["is_paid"]
            .mean()
            .reset_index(name="paid_share")
    )
    daily["baseline"] = daily.paid_share.rolling(7).mean()
    return (daily.paid_share < daily.baseline * 0.65).any()


def detect_attr_missing_for_purchases(events, attr):
    purchase_events = events[events.event_name=="purchase"]
    return purchase_events.client_id.nunique() != attr.client_id.nunique()
