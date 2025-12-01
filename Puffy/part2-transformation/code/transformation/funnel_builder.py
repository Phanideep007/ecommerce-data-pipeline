"""
Funnel Construction (from README taxonomy)
Stages:
1. view_product         → page_viewed AND '/products/'
2. add_to_cart          → product_added_to_cart
3. checkout_started     → checkout_started
4. purchase             → purchase
"""

import pandas as pd

def build_funnel(df):
    df["is_pdp_view"] = df.apply(
        lambda r: r["event_name"]=="page_viewed" and "/products/" in r["page_url"],
        axis=1
    )

    df["is_add_to_cart"] = df["event_name"] == "product_added_to_cart"
    df["is_checkout"] = df["event_name"] == "checkout_started"
    df["is_purchase"] = df["event_name"] == "purchase"

    # Session-level funnel metrics
    funnel = df.groupby(["client_id","session_id"]).agg(
        product_views=("is_pdp_view","sum"),
        add_to_cart=("is_add_to_cart","sum"),
        checkout=("is_checkout","sum"),
        purchase=("is_purchase","sum"),
        session_start=("timestamp","min"),
        session_end=("timestamp","max")
    ).reset_index()

    return funnel
