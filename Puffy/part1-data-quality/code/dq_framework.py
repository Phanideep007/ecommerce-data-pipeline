"""
Design Principles:
1. Fail-Fast: Bad data must NEVER silently enter the warehouse.
2. Layered Validation: Schema → Validity → Consistency → Anomaly.
3. Reusable & Extensible: New rules can be added without touching the core engine.
4. Business-Aware: Validation includes metrics that directly affect revenue attribution.
5. Deterministic Logging: Every failed row is quarantined; nothing is discarded silently.
"""

import pandas as pd
import os
from rules.schema_checks import run_schema_checks
from rules.validity_checks import run_validity_checks
from rules.consistency_checks import run_consistency_checks
from rules.anomaly_checks import run_anomaly_checks

INVALID_ROWS = []
RESULTS = []

def log_result(check_name, status, detail=""):
    RESULTS.append({
        "check": check_name,
        "status": "PASS" if status else "FAIL",
        "detail": detail
    })

def quarantine(row, reason):
    row["_dq_failure_reason"] = reason
    INVALID_ROWS.append(row)

def run_all(input_df):
    print("Running Schema Checks…")
    run_schema_checks(input_df, log_result, quarantine)

    print("Running Validity Checks…")
    run_validity_checks(input_df, log_result, quarantine)

    print("Running Consistency Checks…")
    run_consistency_checks(input_df, log_result, quarantine)

    print("Running Anomaly Checks…")
    run_anomaly_checks(input_df, log_result)

    os.makedirs("reports", exist_ok=True)
    pd.DataFrame(RESULTS).to_csv("reports/validation_results.csv", index=False)

    if INVALID_ROWS:
        pd.DataFrame(INVALID_ROWS).to_csv("reports/quarantined_rows.csv", index=False)

    print("Data Quality Framework Execution Complete.")
