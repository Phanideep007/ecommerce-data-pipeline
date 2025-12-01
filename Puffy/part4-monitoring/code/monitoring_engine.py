"""
monitoring_engine.py

Combines all monitoring checks:
- Pipeline operational checks
- Data quality checks
- Business KPI checks
- Attribution checks
- Sends alerts via email/slack

Returns alert messages for Airflow logs.
"""

from .pipeline_checks import run_pipeline_operational_checks
from .data_quality_checks import run_data_quality_monitors
from .business_kpi_monitors import run_business_kpi_monitors
from .email_alerts import send_email_alert

def run_monitoring(raw_events_df, funnel_df, attribution_df, env):
    alerts = []

    # -----------------------------
    # 1. Operational Health Checks
    # -----------------------------
    op_results = run_pipeline_operational_checks(env)
    for name, res in op_results.items():
        if not res["status"]:
            alerts.append(f"[OPERATIONAL] {res['message']}")

    # -----------------------------
    # 2. Data Quality Checks
    # -----------------------------
    dq_results = run_data_quality_monitors(raw_events_df)
    for a in dq_results:
        alerts.append(f"[DATA QUALITY] {a}")

    # -----------------------------
    # 3. Business KPI Checks
    # -----------------------------
    biz_results = run_business_kpi_monitors(funnel_df, attribution_df)
    for a in biz_results:
        alerts.append(f"[BUSINESS KPI] {a}")

    # -----------------------------
    # 4. Email Alerts (if needed)
    # -----------------------------
    if alerts:
        send_email_alert(
            subject="Production Monitoring Alerts Detected",
            messages=alerts,
            env=env,
        )

    return alerts
