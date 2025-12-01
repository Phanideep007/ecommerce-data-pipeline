"""
E2E E-Commerce Pipeline DAG for Cloud Composer (GCP)

Includes:
- Part 1: Data Quality Validation
- Part 2: Transformations (Sessionization, Funnel Modeling, Attribution)
- Part 3: Business Analysis
- Part 4: Production Monitoring
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import pandas as pd
import json
import os

# Import your Python modules
from code.part1_validation import run_data_quality_validation
from code.part2_transformations import run_transformations
from code.part3_analysis import run_business_analysis
from code.part4_monitoring.monitoring_engine import run_monitoring


# ==============================
# DAG CONFIG
# ==============================
default_args = {
    "owner": "phanideep",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["youremail@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id="ecommerce_full_pipeline",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # Runs daily at 3 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

# ==============================
# TASK 1 — LIST RAW EVENT FILES IN GCS
# ==============================
list_gcs_files = GCSListObjectsOperator(
    task_id="list_gcs_event_files",
    bucket="your-raw-events-bucket",
    prefix="events/",
    dag=dag,
)

# ==============================
# TASK 2 — LOAD RAW EVENTS INTO XCOM
# ==============================
def load_events_callable(**context):
    bucket = "your-raw-events-bucket"
    files = context["ti"].xcom_pull(task_ids="list_gcs_event_files")

    dfs = []
    for f in files:
        if f.endswith(".csv"):
            dfs.append(pd.read_csv(f"gs://{bucket}/{f}"))

    full_df = pd.concat(dfs, ignore_index=True)
    context["ti"].xcom_push("raw_df", full_df.to_json())

load_raw_events = PythonOperator(
    task_id="load_raw_events",
    python_callable=load_events_callable,
    dag=dag,
)

# ==============================
# TASK 3 — PART 1 VALIDATION
# ==============================
def validation_callable(**context):
    raw_json = context["ti"].xcom_pull("raw_df")
    df = pd.read_json(raw_json)

    report = run_data_quality_validation(df)
    context["ti"].xcom_push("validated_df", df.to_json())
    context["ti"].xcom_push("dq_report", json.dumps(report))

validate_events = PythonOperator(
    task_id="validate_events",
    python_callable=validation_callable,
    dag=dag,
)

# ==============================
# TASK 4 — PART 2 TRANSFORMATION (SESSION, FUNNEL, ATTRIBUTION)
# ==============================
def transformations_callable(**context):
    raw_json = context["ti"].xcom_pull("raw_df")
    df = pd.read_json(raw_json)

    session_df, funnel_df, attribution_df = run_transformations(df)

    context["ti"].xcom_push("session_df", session_df.to_json())
    context["ti"].xcom_push("funnel_df", funnel_df.to_json())
    context["ti"].xcom_push("attribution_df", attribution_df.to_json())

transform_data = PythonOperator(
    task_id="transform_data",
    python_callable=transformations_callable,
    dag=dag,
)

# ==============================
# TASK 5 — LOAD TO BIGQUERY USING SQL FILES
# ==============================

BQ_PROJECT = "your-gcp-project"
BQ_DATASET = "analytics"

load_sessions_sql = BigQueryInsertJobOperator(
    task_id="load_sessions",
    configuration={
        "query": {
            "query": open("/home/airflow/gcs/data/sql/create_sessions.sql").read(),
            "useLegacySql": False,
        }
    },
    dag=dag,
)

load_funnel_sql = BigQueryInsertJobOperator(
    task_id="load_funnel",
    configuration={
        "query": {
            "query": open("/home/airflow/gcs/data/sql/create_funnel.sql").read(),
            "useLegacySql": False,
        }
    },
    dag=dag,
)

load_attribution_sql = BigQueryInsertJobOperator(
    task_id="load_attribution",
    configuration={
        "query": {
            "query": open("/home/airflow/gcs/data/sql/create_attribution.sql").read(),
            "useLegacySql": False,
        }
    },
    dag=dag,
)

# ==============================
# TASK 6 — PART 3 BUSINESS ANALYSIS
# ==============================
def analysis_callable(**context):
    session = pd.read_json(context["ti"].xcom_pull("session_df"))
    funnel = pd.read_json(context["ti"].xcom_pull("funnel_df"))
    attr = pd.read_json(context["ti"].xcom_pull("attribution_df"))

    summary = run_business_analysis(session, funnel, attr)
    context["ti"].xcom_push("business_summary", json.dumps(summary))

run_analysis = PythonOperator(
    task_id="run_analysis",
    python_callable=analysis_callable,
    dag=dag,
)

# ==============================
# TASK 7 — PART 4 MONITORING
# ==============================
def monitoring_callable(**context):
    raw = pd.read_json(context["ti"].xcom_pull("raw_df"))
    session = pd.read_json(context["ti"].xcom_pull("session_df"))
    funnel = pd.read_json(context["ti"].xcom_pull("funnel_df"))
    attr = pd.read_json(context["ti"].xcom_pull("attribution_df"))

    env = {
        "gcs_bucket": "your-raw-events-bucket",
        "email_sender": "alerts@example.com",
        "email_receiver": "ops@example.com",
        "email_password": "APP_PASSWORD_OR_TOKEN"
    }

    alerts = run_monitoring(raw, funnel, attr, env)
    print(alerts)

production_monitoring = PythonOperator(
    task_id="production_monitoring",
    python_callable=monitoring_callable,
    dag=dag,
)

# ==============================
# DAG FLOW
# ==============================
(
    list_gcs_files
    >> load_raw_events
    >> validate_events
    >> transform_data
    >> [load_sessions_sql, load_funnel_sql, load_attribution_sql]
    >> run_analysis
    >> production_monitoring
)
