"""
pipeline_checks.py
Production-grade operational checks for GCS, Dataflow, BigQuery.
"""

import datetime
from google.cloud import storage, bigquery, dataflow_v1beta3
from google.api_core.exceptions import GoogleAPIError

EXPECTED_GCS_PREFIX = "raw_events/"
EXPECTED_MIN_FILES = 5
ARRIVAL_SLA_MINUTES = 60
BQ_DATASET = "analytics"
BQ_TABLE = "events_raw"
DATAFLOW_JOB_NAME = "events-transform-job"
PROJECT_ID = "your-project-id"
LOCATION = "us-central1"


# ------------------------------------------------------------
# GCS arrival validation
# ------------------------------------------------------------
def check_gcs_arrival(env):
    try:
        client = storage.Client()
        blobs = list(client.list_blobs(env["gcs_bucket"], prefix=EXPECTED_GCS_PREFIX))

        if len(blobs) == 0:
            return False, "No raw event files in GCS."

        if len(blobs) < EXPECTED_MIN_FILES:
            return False, f"Low GCS file count ({len(blobs)}) — possible ingestion failure."

        latest = max(blobs, key=lambda b: b.updated)
        age = (datetime.datetime.utcnow() - latest.updated.replace(tzinfo=None)).total_seconds() / 60

        if age > ARRIVAL_SLA_MINUTES:
            return False, f"GCS latest file is stale ({age:.1f}m old). SLA = {ARRIVAL_SLA_MINUTES}m."

        return True, "GCS arrival OK."

    except Exception as e:
        return False, f"GCS check failed: {e}"


# ------------------------------------------------------------
# Dataflow job validation
# ------------------------------------------------------------
def check_dataflow_job(env):
    try:
        client = dataflow_v1beta3.JobsV1Beta3Client()
        resp = client.list_jobs(
            project_id=PROJECT_ID,
            location=LOCATION,
            filter="ACTIVE"
        )

        for job in resp.jobs:
            if DATAFLOW_JOB_NAME in job.name:
                state = job.current_state.name

                if state == "JOB_STATE_DONE":
                    return True, "Dataflow job completed successfully."

                return False, f"Dataflow job state={state}"

        return False, "Dataflow job missing for today."

    except Exception as e:
        return False, f"Dataflow check failed: {e}"


# ------------------------------------------------------------
# BigQuery Partition Check
# ------------------------------------------------------------
def check_bigquery_load(env):
    try:
        client = bigquery.Client()

        query = f"""
        SELECT COUNT(*) AS cnt
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE _PARTITIONDATE = CURRENT_DATE()
        """

        count = list(client.query(query).result())[0]["cnt"]

        if count == 0:
            return False, "BigQuery partition exists but has 0 rows."

        return True, f"BigQuery load OK ({count} rows)."

    except Exception as e:
        return False, f"BigQuery check failed: {e}"


# ------------------------------------------------------------
# MASTER RUNNER
# ------------------------------------------------------------
def run_pipeline_operational_checks(env):
    checks = {
        "GCS Arrival": check_gcs_arrival,
        "Dataflow Job": check_dataflow_job,
        "BigQuery Load": check_bigquery_load
    }

    results = {}
    for name, fn in checks.items():
        ok, msg = fn(env)
        results[name] = {"status": ok, "message": msg}

    return results
