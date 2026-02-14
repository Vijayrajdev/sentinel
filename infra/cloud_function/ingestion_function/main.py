import functions_framework
import json
import io
import logging
import os
import uuid
import datetime
import re
import csv
from typing import Dict, Optional, Any, List, Tuple

from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# Global clients for reuse (warm starts)
bq_client: Optional[bigquery.Client] = None
storage_client: Optional[storage.Client] = None

PROJECT_ID = os.environ.get("GCP_PROJECT")
METADATA_DATASET = os.environ.get("METADATA_DATASET", "sentinel_audit")
MASTER_TABLE = os.environ.get("MASTER_TABLE", "ingestion_master")
LOGS_TABLE = os.environ.get("LOGS_TABLE", "ingestion_log")
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET")

# Standard logging setup
logging.basicConfig(level=logging.INFO)


# ==============================================================================
# CUSTOM EXCEPTIONS
# ==============================================================================
class SchemaDriftError(Exception):
    """Raised when file contains columns not present in BigQuery Table."""

    pass


class TableNotFoundError(Exception):
    """Raised when the target BigQuery Table does not exist."""

    pass


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================
def get_clients():
    """Lazy initialization of GCP clients."""
    global bq_client, storage_client
    if not bq_client:
        bq_client = bigquery.Client()
    if not storage_client:
        storage_client = storage.Client()
    return bq_client, storage_client


def log_event(severity: str, message: str, trace_id: str, **kwargs):
    """Structured JSON logging for Cloud Logging."""
    entry = {
        "severity": severity,
        "message": message,
        "logging.googleapis.com/trace": trace_id,
        "component": "sentinel-ingestor",
        **kwargs,
    }
    print(json.dumps(entry))


def get_csv_headers(bucket: str, file_name: str, delimiter: str = ",") -> List[str]:
    """Downloads the first line of the file to extract headers."""
    _, storage = get_clients()
    blob = storage.bucket(bucket).blob(file_name)

    # Download first 4KB to get the header row
    data = blob.download_as_bytes(start=0, end=4096).decode("utf-8")
    first_line = data.split("\n")[0]

    reader = csv.reader([first_line], delimiter=delimiter)
    headers = next(reader)
    return headers


def validate_schema(final_table_ref: str, file_headers: List[str], trace_id: str):
    """
    Strictly checks if table exists and if file headers match table schema.
    Raises SchemaDriftError or TableNotFoundError.
    """
    bq, _ = get_clients()

    log_event("INFO", f"🔍 Validating schema for: {final_table_ref}", trace_id)

    try:
        table = bq.get_table(final_table_ref)
    except NotFound:
        raise TableNotFoundError(
            f"Target table '{final_table_ref}' does not exist. Please create via Terraform."
        )

    # Get column names from BQ
    bq_columns = {schema_field.name for schema_field in table.schema}
    file_columns = set(file_headers)

    # Check for Unknown Columns (Drift)
    unknown_cols = file_columns - bq_columns

    if unknown_cols:
        error_msg = (
            f"Schema Drift Detected. File contains columns not in table: {unknown_cols}"
        )
        log_event("ERROR", error_msg, trace_id, unknown_columns=list(unknown_cols))
        raise SchemaDriftError(error_msg)

    log_event("INFO", "✅ Schema Validation Passed.", trace_id)


# ==============================================================================
# CORE LOGIC: RAW STRING LOADER
# ==============================================================================
def load_raw_strings(
    bucket: str,
    file_name: str,
    rule: Dict[str, Any],
    final_table_ref: str,
    trace_id: str,
) -> Dict[str, int]:
    """
    Loads CSV to BigQuery via Staging.
    Calculates Good/Bad record counts using SQL.
    Returns Dictionary of counts.
    """
    bq, _ = get_clients()

    # 1. Read Headers & Validate Schema
    delimiter = rule.get("delimiter", ",")
    headers = get_csv_headers(bucket, file_name, delimiter)
    validate_schema(final_table_ref, headers, trace_id)

    # 2. Prepare Staging Load
    staging_table_id = f"{PROJECT_ID}.{rule['target_dataset']}.staging_{rule['target_table']}_{uuid.uuid4().hex[:8]}"

    try:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=rule.get("skip_header_rows", 1),
            field_delimiter=delimiter,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            # Force all columns to STRING
            schema=[bigquery.SchemaField(h, "STRING") for h in headers],
            autodetect=False,
        )

        log_event(
            "INFO", f"⏳ Loading data to staging table: {staging_table_id}", trace_id
        )
        uri = f"gs://{bucket}/{file_name}"
        load_job = bq.load_table_from_uri(uri, staging_table_id, job_config=job_config)
        load_job.result()  # Wait for staging load

        # 3. CALCULATE QUALITY METRICS (Good vs Bad)
        # We construct a dynamic SQL query to count NULLs across all columns
        log_event("INFO", "📊 Calculating Good/Bad record counts...", trace_id)

        # Logic: If ANY column is NULL or Empty String, it's a "Bad" record.
        # "COALESCE(col, '') = ''" checks for both NULL and Empty String
        conditions = [f"(COALESCE(`{col}`, '') = '')" for col in headers]
        bad_record_condition = " OR ".join(conditions)

        quality_query = f"""
            SELECT 
                COUNT(*) as total_cnt,
                COUNTIF({bad_record_condition}) as bad_cnt
            FROM `{staging_table_id}`
        """

        query_job = bq.query(quality_query)
        res = query_job.result()

        metrics = {"total": 0, "bad": 0, "good": 0}
        for row in res:
            metrics["total"] = row.total_cnt
            metrics["bad"] = row.bad_cnt
            metrics["good"] = row.total_cnt - row.bad_cnt

        log_event("INFO", f"📈 Quality Metrics: {json.dumps(metrics)}", trace_id)

        # 4. Final Insert (Staging -> Final)
        col_names = headers
        cols_string = ", ".join([f"`{c}`" for c in col_names])

        insert_query = f"""
            INSERT INTO `{final_table_ref}` ({cols_string})
            SELECT {cols_string}
            FROM `{staging_table_id}`
        """

        log_event(
            "INFO", f"⏳ Executing Final Insert into {final_table_ref}...", trace_id
        )
        bq.query(insert_query).result()

        return metrics

    finally:
        # Cleanup Staging
        log_event("INFO", f"🧹 Cleaning up staging table: {staging_table_id}", trace_id)
        bq.delete_table(staging_table_id, not_found_ok=True)


def get_routing_rule(file_name: str, trace_id: str) -> Optional[Dict[str, Any]]:
    bq, _ = get_clients()
    log_event("INFO", "🔍 Looking up routing rules...", trace_id)

    query = f"""
        SELECT * FROM `{PROJECT_ID}.{METADATA_DATASET}.{MASTER_TABLE}` 
        WHERE is_active = TRUE
    """
    results = bq.query(query).result()
    for row in results:
        if re.search(row["file_pattern"], file_name):
            log_event(
                "INFO",
                f"✅ Found Rule: Pattern '{row['file_pattern']}' -> Table '{row['target_table']}'",
                trace_id,
            )
            return dict(row)

    log_event("WARNING", "⚠️ No matching routing rule found.", trace_id)
    return None


def archive_file(
    source_bucket: str, file_name: str, folder: str, trace_id: str
) -> Optional[str]:
    """Moves file to archive bucket and returns the NEW URI."""
    _, storage = get_clients()
    if not ARCHIVE_BUCKET:
        log_event(
            "WARNING", "⚠️ ARCHIVE_BUCKET env var not set. Skipping archive.", trace_id
        )
        return None

    src_bucket = storage.bucket(source_bucket)
    dest_bucket = storage.bucket(ARCHIVE_BUCKET)
    source_blob = src_bucket.blob(file_name)

    if not source_blob.exists():
        log_event(
            "WARNING",
            f"⚠️ Cannot archive {file_name}, file not found (phantom?).",
            trace_id,
        )
        return None

    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
    new_name = f"{folder}/{os.path.basename(file_name)}_{ts}"
    new_uri = f"gs://{ARCHIVE_BUCKET}/{new_name}"

    log_event("INFO", f"📦 Archiving file to: {new_uri}", trace_id)

    dest_bucket.copy_blob(source_blob, dest_bucket, new_name)
    source_blob.delete()

    return new_uri


def audit_log(
    trace_id,
    ingestion_id,
    file_name,
    file_uri,
    status,
    start_time,
    metrics: Dict[str, int] = None,
    target_table=None,
    error_msg=None,
):
    """
    Logs status AND Quality Metrics (Total, Processed, Good, Bad) to BigQuery.
    """
    bq, _ = get_clients()

    if isinstance(start_time, datetime.datetime):
        start_time_str = start_time.isoformat()
    else:
        start_time_str = str(start_time)

    # Initialize defaults if metrics is None (e.g., failure before load)
    if metrics is None:
        metrics = {"total": 0, "good": 0, "bad": 0}

    row = {
        "ingestion_id": ingestion_id,
        "file_name": file_name,
        "file_uri": file_uri,
        "status": status,
        "target_table": target_table,
        "error_message": error_msg[:2000] if error_msg else None,
        "start_time": start_time_str,
        "end_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        # --- NEW COLUMNS ---
        "total_records": metrics.get("total", 0),
        "processed_records": metrics.get("total", 0),  # We attempted to load total
        "good_records": metrics.get("good", 0),
        "bad_records": metrics.get("bad", 0),
    }

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
    )

    table_id = f"{PROJECT_ID}.{METADATA_DATASET}.{LOGS_TABLE}"

    try:
        log_event("INFO", "📝 Writing Audit Log...", trace_id)
        json_data = json.dumps(row) + "\n"
        file_obj = io.StringIO(json_data)
        job = bq.load_table_from_file(file_obj, table_id, job_config=job_config)
        job.result()
        log_event("INFO", "🟢 Audit Log Saved Successfully", trace_id)
    except Exception as e:
        print(f"CRITICAL: FAILED TO WRITE AUDIT LOG: {e}")


def handle_failure(
    bucket,
    file_name,
    status,
    error_msg,
    trace_id,
    ingestion_id,
    start_time,
    target_folder="exempted",
):
    """
    Safe failure handling. Moves file to specific folder (unprocessed vs exempted).
    """
    try:
        log_event(
            "INFO",
            f"🛑 Handling Failure: Status={status}, TargetFolder={target_folder}",
            trace_id,
        )

        final_uri = None
        try:
            final_uri = archive_file(bucket, file_name, target_folder, trace_id)
        except Exception as e:
            log_event("WARNING", f"Could not archive failed file: {e}", trace_id)

        if not final_uri:
            final_uri = f"gs://{bucket}/{file_name} (Failed to Archive)"

        audit_log(
            trace_id=trace_id,
            ingestion_id=ingestion_id,
            file_name=file_name,
            file_uri=final_uri,
            status=status,
            start_time=start_time,
            metrics=None,  # No metrics for failed runs
            error_msg=error_msg,
        )

        log_event("INFO", "🏁 Failure Handled Cleanly.", trace_id)

    except Exception as e:
        print(f"CRITICAL: Error handler crashed. Swallowing error to stop loop. {e}")


# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================
@functions_framework.cloud_event
def process_file(cloud_event):
    # Context Initialization
    start_time = datetime.datetime.now(datetime.timezone.utc)
    ingestion_id = uuid.uuid4().hex
    trace_id = f"projects/{PROJECT_ID}/traces/{ingestion_id}"

    data = cloud_event.data
    bucket_name = data.get("bucket")
    file_name = data.get("name")

    # --- CIRCUIT BREAKER 1: Ignore Folders ---
    if not file_name or file_name.endswith("/"):
        return

    # --- CIRCUIT BREAKER 2: Ignore Output Files ---
    if (
        "processed/" in file_name
        or "exempted/" in file_name
        or "unprocessed/" in file_name
    ):
        print(f"🚫 Ignoring internal event: {file_name}")
        return

    # --- CIRCUIT BREAKER 3: Phantom File Check ---
    _, storage = get_clients()
    if not storage.bucket(bucket_name).blob(file_name).exists():
        print(f"👻 File not found: {file_name}. Stopping to prevent retry loop.")
        return

    log_event("INFO", f"🚀 Started Processing: {file_name}", trace_id)
    log_event("INFO", f"🆔 Ingestion ID: {ingestion_id}", trace_id)

    try:
        # 1. ROUTER
        rule = get_routing_rule(file_name, trace_id)
        if not rule:
            handle_failure(
                bucket_name,
                file_name,
                "SKIPPED",
                "No matching routing rule",
                trace_id,
                ingestion_id,
                start_time,
                target_folder="exempted",
            )
            return

        final_table_ref = (
            f"{PROJECT_ID}.{rule['target_dataset']}.{rule['target_table']}"
        )
        log_event("INFO", f"🎯 Target Table identified: {final_table_ref}", trace_id)

        # 2. LOADER (Includes strict Schema Validation + Metric Calculation)
        # Returns dict: {'total': 100, 'good': 98, 'bad': 2}
        metrics = load_raw_strings(
            bucket_name, file_name, rule, final_table_ref, trace_id
        )

        # 3. ARCHIVER (Success Case)
        final_uri = archive_file(bucket_name, file_name, "processed", trace_id)

        # 4. AUDIT
        audit_log(
            trace_id=trace_id,
            ingestion_id=ingestion_id,
            file_name=file_name,
            file_uri=final_uri,
            status="SUCCESS",
            start_time=start_time,
            metrics=metrics,
            target_table=final_table_ref,
        )
        log_event("INFO", "✅ Ingestion Complete Successfully.", trace_id)

    except (SchemaDriftError, TableNotFoundError) as e:
        error_msg = str(e)
        log_event("ERROR", f"❌ Validation Error: {error_msg}", trace_id)
        # Schema/Table errors -> "unprocessed"
        handle_failure(
            bucket_name,
            file_name,
            "FAILED",
            error_msg,
            trace_id,
            ingestion_id,
            start_time,
            target_folder="unprocessed",
        )

    except Exception as e:
        error_msg = str(e)
        log_event("ERROR", f"❌ System Pipeline Failed: {error_msg}", trace_id)
        # System crashes -> "exempted"
        handle_failure(
            bucket_name,
            file_name,
            "FAILED",
            error_msg,
            trace_id,
            ingestion_id,
            start_time,
            target_folder="exempted",
        )
