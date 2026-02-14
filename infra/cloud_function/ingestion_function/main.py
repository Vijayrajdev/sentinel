import functions_framework
import json
import io
import logging
import os
import uuid
import datetime
import re
import csv
from typing import Dict, Optional, Any, List

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

# Use standard logging, but we will format as JSON
logging.basicConfig(level=logging.INFO)


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
    # Print to stdout/stderr is how Cloud Functions captures logs
    print(json.dumps(entry))


def get_csv_headers(bucket: str, file_name: str, delimiter: str = ",") -> List[str]:
    """
    Downloads the first line of the file to extract headers.
    Raises exception on failure (caught by main loop).
    """
    _, storage = get_clients()
    blob = storage.bucket(bucket).blob(file_name)

    # Download first 4KB to get the header row
    data = blob.download_as_bytes(start=0, end=4096).decode("utf-8")
    first_line = data.split("\n")[0]

    reader = csv.reader([first_line], delimiter=delimiter)
    headers = next(reader)
    return headers


# ==============================================================================
# CORE LOGIC: RAW STRING LOADER
# ==============================================================================
def load_raw_strings(
    bucket: str,
    file_name: str,
    rule: Dict[str, Any],
    final_table_ref: str,
    trace_id: str,
) -> int:
    """
    Loads CSV to BigQuery treating ALL columns as STRING.
    """
    bq, _ = get_clients()
    # Unique staging table to avoid collisions
    staging_table_id = f"{PROJECT_ID}.{rule['target_dataset']}.staging_{rule['target_table']}_{uuid.uuid4().hex[:8]}"

    try:
        # --- 1. Prepare Job Config ---
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=rule.get("skip_header_rows", 1),
            field_delimiter=rule.get("delimiter", ","),
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        # Force all columns to STRING
        headers = get_csv_headers(bucket, file_name, rule.get("delimiter", ","))
        job_config.schema = [bigquery.SchemaField(h, "STRING") for h in headers]
        job_config.autodetect = False

        log_event("INFO", f"⏳ Loading to staging: {staging_table_id}", trace_id)

        uri = f"gs://{bucket}/{file_name}"
        load_job = bq.load_table_from_uri(uri, staging_table_id, job_config=job_config)
        load_job.result()  # Wait for load

        # --- 2. Schema Evolution ---
        staging_table = bq.get_table(staging_table_id)

        try:
            final_table = bq.get_table(final_table_ref)
        except NotFound:
            log_event("WARNING", f"Creating new table: {final_table_ref}", trace_id)
            bq.create_table(
                bigquery.Table(final_table_ref, schema=staging_table.schema)
            )
            final_table = bq.get_table(final_table_ref)

        # Detect new columns
        final_cols = {f.name for f in final_table.schema}
        new_columns = [f for f in staging_table.schema if f.name not in final_cols]

        if new_columns:
            log_event(
                "WARNING", f"⚠️ Adding {len(new_columns)} columns to schema.", trace_id
            )
            updated_schema = final_table.schema[:]
            updated_schema.extend(new_columns)
            final_table.schema = updated_schema
            bq.update_table(final_table, ["schema"])

        # --- 3. Final Insert ---
        col_names = [f.name for f in staging_table.schema]
        cols_string = ", ".join([f"`{c}`" for c in col_names])

        insert_query = f"""
            INSERT INTO `{final_table_ref}` ({cols_string})
            SELECT {cols_string}
            FROM `{staging_table_id}`
        """

        log_event("INFO", "⏳ Executing Final Insert...", trace_id)
        bq.query(insert_query).result()

        return load_job.output_rows

    finally:
        # Always clean up staging
        bq.delete_table(staging_table_id, not_found_ok=True)


def get_routing_rule(file_name: str) -> Optional[Dict[str, Any]]:
    bq, _ = get_clients()
    query = f"""
        SELECT * FROM `{PROJECT_ID}.{METADATA_DATASET}.{MASTER_TABLE}` 
        WHERE is_active = TRUE
    """
    results = bq.query(query).result()
    for row in results:
        if re.search(row["file_pattern"], file_name):
            return dict(row)
    return None


def archive_file(source_bucket: str, file_name: str, folder: str, trace_id: str):
    _, storage = get_clients()
    if not ARCHIVE_BUCKET:
        return

    src_bucket = storage.bucket(source_bucket)
    dest_bucket = storage.bucket(ARCHIVE_BUCKET)
    source_blob = src_bucket.blob(file_name)

    if not source_blob.exists():
        log_event("WARNING", f"Cannot archive {file_name}, file not found.", trace_id)
        return

    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
    new_name = f"{folder}/{os.path.basename(file_name)}_{ts}"

    # Copy then Delete
    dest_bucket.copy_blob(source_blob, dest_bucket, new_name)
    source_blob.delete()

    log_event("INFO", f"📦 Archived to: {new_name}", trace_id)


def audit_log(
    trace_id,
    ingestion_id,
    file_name,
    file_uri,
    status,
    start_time,
    row_count=None,
    target_table=None,
    error_msg=None,
):
    bq, _ = get_clients()

    # Ensure start_time is a string
    if isinstance(start_time, datetime.datetime):
        start_time_str = start_time.isoformat()
    else:
        start_time_str = str(start_time)

    row = {
        "ingestion_id": ingestion_id,
        "file_name": file_name,
        "file_uri": file_uri,
        "status": status,
        "row_count": row_count,
        "target_table": target_table,
        "error_message": error_msg[:2000] if error_msg else None,
        "start_time": start_time_str,
        "end_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # Ignore unknown values to prevent audit logging from failing the pipeline
        ignore_unknown_values=True,
    )

    table_id = f"{PROJECT_ID}.{METADATA_DATASET}.{LOGS_TABLE}"

    try:
        json_data = json.dumps(row) + "\n"
        file_obj = io.StringIO(json_data)
        job = bq.load_table_from_file(file_obj, table_id, job_config=job_config)
        job.result()  # Wait for it to confirm success
        log_event("INFO", "🟢 Audit Log Saved", trace_id)
    except Exception as e:
        # Just print error, don't crash main thread
        print(f"FAILED TO WRITE AUDIT LOG: {e}")


def handle_failure(
    bucket, file_name, status, error_msg, trace_id, ingestion_id, start_time
):
    """
    Safe failure handling that swallows errors to prevent retry loops.
    """
    try:
        # 1. Archive to 'exempted'
        try:
            archive_file(bucket, file_name, "exempted", trace_id)
        except Exception as e:
            log_event("WARNING", f"Could not archive failed file: {e}", trace_id)

        # 2. Log to BigQuery
        file_uri = f"gs://{bucket}/{file_name}"
        audit_log(
            trace_id=trace_id,
            ingestion_id=ingestion_id,
            file_name=file_name,
            file_uri=file_uri,
            status=status,
            start_time=start_time,
            error_msg=error_msg,
        )

        log_event("INFO", "🛑 Failure Handled. Exiting Gracefully.", trace_id)

    except Exception as e:
        # CRITICAL: Do not let this function raise an exception.
        # If it does, Cloud Functions will RETRY the whole event.
        print(f"CRITICAL: Error handler crashed. Swallowing error to stop loop. {e}")


# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================
@functions_framework.cloud_event
def process_file(cloud_event):
    # Initialize Context
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
    # Stops infinite loops if writing to same bucket
    if "processed/" in file_name or "exempted/" in file_name:
        print(f"🚫 Ignoring internal event: {file_name}")
        return

    # --- CIRCUIT BREAKER 3: Phantom File Check ---
    # If file was already moved/deleted by a previous retry, stop now.
    _, storage = get_clients()
    if not storage.bucket(bucket_name).blob(file_name).exists():
        print(f"👻 File not found: {file_name}. Stopping to prevent retry loop.")
        return

    log_event("INFO", f"🚀 Started Processing: {file_name}", trace_id)

    try:
        # 1. ROUTER
        rule = get_routing_rule(file_name)
        if not rule:
            handle_failure(
                bucket_name,
                file_name,
                "SKIPPED",
                "No matching routing rule",
                trace_id,
                ingestion_id,
                start_time,
            )
            return

        final_table_ref = (
            f"{PROJECT_ID}.{rule['target_dataset']}.{rule['target_table']}"
        )
        log_event("INFO", f"🎯 Routing to: {final_table_ref}", trace_id)

        # 2. LOADER
        row_count = load_raw_strings(
            bucket_name, file_name, rule, final_table_ref, trace_id
        )

        # 3. ARCHIVER
        archive_file(bucket_name, file_name, "processed", trace_id)

        # 4. AUDIT
        uri = f"gs://{bucket_name}/{file_name}"
        audit_log(
            trace_id=trace_id,
            ingestion_id=ingestion_id,
            file_name=file_name,
            file_uri=uri,
            status="SUCCESS",
            start_time=start_time,
            row_count=row_count,
            target_table=final_table_ref,
        )
        log_event("INFO", "✅ Ingestion Complete.", trace_id)

    except Exception as e:
        error_msg = str(e)
        log_event("ERROR", f"❌ Pipeline Failed: {error_msg}", trace_id)

        # Pass ALL required context to the handler
        handle_failure(
            bucket_name,
            file_name,
            "FAILED",
            error_msg,
            trace_id,
            ingestion_id,
            start_time,
        )
