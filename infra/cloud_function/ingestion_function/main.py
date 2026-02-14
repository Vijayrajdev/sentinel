import functions_framework
import json
import logging
import os
import sys
import uuid
import datetime
import re
from typing import Dict, Optional, Any, Tuple

from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import GoogleAPICallError, NotFound

# ==============================================================================
# CONFIGURATION & GLOBAL CLIENTS
# ==============================================================================
# We initialize clients globally to leverage "Warm Starts" for performance.
# However, we allow them to be lazy-loaded to prevent startup crashes if env vars are missing.

bq_client: Optional[bigquery.Client] = None
storage_client: Optional[storage.Client] = None

# Environment Variables (Fail fast if critical ones are missing)
PROJECT_ID = os.environ.get("GCP_PROJECT")
METADATA_DATASET = os.environ.get("METADATA_DATASET", "sentinel_audit")
MASTER_TABLE = os.environ.get("MASTER_TABLE", "ingestion_master")
LOGS_TABLE = os.environ.get("LOGS_TABLE", "ingestion_logs")
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET")

# Logging Setup
# We configure the root logger to output JSON for Cloud Logging to parse correctly.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sentinel-ingestor")

def get_bq_client() -> bigquery.Client:
    global bq_client
    if not bq_client:
        bq_client = bigquery.Client()
    return bq_client

def get_storage_client() -> storage.Client:
    global storage_client
    if not storage_client:
        storage_client = storage.Client()
    return storage_client

# ==============================================================================
# HELPER: STRUCTURED LOGGING
# ==============================================================================
def log_event(severity: str, message: str, trace_id: str, **kwargs):
    """
    Emits a structured JSON log entry.
    Cloud Logging automatically picks up 'severity', 'message', and extra fields.
    """
    entry = {
        "severity": severity,
        "message": message,
        "logging.googleapis.com/trace": trace_id,
        "component": "sentinel-ingestor",
        **kwargs
    }
    # We use json.dumps ensures it's a single line for the log collector
    print(json.dumps(entry))

# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================
@functions_framework.cloud_event
def process_file(cloud_event):
    """
    Triggered by a Google Cloud Storage object finalization event.
    """
    # 1. Context Setup
    trace_id = f"projects/{PROJECT_ID}/traces/{uuid.uuid4().hex}"
    data = cloud_event.data
    
    bucket_name = data.get("bucket")
    file_name = data.get("name")
    
    # Validation: Ignore folders or empty names
    if not file_name or file_name.endswith("/"):
        log_event("INFO", "Ignoring folder or empty file.", trace_id, file=file_name)
        return

    log_event("INFO", f"🚀 Received Trigger: gs://{bucket_name}/{file_name}", trace_id)

    try:
        # 2. ROUTER: Determine where this file goes
        rule = get_routing_rule(file_name, trace_id)
        
        if not rule:
            error_msg = f"No routing rule found for file: {file_name}"
            log_event("WARNING", error_msg, trace_id)
            
            # Action: Move to 'exempted' so landing zone stays clean
            archive_file(bucket_name, file_name, "exempted", trace_id)
            
            # Action: Log Audit
            audit_log(trace_id, file_name, "SKIPPED", error_msg=error_msg)
            return

        target_ref = f"{rule['target_dataset']}.{rule['target_table']}"
        log_event("INFO", f"🎯 Match Found. Routing to {target_ref}", trace_id)

        # 3. LOADER: Load data into BigQuery
        row_count = load_to_bigquery(bucket_name, file_name, rule, trace_id)

        # 4. ARCHIVER: Move to processed
        archive_file(bucket_name, file_name, "processed", trace_id)

        # 5. AUDIT: Success Log
        audit_log(
            trace_id, 
            file_name, 
            "SUCCESS", 
            row_count=row_count, 
            target_table=target_ref
        )
        log_event("INFO", "✅ Ingestion Pipeline Completed Successfully.", trace_id)

    except Exception as e:
        error_msg = str(e)
        log_event("ERROR", f"❌ Critical Failure: {error_msg}", trace_id, exc_info=True)
        
        # Action: Move to 'exempted' (Or 'failed' if you prefer a separate bucket)
        # We try-except this because if archiving fails, we still want to log the original error
        try:
            archive_file(bucket_name, file_name, "exempted", trace_id)
        except Exception as archive_error:
            log_event("ERROR", f"Double Fault: Could not archive failed file. {archive_error}", trace_id)

        # Action: Audit Log
        audit_log(trace_id, file_name, "FAILED", error_msg=error_msg)
        
        # Re-raise to signal Cloud Functions environment of failure (triggers Retry if enabled)
        raise e

# ==============================================================================
# LOGIC: ROUTING & METADATA
# ==============================================================================
def get_routing_rule(file_name: str, trace_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetches active rules from BigQuery and matches against the filename regex.
    """
    client = get_bq_client()
    
    query = f"""
        SELECT file_pattern, target_dataset, target_table, file_format, delimiter, skip_header_rows
        FROM `{PROJECT_ID}.{METADATA_DATASET}.{MASTER_TABLE}`
        WHERE is_active = TRUE
    """
    
    try:
        # In a high-scale prod env, implement a Redis cache or in-memory TTL cache here
        # to avoid hitting BQ for every single file.
        job = client.query(query)
        results = job.result()
        
        for row in results:
            pattern = row["file_pattern"]
            if re.search(pattern, file_name):
                return {
                    "target_dataset": row["target_dataset"],
                    "target_table": row["target_table"],
                    "file_format": row["file_format"] or "CSV",
                    "delimiter": row["delimiter"] or ",",
                    "skip_header_rows": row["skip_header_rows"] if row["skip_header_rows"] is not None else 1
                }
        return None

    except Exception as e:
        log_event("ERROR", f"Failed to query routing rules: {e}", trace_id)
        raise e

# ==============================================================================
# LOGIC: BIGQUERY LOADING
# ==============================================================================
def load_to_bigquery(bucket: str, file_name: str, rule: Dict[str, Any], trace_id: str) -> int:
    """
    Configures and executes the Load Job with Schema Evolution.
    """
    client = get_bq_client()
    uri = f"gs://{bucket}/{file_name}"
    table_id = f"{PROJECT_ID}.{rule['target_dataset']}.{rule['target_table']}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=getattr(bigquery.SourceFormat, rule["file_format"]),
        skip_leading_rows=rule["skip_header_rows"],
        field_delimiter=rule["delimiter"],
        
        # SCHEMA EVOLUTION: This allows new columns to be added automatically
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ],
        
        # We use autodetect=True for simplicity in the generic loader.
        # Ideally, 'raw' layers should treat columns as STRING to avoid casting errors,
        # but BQ autodetect is usually smart enough for CSVs.
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    try:
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        log_event("INFO", f"⏳ Load Job Running: {load_job.job_id}", trace_id)
        
        load_job.result()  # Wait for completion
        
        row_count = load_job.output_rows
        log_event("INFO", f"✅ Loaded {row_count} rows into {table_id}", trace_id)
        return row_count

    except Exception as e:
        log_event("ERROR", f"Load Job Failed: {e}", trace_id)
        # We can inspect load_job.errors here for more detail if needed
        raise e

# ==============================================================================
# LOGIC: FILE ARCHIVING
# ==============================================================================
def archive_file(source_bucket: str, file_name: str, folder: str, trace_id: str):
    """
    Moves file to Archive Bucket under specific folder (processed/exempted).
    Appends timestamp to filename to prevent collisions.
    """
    if not ARCHIVE_BUCKET:
        log_event("WARNING", "Skipping Archive: ARCHIVE_BUCKET env var not set.", trace_id)
        return

    client = get_storage_client()
    
    try:
        src_bucket_obj = client.bucket(source_bucket)
        src_blob = src_bucket_obj.blob(file_name)
        
        dest_bucket_obj = client.bucket(ARCHIVE_BUCKET)
        
        # Rename: folder/filename_YYYYMMDDHHMMSS.ext
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        base_name, ext = os.path.splitext(os.path.basename(file_name))
        new_name = f"{folder}/{base_name}_{timestamp}{ext}"
        
        # Copy and Delete (Move)
        src_bucket_obj.copy_blob(src_blob, dest_bucket_obj, new_name)
        src_blob.delete()
        
        log_event("INFO", f"📦 Archived file to gs://{ARCHIVE_BUCKET}/{new_name}", trace_id)

    except NotFound:
        log_event("WARNING", f"File not found during archive (maybe already moved): {file_name}", trace_id)
    except Exception as e:
        log_event("ERROR", f"Failed to archive file: {e}", trace_id)
        raise e

# ==============================================================================
# LOGIC: AUDIT LOGGING
# ==============================================================================
def audit_log(trace_id: str, file_name: str, status: str, row_count: int = None, target_table: str = None, error_msg: str = None):
    """
    Writes a persistent record to the Audit Table in BigQuery.
    """
    client = get_bq_client()
    table_ref = f"{PROJECT_ID}.{METADATA_DATASET}.{LOGS_TABLE}"
    
    row = {
        "ingestion_id": trace_id, # We use the Trace ID as the unique key
        "file_name": file_name,
        "status": status,
        "row_count": row_count,
        "target_table": target_table,
        "error_message": error_msg[:1000] if error_msg else None, # Truncate massive error stacks
        "created_at": datetime.datetime.utcnow().isoformat()
    }
    
    errors = client.insert_rows_json(table_ref, [row])
    
    if errors:
        log_event("ERROR", f"Failed to write Audit Log: {errors}", trace_id)
    else:
        log_event("DEBUG", "Audit Log written.", trace_id)