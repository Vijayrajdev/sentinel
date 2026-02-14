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
from google.api_core.exceptions import NotFound, BadRequest

# ==============================================================================
# CONFIGURATION
# ==============================================================================
bq_client: Optional[bigquery.Client] = None
storage_client: Optional[storage.Client] = None

PROJECT_ID = os.environ.get("GCP_PROJECT")
METADATA_DATASET = os.environ.get("METADATA_DATASET", "sentinel_audit")
MASTER_TABLE = os.environ.get("MASTER_TABLE", "ingestion_master")
LOGS_TABLE = os.environ.get("LOGS_TABLE", "ingestion_log")
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================
def get_clients():
    global bq_client, storage_client
    if not bq_client: bq_client = bigquery.Client()
    if not storage_client: storage_client = storage.Client()
    return bq_client, storage_client

def log_event(severity: str, message: str, trace_id: str, **kwargs):
    entry = {
        "severity": severity,
        "message": message,
        "logging.googleapis.com/trace": trace_id,
        "component": "sentinel-ingestor",
        **kwargs
    }
    print(json.dumps(entry))

def get_csv_headers(bucket: str, file_name: str, delimiter: str = ",") -> List[str]:
    """
    Downloads just the first line of the file to extract column names.
    This allows us to force 'STRING' type for all columns.
    """
    _, storage = get_clients()
    blob = storage.bucket(bucket).blob(file_name)
    
    # Download first 4KB (enough to cover a header row)
    # decode only the first line
    try:
        data = blob.download_as_bytes(start=0, end=4096).decode("utf-8")
        first_line = data.split('\n')[0]
        # Use csv reader to handle quoted headers correctly
        reader = csv.reader([first_line], delimiter=delimiter)
        headers = next(reader)
        return headers
    except Exception as e:
        raise ValueError(f"Could not read headers from file: {e}")

# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================
@functions_framework.cloud_event
def process_file(cloud_event):
    trace_id = f"projects/{PROJECT_ID}/traces/{uuid.uuid4().hex}"
    data = cloud_event.data
    
    bucket_name = data.get("bucket")
    file_name = data.get("name")
    
    if not file_name or file_name.endswith("/"):
        return

    log_event("INFO", f"🚀 Started Processing: gs://{bucket_name}/{file_name}", trace_id)
    log_event("INFO", f"🫆 Trace Id: {trace_id}", trace_id)

    try:
        # 1. ROUTER
        rule = get_routing_rule(file_name, trace_id)
        
        if not rule:
            handle_failure(bucket_name, file_name, "SKIPPED", "No matching routing rule", trace_id)
            return

        final_table_ref = f"{PROJECT_ID}.{rule['target_dataset']}.{rule['target_table']}"
        log_event("INFO", f"🎯 Routing to Raw Table: {final_table_ref}", trace_id)

        # 2. LOADER
        row_count = load_raw_strings(bucket_name, file_name, rule, final_table_ref, trace_id)

        # 3. ARCHIVER
        archive_file(bucket_name, file_name, "processed", trace_id)

        # 4. AUDIT
        audit_log(trace_id, file_name, "SUCCESS", row_count=row_count, target_table=final_table_ref)
        log_event("INFO", f"✅ Successfully inserted {row_count} records into table: {final_table_ref}.", trace_id)
        log_event("INFO", "📂 Ingestion Complete.", trace_id)

    except Exception as e:
        # STOP RETRY: Move to exempted and exit gracefully
        error_msg = str(e)
        log_event("ERROR", f"❌ Pipeline Failed: {error_msg}", trace_id)
        handle_failure(bucket_name, file_name, "FAILED", error_msg, trace_id)

# ==============================================================================
# CORE LOGIC: RAW STRING LOADER
# ==============================================================================

def load_raw_strings(bucket: str, file_name: str, rule: Dict[str, Any], final_table_ref: str, trace_id: str) -> int:
    """
    1. Reads Headers -> Creates Schema where ALL columns are STRING.
    2. Loads to Staging.
    3. Evolves Final Schema (adding new cols as STRING).
    4. Inserts (relying on DB Defaults for audit cols).
    """
    bq, _ = get_clients()
    staging_table_id = f"{PROJECT_ID}.{rule['target_dataset']}.staging_{rule['target_table']}_{uuid.uuid4().hex[:8]}"
    
    try:
        # --- STEP A: Force All-String Schema ---
        # We assume CSV for this logic. If JSON, we fall back to autodetect or different parser.
        file_format = rule.get("file_format", "CSV")
        job_config = bigquery.LoadJobConfig(
            source_format=getattr(bigquery.SourceFormat, file_format),
            skip_leading_rows=rule.get("skip_header_rows", 1),
            field_delimiter=rule.get("delimiter", ","),
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        if file_format == "CSV":
            headers = get_csv_headers(bucket, file_name, rule.get("delimiter", ","))
            # FORCE every column to be STRING (The "Raw Layer" Rule)
            job_config.schema = [bigquery.SchemaField(h, "STRING") for h in headers]
            job_config.autodetect = False # We provided schema, so turn this off
        else:
            # For JSON/Parquet, we usually rely on autodetect or implicit schema
            job_config.autodetect = True

        log_event("INFO", f"⏳ Loading to staging with STRING schema: {staging_table_id}", trace_id)
        
        uri = f"gs://{bucket}/{file_name}"
        load_job = bq.load_table_from_uri(uri, staging_table_id, job_config=job_config)
        load_job.result()
        
        # --- STEP B: Schema Evolution (String Only) ---
        staging_table = bq.get_table(staging_table_id)
        
        try:
            final_table = bq.get_table(final_table_ref)
        except NotFound:
            log_event("WARNING", "Target table missing. Creating from staging schema...", trace_id)
            # Create table using Staging schema (All Strings)
            # We DO NOT add audit cols here manually, assuming DDL created them or we let them be null initially
            # But better practice: If we create it fresh, we should probably add the audit definition.
            # For now, we clone the staging schema.
            bq.create_table(bigquery.Table(final_table_ref, schema=staging_table.schema))
            final_table = bq.get_table(final_table_ref)

        # Sync Columns
        final_cols = {f.name for f in final_table.schema}
        staging_cols = [f for f in staging_table.schema]
        
        new_columns = []
        for col in staging_cols:
            if col.name not in final_cols:
                # Ensure the new column is treated as STRING (inherits from staging)
                new_columns.append(col)
        
        if new_columns:
            log_event("WARNING", f"⚠️ Schema Drift: Adding {len(new_columns)} STRING columns.", trace_id)
            updated_schema = final_table.schema[:]
            updated_schema.extend(new_columns)
            final_table.schema = updated_schema
            bq.update_table(final_table, ["schema"])

        # --- STEP C: Insert (Using Table Defaults) ---
        # We Select ONLY the columns present in the file.
        # We do NOT select 'batch_date' or 'processed_dttm'.
        # BigQuery will see they are missing and fill them with DEFAULT values ('9999-12-31', NOW()).
        
        col_names = [f.name for f in staging_cols]
        cols_string = ", ".join([f"`{c}`" for c in col_names])
        
        insert_query = f"""
            INSERT INTO `{final_table_ref}` ({cols_string})
            SELECT {cols_string}
            FROM `{staging_table_id}`
        """
        
        log_event("INFO", "⏳ Executing Final Insert (Defaults applied)...", trace_id)
        bq.query(insert_query).result()
        
        return load_job.output_rows

    finally:
        bq.delete_table(staging_table_id, not_found_ok=True)

def get_routing_rule(file_name: str, trace_id: str) -> Optional[Dict[str, Any]]:
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
    if not ARCHIVE_BUCKET: return
    
    try:
        # 1. Define your bucket and blob objects
        src_bucket = storage.bucket(source_bucket)
        dest_bucket = storage.bucket(ARCHIVE_BUCKET)
        source_blob = src_bucket.blob(file_name) # This is the BLOB we want to copy
        
        # 2. Prepare the destination path
        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
        new_name = f"{folder}/{os.path.basename(file_name)}_{ts}"
        
        # 3. FIX: Use the blob object as the first argument
        # Syntax: destination_bucket.copy_blob(source_blob, destination_bucket, new_name)
        new_blob = dest_bucket.copy_blob(source_blob, dest_bucket, new_name)
        
        # 4. Delete the original only after successful copy
        source_blob.delete()
        
        log_event("INFO", f"📦 Archived: {new_name}", trace_id)
        
    except Exception as e:
        error_msg = str(e)
        log_event("ERROR", f"❌ Archival Failed: {error_msg}", trace_id)
        pass

def audit_log(trace_id, file_name, status, row_count=None, target_table=None, error_msg=None):
    bq, _ = get_clients()
    
    # 1. Prepare the data exactly like before
    row = {
        "ingestion_id": trace_id, 
        "file_name": file_name, 
        "status": status,
        "row_count": row_count, 
        "target_table": target_table, 
        "error_message": error_msg[:2000] if error_msg else None,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }

    # 2. Configure the Batch Load Job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # Adds to table
    )

    try:
        table_id = f"{PROJECT_ID}.{METADATA_DATASET}.{LOGS_TABLE}"
        
        # 3. Convert dict to a Newline Delimited JSON string in memory
        json_data = json.dumps(row) + "\n"
        file_obj = io.StringIO(json_data)

        # 4. Trigger the Load Job
        log_event("INFO", f"🚀 Starting Batch Audit Load: {trace_id}", trace_id)
        
        # We wrap the string in a Bio/StringIO to treat it like a file
        job = bq.load_table_from_file(
            file_obj, 
            table_id, 
            job_config=job_config
        )
        
        # 5. WAIT for the job to complete (Crucial for Batch)
        job.result() 
        
        log_event("INFO", "🟢 Batch Audit Insert Completed", trace_id)

    except Exception as e:
        log_event("ERROR", f"❌ Batch Audit Failed: {str(e)}", trace_id)

def handle_failure(bucket, file_name, status, error_msg, trace_id):
    try:
        archive_file(bucket, file_name, "exempted", trace_id)
        audit_log(trace_id, file_name, status, error_msg=error_msg)
        log_event("INFO", "🛑 Handled Failure. No Retry.", trace_id)
    except Exception: pass