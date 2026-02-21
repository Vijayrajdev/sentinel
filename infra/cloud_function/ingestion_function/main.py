import functions_framework
import json
import io
import logging
import os
import uuid
import datetime
import re
from typing import Dict, Optional, Any, List, Tuple

import pandas as pd
import gcsfs
import pyarrow.parquet as pq

from google.cloud import bigquery
from google.cloud import storage
from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound

# ==============================================================================
# CONFIGURATION
# ==============================================================================
bq_client: Optional[bigquery.Client] = None
storage_client: Optional[storage.Client] = None
publisher_client: Optional[pubsub_v1.PublisherClient] = None

PROJECT_ID = os.environ.get("GCP_PROJECT")
METADATA_DATASET = os.environ.get("METADATA_DATASET", "sentinel_audit")
MASTER_TABLE = os.environ.get("MASTER_TABLE", "ingestion_master")
LOGS_TABLE = os.environ.get("LOGS_TABLE", "ingestion_log")
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET")
PUBSUB_TOPIC_DRIFT = os.environ.get("PUBSUB_TOPIC_DRIFT")

logging.basicConfig(level=logging.INFO)


# ==============================================================================
# CUSTOM EXCEPTIONS
# ==============================================================================
class SchemaDriftError(Exception):
    def __init__(self, message, new_columns, sample_rows=None):
        super().__init__(message)
        self.new_columns = new_columns
        self.sample_rows = sample_rows


class TableNotFoundError(Exception):
    def __init__(self, message, columns=None, sample_rows=None):
        super().__init__(message)
        self.columns = columns
        self.sample_rows = sample_rows


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================
def get_clients():
    """Lazy initialization of GCP clients to leverage warm starts."""
    global bq_client, storage_client, publisher_client
    if not bq_client:
        bq_client = bigquery.Client()
    if not storage_client:
        storage_client = storage.Client()
    if not publisher_client:
        publisher_client = pubsub_v1.PublisherClient()
    return bq_client, storage_client, publisher_client


def log_event(severity: str, message: str, trace_id: str, **kwargs):
    """Structured JSON logging for Google Cloud Logging."""
    entry = {
        "severity": severity,
        "message": message,
        "logging.googleapis.com/trace": trace_id,
        "component": "sentinel-ingestor",
        **kwargs,
    }
    print(json.dumps(entry))


def get_file_metadata(
    bucket: str, file_name: str, rule: Dict[str, Any], trace_id: str
) -> Tuple[List[str], List[Dict]]:
    """Extracts headers and 5 sample rows using dynamic metadata."""
    log_event(
        "INFO",
        f"🎬 [Metadata] Initiating metadata extraction for gs://{bucket}/{file_name}",
        trace_id,
    )

    log_event(
        "INFO",
        "🎛️ [Config] Parsing file configuration properties from routing rule...",
        trace_id,
    )
    file_ext = rule.get("file_format")
    if not file_ext:
        file_ext = os.path.splitext(file_name)[1].lower().replace(".", "")
    else:
        file_ext = str(file_ext).lower().replace(".", "")

    delimiter = rule.get("delimiter") or ","
    quote_char = rule.get("quote_char") or '"'

    raw_skip = rule.get("skip_header_rows")
    skip_header_rows = (
        int(raw_skip) if pd.notna(raw_skip) and raw_skip is not None else 1
    )
    pd_skip_rows = max(0, skip_header_rows - 1)

    uri = f"gs://{bucket}/{file_name}"

    log_event(
        "INFO",
        f"🐼 [Pandas] Setting up extraction. Type: {file_ext}, Delim: '{delimiter}', Skip: {pd_skip_rows}",
        trace_id,
    )

    try:
        log_event("INFO", "🧠 [Memory] Loading target rows into DataFrame...", trace_id)
        if file_ext == "csv":
            df = pd.read_csv(
                uri,
                nrows=5,
                sep=delimiter,
                quotechar=quote_char,
                skiprows=pd_skip_rows,
                dtype=str,
            )
        elif file_ext in ["json", "ndjson"]:
            df = pd.read_json(uri, lines=True, nrows=5, dtype=str)
        elif file_ext == "parquet":
            fs = gcsfs.GCSFileSystem()
            with fs.open(uri, "rb") as f:
                pf = pq.ParquetFile(f)
                df = pf.read_row_group(0).to_pandas().head(5).astype(str)
        elif file_ext in ["xlsx", "xls"]:
            df = pd.read_excel(uri, nrows=5, skiprows=pd_skip_rows, dtype=str)
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")

        log_event(
            "INFO",
            "🧬 [Extract] DataFrame secured. Extracting exact column headers...",
            trace_id,
        )
        headers = df.columns.tolist()

        df = df.where(pd.notnull(df), None)
        samples = df.to_dict(orient="records")

        log_event(
            "INFO",
            f"🏁 [Metadata] Extraction complete. Successfully retrieved {len(headers)} headers.",
            trace_id,
        )
        return headers, samples
    except Exception as e:
        log_event(
            "ERROR",
            f"🩸 [Metadata Fail] Exception during data read: {str(e)}",
            trace_id,
        )
        raise Exception(f"Failed to read metadata from {file_name}: {str(e)}")


def trigger_ai_agent(bucket, file_name, table_ref, new_columns, sample_data, trace_id):
    """Summons the AI Agent via Pub/Sub."""
    log_event(
        "INFO", f"📡 [AI Trigger] Waking up Sentinel Forge for {table_ref}...", trace_id
    )
    _, _, publisher = get_clients()

    if not PUBSUB_TOPIC_DRIFT:
        log_event(
            "WARNING",
            "🔕 [PubSub] Topic unconfigured. Agent invocation aborted.",
            trace_id,
        )
        return

    log_event(
        "INFO", "🏗️ [Payload] Assembling schema drift telemetry payload...", trace_id
    )
    message = {
        "event_type": "SCHEMA_DRIFT_AI",
        "trace_id": trace_id,
        "bucket": bucket,
        "file_name": file_name,
        "table_ref": table_ref,
        "new_column_headers": new_columns,
        "sample_data_rows": sample_data,
        "timestamp": datetime.datetime.now().isoformat(),
    }

    try:
        data = json.dumps(message).encode("utf-8")
        future = publisher.publish(PUBSUB_TOPIC_DRIFT, data)
        msg_id = future.result()
        log_event(
            "INFO",
            f"🕊️ [Publish] Transmission sent to Event Mesh. ID: {msg_id}",
            trace_id,
        )
        log_event("INFO", "📫 [Sent] AI Agent successfully summoned.", trace_id)
    except Exception as e:
        log_event("ERROR", f"🔇 [Publish Fail] Messaging system failure: {e}", trace_id)


def validate_schema(final_table_ref: str, file_headers: List[str], trace_id: str):
    """Validates incoming file headers against BigQuery destination."""
    log_event(
        "INFO",
        f"🔬 [Validate] Commencing strict schema validation for {final_table_ref}...",
        trace_id,
    )
    bq, _, _ = get_clients()

    try:
        log_event(
            "INFO",
            "🏛️ [BQ Schema] Downloading current structural definition from Data Warehouse...",
            trace_id,
        )
        table = bq.get_table(final_table_ref)
    except NotFound:
        log_event(
            "WARNING",
            f"👻 [Not Found] Destination table {final_table_ref} is completely missing.",
            trace_id,
        )
        raise TableNotFoundError(f"Target table '{final_table_ref}' does not exist.")

    log_event(
        "INFO",
        "⚖️ [Compare] Executing diff between file headers and warehouse columns...",
        trace_id,
    )
    bq_columns = {schema_field.name for schema_field in table.schema}
    file_columns = set(file_headers)
    unknown_cols = list(file_columns - bq_columns)

    if unknown_cols:
        log_event(
            "WARNING",
            f"🌪️ [Drift] Structural mutation identified. Rogue columns: {unknown_cols}",
            trace_id,
        )
        raise SchemaDriftError(
            f"Schema Drift Detected. New columns: {unknown_cols}",
            new_columns=unknown_cols,
        )

    log_event(
        "INFO", "🛡️ [Secure] Parity confirmed. Data structure is stable.", trace_id
    )
    log_event("INFO", "👍 [Validate] Verification sequence concluded.", trace_id)


# ==============================================================================
# CORE LOGIC: DYNAMIC LOADER
# ==============================================================================
def load_raw_strings(
    bucket: str,
    file_name: str,
    rule: Dict[str, Any],
    final_table_ref: str,
    trace_id: str,
) -> Dict[str, int]:
    log_event(
        "INFO",
        f"🚚 [Loader] Booting primary ingestion engine for {file_name}...",
        trace_id,
    )
    bq, _, _ = get_clients()
    uri = f"gs://{bucket}/{file_name}"

    log_event(
        "INFO", "🔧 [Extract Config] Pulling dynamic parsing directives...", trace_id
    )
    file_ext = rule.get("file_format")
    if not file_ext:
        file_ext = os.path.splitext(file_name)[1].lower().replace(".", "")
    else:
        file_ext = str(file_ext).lower().replace(".", "")

    delimiter = rule.get("delimiter") or ","
    quote_char = rule.get("quote_char") or '"'

    raw_skip = rule.get("skip_header_rows")
    skip_header_rows = (
        int(raw_skip) if pd.notna(raw_skip) and raw_skip is not None else 1
    )
    write_disp = str(rule.get("write_disposition") or "WRITE_APPEND").upper()

    headers, sample_rows = get_file_metadata(bucket, file_name, rule, trace_id)

    try:
        validate_schema(final_table_ref, headers, trace_id)
    except SchemaDriftError as e:
        log_event(
            "WARNING",
            "🪁 [Drift Catch] Interrupting load. Packaging drift telemetry...",
            trace_id,
        )
        filtered_samples = [
            {k: row.get(k) for k in e.new_columns} for row in sample_rows
        ]
        raise SchemaDriftError(str(e), e.new_columns, filtered_samples)
    except TableNotFoundError as e:
        log_event(
            "WARNING",
            "🕳️ [Missing Catch] Target void. Packaging creation telemetry...",
            trace_id,
        )
        raise TableNotFoundError(str(e), columns=headers, sample_rows=sample_rows)

    staging_table_id = f"{PROJECT_ID}.{rule['target_dataset']}.staging_{rule['target_table']}_{uuid.uuid4().hex[:8]}"
    log_event(
        "INFO",
        f"🪣 [Staging] Allocating temporary sandbox: {staging_table_id}",
        trace_id,
    )

    try:
        if file_ext == "csv":
            log_event("INFO", "📝 [CSV] Preparing text-delimited load job...", trace_id)
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=skip_header_rows,
                field_delimiter=delimiter,
                quote_character=quote_char,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                schema=[bigquery.SchemaField(h, "STRING") for h in headers],
                autodetect=False,
            )
            bq.load_table_from_uri(
                uri, staging_table_id, job_config=job_config
            ).result()

        elif file_ext in ["json", "ndjson"]:
            log_event(
                "INFO", "📜 [JSON] Preparing document-structured load job...", trace_id
            )
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                schema=[bigquery.SchemaField(h, "STRING") for h in headers],
                ignore_unknown_values=True,
            )
            bq.load_table_from_uri(
                uri, staging_table_id, job_config=job_config
            ).result()

        elif file_ext == "parquet":
            log_event(
                "INFO",
                "🗜️ [Parquet] Preparing columnar-compressed load job...",
                trace_id,
            )
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            bq.load_table_from_uri(
                uri, staging_table_id, job_config=job_config
            ).result()

        elif file_ext in ["xlsx", "xls"]:
            log_event(
                "INFO", "📊 [Excel] Preparing spreadsheet-memory load job...", trace_id
            )
            pd_skip_rows = max(0, skip_header_rows - 1)
            df = pd.read_excel(uri, skiprows=pd_skip_rows, dtype=str)
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                schema=[bigquery.SchemaField(h, "STRING") for h in headers],
            )
            bq.load_table_from_dataframe(
                df, staging_table_id, job_config=job_config
            ).result()

        log_event(
            "INFO",
            "📥 [Ingested] Raw bytes successfully transferred to sandbox.",
            trace_id,
        )

        log_event("INFO", "🧮 [Metrics] Calculating data quality vectors...", trace_id)
        conditions = [
            f"(COALESCE(CAST(`{col}` AS STRING), '') = '')" for col in headers
        ]
        bad_record_condition = " OR ".join(conditions)

        quality_query = f"""
            SELECT COUNT(*) as total_cnt, COUNTIF({bad_record_condition}) as bad_cnt
            FROM `{staging_table_id}`
        """
        log_event(
            "INFO", "🩺 [Health Check] Running statistical diagnostic...", trace_id
        )
        res = bq.query(quality_query).result()
        metrics = {"total": 0, "bad": 0, "good": 0}
        for row in res:
            metrics["total"] = row.total_cnt
            metrics["bad"] = row.bad_cnt
            metrics["good"] = row.total_cnt - row.bad_cnt

        log_event(
            "INFO", f"📈 [Result] Diagnostic complete: {json.dumps(metrics)}", trace_id
        )

        safe_cols = [f"`{c}`" for c in headers]
        cols_string = ", ".join(safe_cols)

        if write_disp == "WRITE_TRUNCATE":
            log_event(
                "INFO",
                f"💥 [Truncate] Wiping target {final_table_ref} (Policy: WRITE_TRUNCATE)...",
                trace_id,
            )
            bq.query(f"TRUNCATE TABLE `{final_table_ref}`").result()

        insert_query = f"""
            INSERT INTO `{final_table_ref}` ({cols_string})
            SELECT {cols_string}
            FROM `{staging_table_id}`
        """
        log_event(
            "INFO",
            f"🎯 [Smart Insert] Projecting data into refined target {final_table_ref}...",
            trace_id,
        )
        bq.query(insert_query).result()

        log_event("INFO", "🎉 [Loader] Core ingestion completely successful.", trace_id)
        return metrics

    except Exception as e:
        log_event(
            "ERROR",
            f"☠️ [Loader Fail] Catastrophic fault in processing engine: {e}",
            trace_id,
        )
        raise
    finally:
        log_event(
            "INFO", f"🧹 [Cleanup] De-allocating temporary resources...", trace_id
        )
        try:
            bq.delete_table(staging_table_id, not_found_ok=True)
            log_event("INFO", "✨ [Spotless] Sandbox dropped.", trace_id)
        except Exception as cleanup_err:
            log_event(
                "WARNING",
                f"🦠 [Residue] Failed to drop staging table: {cleanup_err}",
                trace_id,
            )


def get_routing_rule(
    bucket_name: str, file_name: str, trace_id: str
) -> Optional[Dict[str, Any]]:
    """Fetches routing rules from BigQuery."""
    log_event(
        "INFO", f"🔀 [Router] Searching directory matrix for {file_name}...", trace_id
    )
    bq, _, _ = get_clients()
    query = f"SELECT * FROM `{PROJECT_ID}.{METADATA_DATASET}.{MASTER_TABLE}` WHERE is_active = TRUE"

    log_event("INFO", "🔎 [Search] Scanning ingestion_master registry...", trace_id)
    try:
        results = bq.query(query).result()
        for row in results:
            rule_bucket = row.get("landing_bucket")
            if rule_bucket and pd.notna(rule_bucket) and rule_bucket != bucket_name:
                continue

            if re.search(row["file_pattern"], file_name):
                log_event(
                    "INFO",
                    f"📌 [Matched] Confirmed routing pattern: {row['file_pattern']}",
                    trace_id,
                )
                log_event("INFO", "🚦 [Routed] Pathing established.", trace_id)
                return dict(row)

        log_event("INFO", "🤷 [Miss] No registry match for this file format.", trace_id)
        return None
    except Exception as e:
        log_event(
            "ERROR", f"🚧 [Router Fail] Registry offline or corrupted: {e}", trace_id
        )
        raise


def archive_file(
    source_bucket: str,
    file_name: str,
    folder: str,
    trace_id: str,
    dest_archive_bucket: str = None,
) -> Optional[str]:
    """Moves file to target archive bucket."""
    log_event(
        "INFO",
        f"📦 [Archive] Initiating cold storage protocol for '{folder}'...",
        trace_id,
    )
    _, storage, _ = get_clients()

    target_bucket_name = dest_archive_bucket or ARCHIVE_BUCKET
    if not target_bucket_name or pd.isna(target_bucket_name):
        log_event(
            "WARNING", "🚫 [No Dest] Storage vault undefined. Bypassing.", trace_id
        )
        return None

    log_event("INFO", "🧳 [Pack] Validating object mobility...", trace_id)
    try:
        src_bucket = storage.bucket(source_bucket)
        dest_bucket = storage.bucket(target_bucket_name)
        source_blob = src_bucket.blob(file_name)

        if not source_blob.exists():
            log_event(
                "WARNING",
                f"🌫️ [Vaporized] Object gs://{source_bucket}/{file_name} is gone.",
                trace_id,
            )
            return None

        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
        new_name = f"{folder}/{os.path.basename(file_name)}_{ts}"
        new_uri = f"gs://{target_bucket_name}/{new_name}"

        log_event("INFO", f"🛸 [Transfer] Relocating bytes to {new_uri}...", trace_id)
        dest_bucket.copy_blob(source_blob, dest_bucket, new_name)

        log_event("INFO", "🗑️ [Delete] Purging origin file...", trace_id)
        source_blob.delete()

        log_event("INFO", "🏦 [Vaulted] Archival procedure sealed.", trace_id)
        return new_uri
    except Exception as e:
        log_event(
            "ERROR", f"🧊 [Archive Fail] Storage API rejected move: {e}", trace_id
        )
        raise


def audit_log(
    trace_id,
    ingestion_id,
    file_name,
    file_uri,
    status,
    start_time,
    metrics=None,
    target_table=None,
    error_msg=None,
):
    """Writes standardized audit trace into BigQuery."""
    log_event(
        "INFO",
        f"🧾 [Audit] Generating compliance manifest for {ingestion_id}...",
        trace_id,
    )
    bq, _, _ = get_clients()
    if metrics is None:
        metrics = {"total": 0, "good": 0, "bad": 0}

    row = {
        "ingestion_id": ingestion_id,
        "file_name": file_name,
        "file_uri": file_uri,
        "status": status,
        "target_table": target_table,
        "error_message": error_msg[:2000] if error_msg else None,
        "start_time": (
            start_time.isoformat()
            if isinstance(start_time, datetime.datetime)
            else str(start_time)
        ),
        "end_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "total_records": metrics.get("total", 0),
        "processed_records": metrics.get("total", 0),
        "good_records": metrics.get("good", 0),
        "bad_records": metrics.get("bad", 0),
    }

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
    )
    table_id = f"{PROJECT_ID}.{METADATA_DATASET}.{LOGS_TABLE}"

    log_event("INFO", "✒️ [Write] Engraving record to operational ledger...", trace_id)
    try:
        json_data = json.dumps(row) + "\n"
        bq.load_table_from_file(
            io.StringIO(json_data), table_id, job_config=job_config
        ).result()
        log_event("INFO", "🔏 [Recorded] Ledger updated.", trace_id)
        log_event("INFO", "📁 [Audit Done] Compliance protocol fulfilled.", trace_id)
    except Exception as e:
        log_event("CRITICAL", f"🔥 [Audit Fail] SEVERE COMPLIANCE ERROR: {e}", trace_id)


def handle_failure(
    bucket,
    file_name,
    status,
    error_msg,
    trace_id,
    ingestion_id,
    start_time,
    target_folder="exempted",
    archive_bucket=None,
):
    """Fallback handler for clean archiving of broken files."""
    log_event(
        "INFO",
        f"🛟 [Rescue] Initiating catastrophe containment for {file_name}...",
        trace_id,
    )
    try:
        log_event(
            "INFO", "🚑 [Evacuate] Moving corrupted payload to quarantine...", trace_id
        )
        final_uri = (
            archive_file(bucket, file_name, target_folder, trace_id, archive_bucket)
            or f"gs://{bucket}/{file_name} (Archive Failed)"
        )

        log_event(
            "INFO", "🏥 [Chart] Documenting casualties in audit system...", trace_id
        )
        audit_log(
            trace_id,
            ingestion_id,
            file_name,
            final_uri,
            status,
            start_time,
            error_msg=error_msg,
        )

        log_event("INFO", "🩹 [Stabilized] Threat neutralized. System ready.", trace_id)
    except Exception as e:
        log_event(
            "CRITICAL",
            f"💣 [Rescue Fail] Absolute failure in containment: {e}",
            trace_id,
        )


# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================
@functions_framework.cloud_event
def process_file(cloud_event):
    start_time = datetime.datetime.now(datetime.timezone.utc)
    ingestion_id = uuid.uuid4().hex
    trace_id = f"projects/{PROJECT_ID}/traces/{ingestion_id}"

    data = cloud_event.data
    bucket_name = data.get("bucket")
    file_name = data.get("name")

    if not file_name or file_name.endswith("/"):
        return
    if any(
        folder in file_name
        for folder in ["processed/", "exempted/", "unprocessed/", "schema_pending/"]
    ):
        return

    _, storage, _ = get_clients()
    if not storage.bucket(bucket_name).blob(file_name).exists():
        return

    log_event(
        "INFO",
        f"🚀 [Cloud Event] Payload detected. Commencing Operation: {file_name}",
        trace_id,
    )
    final_table_ref = "UNKNOWN"
    rule_archive_bucket = None

    try:
        log_event("INFO", "🗺️ [Step 1] Requesting navigation vectors...", trace_id)
        rule = get_routing_rule(bucket_name, file_name, trace_id)
        if not rule:
            log_event("WARNING", "🛑 [Halt] Rogue file. Aborting ingestion.", trace_id)
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

        rule_archive_bucket = rule.get("archive_bucket")
        final_table_ref = (
            f"{PROJECT_ID}.{rule['target_dataset']}.{rule['target_table']}"
        )

        log_event(
            "INFO", "⚙️ [Step 2] Executing dynamic transformation sequence...", trace_id
        )
        metrics = load_raw_strings(
            bucket_name, file_name, rule, final_table_ref, trace_id
        )

        log_event("INFO", "🗃️ [Step 3] Executing post-load sanitation...", trace_id)
        final_uri = archive_file(
            bucket_name, file_name, "processed", trace_id, rule_archive_bucket
        )

        log_event("INFO", "🏅 [Step 4] Finalizing mission reports...", trace_id)
        audit_log(
            trace_id,
            ingestion_id,
            file_name,
            final_uri,
            "SUCCESS",
            start_time,
            metrics,
            final_table_ref,
        )

        log_event(
            "INFO",
            "🏆 [Pipeline Success] Event resolved flawlessly. Standing by.",
            trace_id,
        )

    except SchemaDriftError as e:
        error_msg = f"Drift Detected: {e.new_columns}"
        log_event(
            "WARNING", f"🌀 [Catch Drift] Protocol interrupted: {error_msg}", trace_id
        )
        trigger_ai_agent(
            bucket_name,
            file_name,
            final_table_ref,
            e.new_columns,
            e.sample_rows,
            trace_id,
        )
        handle_failure(
            bucket_name,
            file_name,
            "DRIFT_DETECTED",
            error_msg,
            trace_id,
            ingestion_id,
            start_time,
            "schema_pending",
            rule_archive_bucket,
        )

    except TableNotFoundError as e:
        error_msg = f"Table Missing: {str(e)}"
        log_event("WARNING", f"🕳️ [Catch Missing] Target absent: {error_msg}", trace_id)
        trigger_ai_agent(
            bucket_name, file_name, final_table_ref, e.columns, e.sample_rows, trace_id
        )
        handle_failure(
            bucket_name,
            file_name,
            "TABLE_MISSING",
            error_msg,
            trace_id,
            ingestion_id,
            start_time,
            "schema_pending",
            rule_archive_bucket,
        )

    except Exception as e:
        error_msg = str(e)
        log_event(
            "ERROR", f"🌋 [Catch Fatal] Unrecoverable Exception: {error_msg}", trace_id
        )
        handle_failure(
            bucket_name,
            file_name,
            "FAILED",
            error_msg,
            trace_id,
            ingestion_id,
            start_time,
            "exempted",
            rule_archive_bucket,
        )
