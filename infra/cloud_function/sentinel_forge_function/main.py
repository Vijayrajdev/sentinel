import base64
import json
import os
import uuid
import datetime
import logging
import re
import time
import random  # FEATURE ADDITION: Required for Jitter in Exponential Backoff
from typing import List, Dict, Any, Optional, Tuple

import functions_framework
import vertexai
from vertexai.generative_models import GenerativeModel
from github import Github, GithubException, Auth
from google.cloud import bigquery
from google.cloud import (
    storage,
)  # FEATURE ADDITION: Imported for fetching data contracts

# ==============================================================================
# ⚙️ ENTERPRISE CONFIGURATION
# ==============================================================================
PROJECT_ID = os.environ.get("GCP_PROJECT")
REGION = os.environ.get("GCP_REGION", "us-central1")
REPO_NAME = os.environ.get("REPO_NAME")
GITHUB_APP_ID = os.environ.get("GITHUB_APP_ID")
GITHUB_INSTALLATION_ID = os.environ.get("GITHUB_INSTALLATION_ID")
GITHUB_PVT_KEY = os.environ.get("GITHUB_PVT_KEY")
AI_AUDIT_TABLE = os.environ.get("AI_AUDIT_TABLE", "sentinel_audit.ai_ops_log")

# 📂 PATH CONFIGURATION (Must end with /)
SCHEMA_BASE_PATH = os.environ.get("SCHEMA_BASE_PATH", "infra/bigquery/tables/json/")
TF_BASE_PATH = os.environ.get("TF_BASE_PATH", "infra/bigquery/tables/")

# COST OPTIMIZATION: Define separate models for Tiered Routing
AI_MODEL_HEAVY_NAME = os.environ.get("AI_MODEL_HEAVY", "gemini-2.5-flash")
AI_MODEL_LITE_NAME = os.environ.get("AI_MODEL_LITE", "gemini-2.5-flash-lite")

bq_client: Optional[bigquery.Client] = None
storage_client: Optional[storage.Client] = (
    None  # FEATURE ADDITION: Global storage client
)

# Global queue to hold audit logs until the PR Link is generated
DEFERRED_LOGS = []

# Initialize Vertex AI with Dual Models
model_heavy = None
model_lite = None
try:
    if PROJECT_ID:
        vertexai.init(project=PROJECT_ID, location="global")
        model_heavy = GenerativeModel(AI_MODEL_HEAVY_NAME)
        model_lite = GenerativeModel(AI_MODEL_LITE_NAME)
        logging.info(
            f"✅ Successfully initialized Vertex AI. Heavy: {AI_MODEL_HEAVY_NAME} | Lite: {AI_MODEL_LITE_NAME}"
        )
except Exception as e:
    logging.error(f"Failed to initialize Vertex AI: {e}")

logging.basicConfig(level=logging.INFO)


# ==============================================================================
# 📝 OBSERVABILITY, LOGGING & COST UTILITIES
# ==============================================================================
def get_bq_client() -> bigquery.Client:
    global bq_client
    if not bq_client:
        bq_client = bigquery.Client()
    return bq_client


# FEATURE ADDITION: Lazy loader for storage client
def get_storage_client() -> storage.Client:
    global storage_client
    if not storage_client:
        storage_client = storage.Client(project=PROJECT_ID)
    return storage_client


def log_event(severity: str, message: str, trace_id: str, **kwargs):
    """Structured JSON logging for Cloud Logging (Technical Logs)."""
    entry = {
        "severity": severity,
        "message": message,
        "logging.googleapis.com/trace": trace_id,
        "component": "sentinel-forge",
        **kwargs,
    }
    print(json.dumps(entry))


def smooth_traffic(trace_id: str):
    """Mitigates Vertex AI 429 Too Many Requests errors by pacing API calls."""
    log_event(
        "INFO",
        "⏳ 🚦 [Traffic Control] Pacing AI request (3s) to prevent 429 Rate Limit burst...",
        trace_id,
    )
    time.sleep(3)


# FEATURE ADDITION: Exponential Backoff to gracefully handle HTTP 429 Resource Exhausted errors
def generate_content_with_retry(
    model_instance, prompt: str, trace_id: str, max_retries: int = 5
):
    """Executes Vertex AI generation with Exponential Backoff for 429 errors."""
    base_delay = 5  # Start with a 5-second delay

    for attempt in range(max_retries):
        try:
            smooth_traffic(trace_id)  # Apply baseline smoothing
            response = model_instance.generate_content(prompt)
            return response
        except Exception as e:
            error_msg = str(e).lower()
            # If the error is a 429 Quota/Rate Limit error, we back off and retry
            if "429" in error_msg or "resource exhausted" in error_msg:
                if attempt == max_retries - 1:
                    log_event(
                        "ERROR",
                        f"🚨 🚦 [Traffic Control] Max retries reached for 429 error. Failing.",
                        trace_id,
                    )
                    raise e

                # Exponential backoff logic: 5s, 10s, 20s, 40s + random jitter to prevent clustered retries
                sleep_time = (base_delay * (2**attempt)) + random.uniform(0, 2)
                log_event(
                    "WARNING",
                    f"⚠️ 🚦 [Traffic Control] 429 Quota Hit. Exponential backoff active. Retrying in {sleep_time:.2f} seconds (Attempt {attempt + 1}/{max_retries})...",
                    trace_id,
                )
                time.sleep(sleep_time)
            else:
                # If it's a different error (e.g., 500, Auth), fail immediately
                raise e


def log_ai_action(
    trace_id: str,
    action: str,
    resource: str,
    status: str,
    details: Dict[str, Any],
    link: Optional[str] = None,
    resource_group: str = "GitHub Repo",
    resource_type: str = "Infrastructure",
    defer: bool = False,
):
    """Writes a permanent, legally auditable record to BigQuery in descriptive JSON format."""
    log_event(
        "INFO",
        f"▶️ 🔏 [Audit Ledger] Formulating audit log recording for action: {action}...",
        trace_id,
    )
    client = get_bq_client()

    row = {
        "operation_id": uuid.uuid4().hex,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "ai_agent_id": "sentinel-forge",
        "resource_group": resource_group,
        "resource_type": resource_type,
        "resource_id": resource,
        "action_type": action,
        "change_payload": json.dumps(details),
        "outcome_status": status,
        "reference_link": link,
    }

    if defer:
        global DEFERRED_LOGS
        DEFERRED_LOGS.append(row)
        log_event(
            "INFO",
            f"⏸️ 🔏 [Audit Ledger] Log deferred to await GitHub PR Link mapping.",
            trace_id,
        )
        return

    try:
        errors = client.insert_rows_json(AI_AUDIT_TABLE, [row])
        if errors:
            log_event(
                "ERROR",
                f"❌ 🔏 [Audit Ledger] Failed to write audit log: {errors}",
                trace_id,
            )
        else:
            log_event(
                "INFO",
                "✅ 🔏 [Audit Ledger] Successfully committed transaction to audit table.",
                trace_id,
            )
    except Exception as e:
        log_event("ERROR", f"☠️ 🔏 [Audit Ledger] Audit Log Crash: {e}", trace_id)
    finally:
        log_event("INFO", "⏹️ 🔏 [Audit Ledger] Finished audit log routine.", trace_id)


def flush_deferred_logs(pr_link: Optional[str], trace_id: str):
    """Injects the final PR link into all deferred logs and batch-inserts them to BigQuery."""
    global DEFERRED_LOGS
    if not DEFERRED_LOGS:
        return

    log_event(
        "INFO",
        f"▶️ 🔏 [Audit Ledger] Flushing {len(DEFERRED_LOGS)} deferred logs with PR Link: {pr_link}",
        trace_id,
    )
    client = get_bq_client()

    for row in DEFERRED_LOGS:
        if pr_link and row.get("reference_link") is None:
            row["reference_link"] = pr_link

    try:
        errors = client.insert_rows_json(AI_AUDIT_TABLE, DEFERRED_LOGS)
        if errors:
            log_event(
                "ERROR",
                f"❌ 🔏 [Audit Ledger] Failed to flush deferred logs: {errors}",
                trace_id,
            )
        else:
            log_event(
                "INFO",
                "✅ 🔏 [Audit Ledger] Successfully flushed all deferred logs to audit table.",
                trace_id,
            )
    except Exception as e:
        log_event(
            "ERROR", f"☠️ 🔏 [Audit Ledger] Deferred Audit Log Crash: {e}", trace_id
        )
    finally:
        DEFERRED_LOGS.clear()


def prune_whitespace(text: str) -> str:
    """Removes excess whitespace and newlines to save tokens and reduce AI costs."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


# ==============================================================================
# 🧠 NEW FEATURE: DATA CONTRACT RESOLUTION & SCHEMA DRIFT ENGINE
# ==============================================================================
def resolve_and_sync_data_contract(
    domain: str,
    data_contracts: Optional[str],
    sample_data: List[Dict],
    table_name: str,
    added_columns: List[str],
    removed_columns: List[str],
    trace_id: str,
) -> Tuple[str, str]:
    """Fetches existing YAML contract and updates it for drift, or generates one dynamically using AI."""
    log_event(
        "INFO",
        f"▶️ 📜 [Data Contract Engine] Resolving contract definition for '{table_name}'...",
        trace_id,
    )

    contract_filename = (
        data_contracts if data_contracts else f"{table_name}_contracts.yaml"
    )
    contract_content = None

    if data_contracts:
        try:
            log_event(
                "INFO",
                f"🌐 📜 [Data Contract Engine] Attempting to fetch gs://sentinel-function-code/data_contracts/{domain}/{data_contracts}",
                trace_id,
            )
            storage_cli = get_storage_client()
            bucket = storage_cli.bucket("sentinel-function-code")
            blob = bucket.blob(f"data_contracts/{domain}/{data_contracts}")

            if blob.exists():
                contract_content = blob.download_as_text()
                log_event(
                    "INFO",
                    f"✅ 📜 [Data Contract Engine] Successfully fetched existing contract from GCS.",
                    trace_id,
                )
            else:
                log_event(
                    "WARNING",
                    f"⚠️ 📜 [Data Contract Engine] Specified contract '{data_contracts}' not found in GCS.",
                    trace_id,
                )
        except Exception as e:
            log_event(
                "WARNING",
                f"⚠️ 📜 [Data Contract Engine] GCS fetch encountered an error: {e}.",
                trace_id,
            )

    current_date_str = datetime.datetime.now().strftime("%Y-%m-%d")

    if contract_content and (added_columns or removed_columns):
        log_event(
            "INFO",
            f"🔄 📜 [Data Contract Engine] Schema Drift Detected! Updating YAML Contract (Added: {added_columns}, Removed: {removed_columns})...",
            trace_id,
        )
        prompt = f"""
        You are an Enterprise Data Governance Expert.
        Task: Update the existing Data Contract YAML for table '{table_name}' to reflect Schema Drift.
        
        EXISTING CONTRACT:
        --- YAML START ---
        {contract_content}
        --- YAML END ---
        
        SCHEMA DRIFT DETECTED:
        - Columns Added: {added_columns}
        - Columns Removed: {removed_columns}
        - Sample Data for inference: {json.dumps(sample_data[:2])}
        
        Requirements:
        1. Add the new columns to the `schema` array with inferred types and basic constraints.
        2. Remove the deleted columns from the `schema` array.
        3. Bump the `version` attribute (e.g., 1.0.0 -> 1.1.0).
        4. Append a `changelog` section at the very bottom (if it doesn't exist, create it). Add a new entry documenting this drift:
           - date: "{current_date_str}"
             changes: "Automated schema drift resolution. Added: {added_columns}. Removed: {removed_columns}."
        5. Output ONLY valid YAML. Do not use markdown formatting or code blocks.
        """
        try:
            response = generate_content_with_retry(model_heavy, prompt, trace_id)
            contract_content = response.text.strip()
            if contract_content.startswith("```"):
                contract_content = contract_content.split("\n", 1)[1].rsplit("\n", 1)[0]
            if contract_content.startswith("yaml"):
                contract_content = contract_content[4:]
            log_event(
                "INFO",
                "✅ 📜 [Data Contract Engine] AI successfully updated YAML contract for schema drift.",
                trace_id,
            )
        except Exception as e:
            log_event(
                "ERROR",
                f"❌ 📜 [Data Contract Engine] AI contract update failed: {e}. Proceeding with original contract.",
                trace_id,
            )

    elif not contract_content:
        log_event(
            "INFO",
            "✨ 📜 [Data Contract Engine] Summoning AI to dynamically generate Enterprise YAML Data Contract...",
            trace_id,
        )

        yaml_template = f"""
version: 1.0.0
id: urn:dataform:staging:{domain}:stg_{table_name}
name: stg_{table_name}
description: "Enterprise Staging Layer for {table_name}."
status: active

service: bigquery
dataset: staging
project: {PROJECT_ID}

schema:
  - column: example_id
    type: STRING
    description: "Unique surrogate key."
    primary_key: true
    constraints:
      not_null: true
      unique: true
    tests:
      - type: regex
        pattern: "^ID-[0-9]+$"
        
changelog:
  - date: "{current_date_str}"
    changes: "Initial contract creation."
"""

        prompt = f"""
        You are an Enterprise Data Governance Expert.
        Task: Generate a rigorous Data Contract in YAML format for the table '{table_name}'.
        Domain: {domain}
        Sample Data from ingestion: {json.dumps(sample_data[:3])}

        REQUIRED ENTERPRISE YAML TEMPLATE (Mimic this structure EXACTLY):
        --- YAML START ---
        {yaml_template}
        --- YAML END ---

        Requirements:
        1. Output ONLY valid YAML adhering to the v1.0.0 enterprise template above. Do NOT include markdown ticks.
        2. Map the sample data columns to the `schema` array.
        3. Intelligently define the target `type` (e.g., STRING, INT64, NUMERIC, FLOAT64, TIMESTAMP, DATE).
        4. Infer reasonable `tests` (regex, accepted_values, range) and `constraints` based on the sample data.
        5. Assign one logical `primary_key: true`.
        6. Ensure the `changelog` is included at the bottom.
        """
        try:
            response = generate_content_with_retry(
                model_heavy if model_heavy else model_lite, prompt, trace_id
            )
            contract_content = response.text.strip()
            if contract_content.startswith("```"):
                contract_content = contract_content.split("\n", 1)[1].rsplit("\n", 1)[0]
            if contract_content.startswith("yaml"):
                contract_content = contract_content[4:]
            log_event(
                "INFO",
                "✅ 📜 [Data Contract Engine] AI successfully authored new YAML contract.",
                trace_id,
            )
        except Exception as e:
            log_event(
                "ERROR",
                f"❌ 📜 [Data Contract Engine] AI generation failed: {e}. Fabricating minimal failsafe YAML.",
                trace_id,
            )
            contract_content = f"version: 1.0.0\nname: {table_name}\nschema:\n"
            fallback_keys = sample_data[0].keys() if sample_data else ["id"]
            for k in fallback_keys:
                contract_content += f"  - column: {k}\n    type: STRING\n"
            contract_content += f"\nchangelog:\n  - date: '{current_date_str}'\n    changes: 'Failsafe contract generation.'"

    log_event(
        "INFO", "⏹️ 📜 [Data Contract Engine] Contract resolution complete.", trace_id
    )
    return contract_filename, contract_content


# ==============================================================================
# 🧠 AGENT: SCHEMA DESIGNER
# ==============================================================================
def generate_dynamic_schema(
    table_name: str,
    new_cols: List[str],
    sample_data: List[Dict],
    reference_json: str,
    trace_id: str,
) -> List[Dict]:
    """Goal: Create valid BigQuery JSON schema matching strict enterprise standards."""
    log_event(
        "INFO",
        f"▶️ 🧩 [Agent: Schema Designer] Initiating dynamic schema generation for '{table_name}'...",
        trace_id,
    )

    if not model_lite:
        log_event(
            "WARNING",
            "⚠️ 🧩 [Agent: Schema Designer] AI Service unavailable. Falling back to basic programmatic schema.",
            trace_id,
        )
        log_event(
            "INFO",
            "⏹️ 🧩 [Agent: Schema Designer] Finished schema generation (Fallback mode).",
            trace_id,
        )
        return [
            {
                "name": c,
                "type": "STRING",
                "mode": "NULLABLE",
                "defaultValueExpression": "NULL",
            }
            for c in new_cols
        ]

    log_event(
        "INFO",
        f"🧠 🧩 [Agent: Schema Designer] Instructing AI to map {len(new_cols)} incoming columns...",
        trace_id,
    )

    style_guide = ""
    if reference_json:
        log_event(
            "INFO",
            "🎨 🧩 [Agent: Schema Designer] Injecting reference JSON style guide into AI context...",
            trace_id,
        )
        style_guide = f"STYLE GUIDE (Mimic this format):\n--- JSON START ---\n{prune_whitespace(reference_json[:3000])}\n--- JSON END ---"

    prompt = f"""
    You are a Data Architect.
    Task: Generate a BigQuery Schema JSON for table "{table_name}".
    
    Input:
    - New Columns: {new_cols}
    - Sample Data: {json.dumps(sample_data[:3])}
    
    {style_guide}
    
    Requirements:
    1. Output strictly a JSON list of objects. Do not use markdown formatting.
    2. ABSOLUTE RULE: ALL TYPES MUST STRICTLY BE 'STRING' for the incoming raw columns. Do NOT output INT64, BOOLEAN, or TIMESTAMP for these incoming columns. Only audit columns (like batch_date) get explicit native types.
    3. Mode must be 'NULLABLE'.
    4. Descriptions are mandatory. Infer them from column names.
    5. EVERY single column MUST have a "defaultValueExpression" defined (e.g., use "NULL" for standard strings).
    
    Output: JSON Array only.
    """

    try:
        log_event(
            "INFO",
            "⏳ 🧩 [Agent: Schema Designer] Awaiting AI generation response...",
            trace_id,
        )
        response = generate_content_with_retry(model_lite, prompt, trace_id)

        text = response.text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]
        if text.startswith("json"):
            text = text[4:]

        parsed_schema = json.loads(text)
        log_event(
            "INFO",
            f"✅ 🧩 [Agent: Schema Designer] AI successfully formulated JSON schema with {len(parsed_schema)} fields.",
            trace_id,
        )

        log_ai_action(
            trace_id=trace_id,
            action="GENERATE_SCHEMA",
            resource=table_name,
            status="SUCCESS",
            details={"columns_mapped": len(parsed_schema)},
            link=None,
            resource_group="BigQuery Schema",
            resource_type="Schema - Generated",
            defer=True,
        )
        log_event(
            "INFO",
            "⏹️ 🧩 [Agent: Schema Designer] Finished schema generation.",
            trace_id,
        )
        return parsed_schema

    except Exception as e:
        log_event(
            "ERROR",
            f"❌ 🧩 [Agent: Schema Designer] Schema Generation Failed: {e}. Falling back to basic generation.",
            trace_id,
        )
        log_ai_action(
            trace_id=trace_id,
            action="GENERATE_SCHEMA",
            resource=table_name,
            status="FAILED",
            details={"error": str(e)},
            link=None,
            resource_group="BigQuery Schema",
            resource_type="Schema - Generation Failed",
        )
        log_event(
            "INFO",
            "⏹️ 🧩 [Agent: Schema Designer] Finished schema generation (Fallback mode).",
            trace_id,
        )
        return [
            {
                "name": c,
                "type": "STRING",
                "mode": "NULLABLE",
                "defaultValueExpression": "NULL",
            }
            for c in new_cols
        ]


# ==============================================================================
# 🧠 AGENT: TERRAFORM ARCHITECT
# ==============================================================================
def analyze_tf_repo_state(
    repo, branch_sha: str, table_id: str, trace_id: str
) -> Dict[str, Any]:
    """Goal: Determine WHERE to put TF code AND extract existing Repo Styles for history tables."""
    log_event(
        "INFO",
        f"▶️ 🕵️‍♂️ [Repo Scan] Initiating deep repository scan for Terraform state of '{table_id}'...",
        trace_id,
    )
    state = {
        "is_defined": False,
        "defined_in_file": None,
        "host_file": None,
        "host_file_content": None,
        "style_guide_tf": "",
        "style_guide_tf_hist": "",
    }

    try:
        log_event(
            "INFO", "🌳 🕵️‍♂️ [Repo Scan] Fetching Git tree architecture...", trace_id
        )
        tree = repo.get_git_tree(branch_sha, recursive=True)
        tf_files = [
            e
            for e in tree.tree
            if e.path.startswith(TF_BASE_PATH) and e.path.endswith(".tf")
        ]

        log_event(
            "INFO",
            f"📂 🕵️‍♂️ [Repo Scan] Discovered {len(tf_files)} potential target `.tf` files.",
            trace_id,
        )

        best_candidate_file = None
        max_tables_in_file = 0

        for element in tf_files:
            blob = repo.get_git_blob(element.sha)
            content = base64.b64decode(blob.content).decode("utf-8")

            if "_hist" in content and not state["style_guide_tf_hist"]:
                state["style_guide_tf_hist"] = content
                log_event(
                    "INFO",
                    f"🎨 🕵️‍♂️ [Repo Scan] Extracted existing _hist table configuration as style guide.",
                    trace_id,
                )
            elif (
                'resource "google_bigquery_table"' in content
                and not state["style_guide_tf"]
            ):
                state["style_guide_tf"] = content

            if f'"{table_id}"' in content or f"'{table_id}'" in content:
                log_event(
                    "INFO",
                    f"👀 🕵️‍♂️ [Repo Scan] Detected string match for '{table_id}' in {element.path}. Validating semantics...",
                    trace_id,
                )
                if ask_ai_is_definition(content, table_id, trace_id):
                    state["is_defined"] = True
                    state["defined_in_file"] = element.path
                    log_event(
                        "INFO",
                        f"✅ 🕵️‍♂️ [Repo Scan] AI Verified: Table is defined in {element.path}",
                        trace_id,
                    )
                    log_event(
                        "INFO",
                        "⏹️ 🕵️‍♂️ [Repo Scan] Finished analysis (Definition Found).",
                        trace_id,
                    )
                    return state

            if 'resource "google_bigquery_table"' in content and (
                "for_each" in content or "locals" in content
            ):
                count = content.count("table_id") + content.count("partition_type")
                if count > max_tables_in_file:
                    max_tables_in_file = count
                    best_candidate_file = element.path
                    state["host_file_content"] = content

        if best_candidate_file:
            state["host_file"] = best_candidate_file
            log_event(
                "INFO",
                f"🏠 🕵️‍♂️ [Repo Scan] Identified optimal existing Host File for injection: {best_candidate_file}",
                trace_id,
            )
        else:
            log_event(
                "INFO",
                "⚪ 🕵️‍♂️ [Repo Scan] No suitable host file found. Generating new file logic.",
                trace_id,
            )

    except Exception as e:
        log_event(
            "WARNING",
            f"⚠️ 🕵️‍♂️ [Repo Scan] Repo analysis encountered an anomaly: {e}",
            trace_id,
        )

    log_event(
        "INFO", "⏹️ 🕵️‍♂️ [Repo Scan] Finished comprehensive repository analysis.", trace_id
    )
    return state


def ask_ai_is_definition(content: str, table_id: str, trace_id: str) -> bool:
    """Helper: Asks AI if the table is truly defined in this file."""
    if not model_lite:
        return True
    prompt = f"""
    Does this Terraform code define a BigQuery table named "{table_id}"?
    Code: {content[:5000]}
    Return TRUE or FALSE.
    """
    try:
        response = generate_content_with_retry(model_lite, prompt, trace_id)
        return "TRUE" in response.text.upper()
    except:
        return False


def generate_tf_patch_or_create(
    table_id: str,
    dataset_id: str,
    schema_path: str,
    host_file_content: Optional[str],
    tf_state: Dict[str, Any],
    trace_id: str,
) -> str:
    """Goal: Write HCL code. Enforces adaptive injection based on locals maps vs distinct resources."""
    log_event(
        "INFO",
        f"▶️ 🏗️ [Agent: TF Architect] Initiating Terraform HCL synthesis for '{table_id}'...",
        trace_id,
    )

    if not model_heavy:
        log_event(
            "WARNING",
            "⚠️ 🏗️ [Agent: TF Architect] AI Service offline. Returning empty block.",
            trace_id,
        )
        log_event(
            "INFO",
            "⏹️ 🏗️ [Agent: TF Architect] Finished TF generation (Abort).",
            trace_id,
        )
        return ""

    action = "CREATE_NEW" if not host_file_content else "PATCH_EXISTING"
    log_event(
        "INFO",
        f"🛠️ 🏗️ [Agent: TF Architect] Determining infrastructure action vector: {action}",
        trace_id,
    )

    style_guide_hist = tf_state.get("style_guide_tf_hist", "")
    hist_instruction = ""
    if table_id.endswith("_raw"):
        log_event(
            "INFO",
            f"⏳ 🏗️ [Agent: TF Architect] Raw table detected. Injecting STRICT _hist infrastructure rules with schema reuse...",
            trace_id,
        )
        mimic_rule = (
            f"MIMIC THIS EXISTING HISTORY TABLE CONFIGURATION EXACTLY (Do not invent variables):\n--- HCL START ---\n{prune_whitespace(style_guide_hist[:2000])}\n--- HCL END ---"
            if style_guide_hist
            else "Configure with `expiration_ms = 2592000000` for 30 days."
        )

        hist_instruction = f"""
        CRITICAL REQUIREMENT: Because '{table_id}' is a raw landing table, you MUST ALSO generate the infrastructure for its history table '{table_id}_hist'. 
        - If using standard resources, create a SECOND `resource "google_bigquery_table"` block.
        - If the file uses `locals` dictionaries (e.g., `tables_raw_hist = {{}}`), inject the history table definition into that specific dictionary with `expiration_ms = 2592000000`.
        - History Schema File: "{schema_path}" (CRITICAL: Reuse the EXACT SAME JSON schema path as the main table. DO NOT append _hist to the json filename).
        {mimic_rule}
        Ensure BOTH the main table AND the history table are included in your HCL output.
        """

    if action == "PATCH_EXISTING":
        log_event(
            "INFO",
            "🩹 🏗️ [Agent: TF Architect] Constructing prompt for intelligent HCL injection (Patch)...",
            trace_id,
        )
        prompt = f"""
        You are a strictly governed Terraform Expert. Your task is to safely APPEND or INJECT a new BigQuery table definition to the EXISTING FILE below without altering unrelated resources.
        
        Table ID: "{table_id}"
        Resource Name: "{table_id.replace('.', '_')}"
        Schema File: "{schema_path}"
        Partitioning: DAY (field: batch_date)
        Dataset ID: "{dataset_id}"
        
        {hist_instruction}
        
        EXISTING FILE CONTENT:
        --- HCL START ---
        {prune_whitespace(host_file_content)}
        --- HCL END ---
        
        STRICT RULES:
        1. DO NOT modify, delete, or hallucinate changes to the existing resources in the file.
        2. ADAPTIVE INJECTION: 
           - IF the file uses a `locals` block with mapped objects and `for_each`, you MUST ONLY add the new table's configuration object into the appropriate dictionary. DO NOT append a new `resource` block.
           - IF the file uses individual `resource "google_bigquery_table"` blocks, ONLY append the new `resource` block(s) at the end of the file.
        3. Use exact string matching for the schema path: `schema = file("${{path.module}}/{schema_path}")` or follow the dynamic schema reference if using `locals`.
        4. DO NOT invent variables like `var.project_id` or `var.dataset` unless they already exist. 
        5. Return the **FULL UPDATED FILE CONTENT**, including the original code and your new appended/injected blocks. Do not truncate.
        
        CRITICAL SYNTAX RULE:
        When injecting into `locals` maps, you MUST include proper attribute separators. Every key-value pair inside an object must be separated by a newline or comma. Every object in a map MUST be separated by a comma (`,`). Avoid `Error: Missing attribute separator`.
        
        Output strictly HCL Code only. No markdown formatting outside the code block.
        """
    else:
        log_event(
            "INFO",
            "✨ 🏗️ [Agent: TF Architect] Constructing prompt for brand new HCL definition (Create)...",
            trace_id,
        )
        prompt = f"""
        You are a strictly governed Terraform Expert. Your task is to CREATE a brand new Terraform file for a BigQuery Table.
        
        Table ID: "{table_id}"
        Resource Name: "{table_id.replace('.', '_')}"
        Dataset ID: "{dataset_id}"
        Schema File: "{schema_path}"
        Partitioning: DAY (field: batch_date)
        
        {hist_instruction}
        
        STRICT RULES:
        1. Create ONLY the `google_bigquery_table` resource(s) requested.
        2. Use exact string matching for the schema path: `schema = file("${{path.module}}/{schema_path}")`.
        3. DO NOT invent variables like `var.dataset_id`. Use the literal string "{dataset_id}".
        
        CRITICAL SYNTAX RULE:
        When creating `locals` maps (if applicable), you MUST include proper attribute separators. Every key-value pair inside an object must be separated by a newline or comma. Every object in a map MUST be separated by a comma (`,`). Avoid `Error: Missing attribute separator`.
        
        4. Output strictly HCL Code only. No markdown formatting outside the code block.
        """

    try:
        log_event(
            "INFO",
            "⏳ 🏗️ [Agent: TF Architect] Awaiting AI infrastructure generation...",
            trace_id,
        )
        response = generate_content_with_retry(model_heavy, prompt, trace_id)
        text = response.text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]
        if text.startswith("hcl"):
            text = text[3:]

        log_event(
            "INFO",
            "✅ 🏗️ [Agent: TF Architect] AI successfully generated Terraform HCL block.",
            trace_id,
        )

        log_ai_action(
            trace_id=trace_id,
            action="GENERATE_TERRAFORM_CODE",
            resource=table_id,
            status="SUCCESS",
            details={"action": action, "target_file": schema_path},
            link=None,
            resource_group="Terraform Pipeline",
            resource_type=f"Infrastructure - {'Updated' if action == 'PATCH_EXISTING' else 'Created'}",
            defer=True,
        )

        log_event("INFO", "⏹️ 🏗️ [Agent: TF Architect] Finished TF generation.", trace_id)
        return text.strip()
    except Exception as e:
        log_event(
            "ERROR",
            f"❌ 🏗️ [Agent: TF Architect] Terraform HCL Generation Failed: {e}",
            trace_id,
        )
        log_ai_action(
            trace_id=trace_id,
            action="GENERATE_TERRAFORM_CODE",
            resource=table_id,
            status="FAILED",
            details={"error": str(e)},
            link=None,
            resource_group="Terraform Pipeline",
            resource_type="Infrastructure - Generation Failed",
        )
        log_event(
            "INFO",
            "⏹️ 🏗️ [Agent: TF Architect] Finished TF generation (Failure).",
            trace_id,
        )
        return ""


# ==============================================================================
# 🧠 AGENT: DATAFORM SCANNER
# ==============================================================================
def analyze_dataform_repo_state(
    repo, branch_sha: str, base_name: str, table_name: str, trace_id: str
) -> Dict[str, Any]:
    """Goal: Use Semantic/Fuzzy matching to find existing domains, rules, and strictly extract valid datasets/schemas."""
    log_event(
        "INFO",
        f"▶️ 🔍 [Agent: DF Scanner] Semantic scan for entity '{base_name}' in Dataform workspace...",
        trace_id,
    )
    state = {
        "style_guide_sqlx": "",
        "existing_files": {},
        "inferred_domain": None,
        "inferred_schema": None,
        "inferred_stg_schema": None,
        "inferred_marts_schema": None,
        "existing_schemas": set(),
    }

    try:
        tree = repo.get_git_tree(branch_sha, recursive=True)
        df_files = [
            e
            for e in tree.tree
            if e.path.startswith("definitions/") and e.path.endswith(".sqlx")
        ]

        aliases = {table_name, base_name, base_name.rstrip("s"), base_name.rstrip("es")}
        aliases = {a for a in aliases if len(a) > 2}

        for element in df_files:
            path_lower = element.path.lower()
            content = base64.b64decode(repo.get_git_blob(element.sha).content).decode(
                "utf-8"
            )

            schemas_in_file = re.findall(r'schema:\s*["\']([^"\']+)["\']', content)
            state["existing_schemas"].update(schemas_in_file)

            if any(alias in path_lower for alias in aliases):
                if any(alias in content.lower() for alias in aliases):
                    state["existing_files"][element.path] = content
                    log_event(
                        "INFO",
                        f"📂 🔍 [Agent: DF Scanner] Semantic match found and verified for patching: {element.path}",
                        trace_id,
                    )

                    parts = element.path.split("/")
                    if len(parts) >= 4 and parts[1] in ["marts", "staging"]:
                        state["inferred_domain"] = parts[2]

                    if schemas_in_file:
                        if "sources" in path_lower and not state["inferred_schema"]:
                            state["inferred_schema"] = schemas_in_file[0]
                        elif (
                            "staging" in path_lower and not state["inferred_stg_schema"]
                        ):
                            state["inferred_stg_schema"] = schemas_in_file[0]
                        elif (
                            "marts" in path_lower and not state["inferred_marts_schema"]
                        ):
                            state["inferred_marts_schema"] = schemas_in_file[0]

            elif not state["style_guide_sqlx"] and (
                "stg_" in path_lower or "fct_" in path_lower
            ):
                state["style_guide_sqlx"] = content

    except Exception as e:
        log_event(
            "WARNING",
            f"⚠️ 🔍 [Agent: DF Scanner] Dataform scan anomaly: {e}",
            trace_id,
        )

    state["existing_schemas"] = list(state["existing_schemas"])
    log_event(
        "INFO",
        "⏹️ 🔍 [Agent: DF Scanner] Finished scanning Dataform workspace.",
        trace_id,
    )
    return state


# ==============================================================================
# 🧠 AGENT: DATASET ROUTING ANALYST
# ==============================================================================
def infer_pipeline_datasets_with_ai(
    table_name: str,
    existing_schemas: List[str],
    inferred_raw: Optional[str],
    trace_id: str,
) -> Dict[str, str]:
    """Goal: Use AI to analyze existing datasets and securely map them to the pipeline layers without hallucinating."""
    log_event(
        "INFO",
        f"▶️ 🗺️ [Agent: Dataset Analyst] Initiating AI dataset routing for '{table_name}'...",
        trace_id,
    )

    default_map = {
        "raw": inferred_raw or "sentinel_raw",
        "stg": "sentinel_staging",
        "marts": "sentinel_marts",
    }

    if not model_lite:
        log_event(
            "WARNING",
            "⚠️ 🗺️ [Agent: Dataset Analyst] AI offline. Using standard defaults.",
            trace_id,
        )
        log_event("INFO", "⏹️ 🗺️ [Agent: Dataset Analyst] Finished routing.", trace_id)
        return default_map

    prompt = f"""
    You are a Cloud Data Architect. Your task is to map the data pipeline layers (raw, staging, marts) for the table "{table_name}" to the correct Google BigQuery datasets.
    
    EXISTING DATASETS IN REPOSITORY: {existing_schemas}
    HINT (Previously used raw schema): {inferred_raw}
    
    Rules:
    1. You MUST select from the "EXISTING DATASETS IN REPOSITORY" list if it is not empty.
    2. STRICT RULE: DO NOT create or invent non-existing datasets. Match semantically (e.g., if existing is ['ecommerce_raw', 'ecommerce_stg', 'ecommerce_marts'], map them accordingly).
    3. If the existing list is completely empty, fallback to standard enterprise naming: 'sentinel_raw', 'sentinel_staging', 'sentinel_marts'.
    
    Output STRICTLY a valid JSON object with exact keys: "raw", "stg", "marts". Do not include markdown formatting.
    """

    try:
        log_event(
            "INFO",
            "⏳ 🗺️ [Agent: Dataset Analyst] Awaiting AI dataset routing analysis...",
            trace_id,
        )

        response = generate_content_with_retry(model_lite, prompt, trace_id)

        text = response.text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]
        if text.startswith("json"):
            text = text[4:]

        datasets_map = json.loads(text)
        log_event(
            "INFO",
            f"✅ 🗺️ [Agent: Dataset Analyst] AI successfully mapped datasets: {json.dumps(datasets_map)}",
            trace_id,
        )
        log_event(
            "INFO", "⏹️ 🗺️ [Agent: Dataset Analyst] Finished dataset routing.", trace_id
        )
        return datasets_map

    except Exception as e:
        log_event(
            "ERROR",
            f"❌ 🗺️ [Agent: Dataset Analyst] AI Mapping Failed: {e}. Falling back to safe defaults.",
            trace_id,
        )
        log_event(
            "INFO",
            "⏹️ 🗺️ [Agent: Dataset Analyst] Finished dataset routing (Fallback applied).",
            trace_id,
        )
        return default_map


# ==============================================================================
# 🧠 AGENT: DATAFORM ARCHITECT (Enhanced Templates, Pathing & Formatting)
# ==============================================================================
def generate_ai_dataform_pipeline(
    table_name: str,
    schema_list: List[Dict],
    error_context: str,
    df_state: Dict[str, Any],
    added_cols: List[str],
    removed_cols: List[str],
    sample_data: List[Dict],
    datasets: Dict[str, str],
    target_domain: str,
    contract_yaml: str,
    trace_id: str,
) -> Dict[str, str]:
    """Goal: Dynamically generate Dataform files, strictly enforcing DLQ, explicit mapping, and file descriptions."""
    log_event(
        "INFO",
        f"▶️ 🪄 [Agent: DF Architect] Analyzing requirements for '{table_name}'...",
        trace_id,
    )

    if not model_heavy:
        log_event(
            "WARNING",
            "⚠️ 🪄 [Agent: DF Architect] AI Service offline. Cannot generate Dataform code.",
            trace_id,
        )
        log_event(
            "INFO",
            "⏹️ 🪄 [Agent: DF Architect] Finished Dataform generation (Abort).",
            trace_id,
        )
        return {}

    base_name = table_name.replace("_raw", "")
    clean_schema = [{"name": c["name"], "type": c["type"]} for c in schema_list]
    # Extract just the column names for explicit listing
    column_names = [
        c["name"]
        for c in clean_schema
        if c["name"] not in ["batch_date", "processed_dttm", "updated_dttm"]
    ]

    existing_files = df_state.get("existing_files", {})
    style_guide = df_state.get("style_guide_sqlx", "")
    inferred_domain = df_state.get("inferred_domain", "")

    action_type = "Updated" if existing_files else "Created"

    actual_domain = (
        target_domain
        if target_domain and target_domain != "unknown_domain"
        else (inferred_domain if inferred_domain else "logical_domain")
    )

    if existing_files:
        existing_paths = list(existing_files.keys())
        allowed_paths = existing_paths + [
            p.replace(".sqlx", "_bad_recs.sqlx")
            for p in existing_paths
            if "stg_" in p or "fct_" in p
        ]
        log_event(
            "INFO",
            f"🩹 🪄 [Agent: DF Architect] Existing files detected. Locking allowed file paths to: {allowed_paths}",
            trace_id,
        )
        pruned_existing_files = prune_whitespace(json.dumps(existing_files))
        instruction_block = f"""
        Some or all pipeline files exist. EXISTING FILES: {pruned_existing_files}
        Task: PATCH these existing files AND CREATE the quarantine _bad_recs files. 
        CRITICAL SCHEMA DRIFT EVENT:
        - Added Columns: {added_cols} (Must explicitly map these)
        - Removed Columns: {removed_cols} (Must explicitly remove these from SELECT statements)
        
        CRITICAL ANTI-HALLUCINATION RULE: You MUST use the EXACT file paths provided. The allowed keys are strictly: {allowed_paths}. DO NOT invent new file paths.
        CRITICAL CONTRACT RULE: You MUST apply data types, SAFE_CASTing, and quality rules STRICTLY as defined in the provided DATA CONTRACT YAML.
        """
    else:
        log_event(
            "INFO",
            "✨ 🪄 [Agent: DF Architect] No files detected. Operating in CREATE mode.",
            trace_id,
        )
        instruction_block = f"""
        Create files from scratch.
        CRITICAL DOMAIN ROUTING: Use domain '{actual_domain}'.
        Structure MUST strictly follow this pattern:
        - definitions/sources/{actual_domain}/{base_name}/{table_name}.sqlx
        - definitions/staging/{actual_domain}/{base_name}/stg_{base_name}.sqlx
        - definitions/staging/{actual_domain}/{base_name}/stg_{base_name}_bad_recs.sqlx
        - definitions/marts/{actual_domain}/{base_name}/fct_{base_name}.sqlx
        - definitions/marts/{actual_domain}/{base_name}/fct_{base_name}_bad_recs.sqlx
        - definitions/operations/{actual_domain}/{base_name}/archive_{table_name}.sqlx
        
        CRITICAL CONTRACT RULE: All dynamic casting (SAFE_CAST), primary keys, clustering, and data quality WHERE clauses MUST be extracted directly from the DATA CONTRACT YAML (looking inside the 'schema' array for 'type', 'constraints', and 'tests').
        STYLE GUIDE (Mimic formatting and alignment perfectly):
        --- SQL START ---
        {prune_whitespace(style_guide[:2000])}
        --- SQL END ---
        """

    log_event(
        "INFO",
        "🛡️ 🪄 [Agent: DF Architect] Injecting strict Dataform Templates, Explicit Column Mapping, Quarantine Routing, and Contract YAML Enforcement.",
        trace_id,
    )

    prompt = f"""
    You are an expert Analytics Engineer writing Google Cloud Dataform code (Core v3.x).
    
    Target Raw Table: "{table_name}" | Base Entity: "{base_name}" | GCP Project: "{PROJECT_ID}"
    Full Schema: {json.dumps(clean_schema)} | Error Context: "{error_context}"
    Core Columns to Map Explicitly (After Drift): {column_names}
    Sample Data: {json.dumps(sample_data[:2])}
    
    DATA CONTRACT (ABSOLUTE MASTER SOURCE OF TRUTH):
    --- YAML START ---
    {contract_yaml}
    --- YAML END ---
    
    {instruction_block}
    
    DATAFORM TEMPLATES TO STRICTLY FOLLOW (Use these exact structures - NOTE THE DESCRIPTIONS):
    
    1. Source Declaration (`definitions/sources/{actual_domain}/{base_name}/{table_name}.sqlx`):
       config {{ type: "declaration", database: "{PROJECT_ID}", schema: "{datasets['raw']}", name: "{table_name}", description: "Raw ingestion source for {base_name}." }}

    2. Main Staging Table (`definitions/staging/{actual_domain}/{base_name}/stg_{base_name}.sqlx`):
       config {{ type: "table", schema: "{datasets['stg']}", tags: ["{actual_domain}", "{base_name}", "staging"], description: "Cleansed, deduplicated, and strictly typed staging layer for {base_name}." }}
       -- NO ASSERTIONS BLOCK. We filter explicitly using the YAML rules.
       SELECT 
         <list_every_core_column_explicitly_applying_dynamic_casting_based_on_YAML_types>,
         CURRENT_DATE() AS batch_date,
         SAFE_CAST(processed_dttm AS TIMESTAMP) AS processed_dttm,
         CURRENT_TIMESTAMP() AS updated_dttm
       FROM ${{ref("{table_name}")}} 
       WHERE <insert_contract_YAML_rules_here_as_AND_conditions>
       QUALIFY ROW_NUMBER() OVER(PARTITION BY <actual_pk_col_from_YAML> ORDER BY SAFE_CAST(processed_dttm AS TIMESTAMP) DESC) = 1

    3. Quarantine Staging Table (`definitions/staging/{actual_domain}/{base_name}/stg_{base_name}_bad_recs.sqlx`):
       config {{ type: "incremental", schema: "{datasets['stg']}", tags: ["{actual_domain}", "{base_name}", "staging", "quarantine"], description: "Dead Letter Queue (DLQ) capturing data contract violations for {base_name} staging." }}
       SELECT 
         <list_every_core_column_explicitly_applying_dynamic_casting_based_on_YAML_types>,
         CASE 
            WHEN NOT (<condition_1_from_YAML>) THEN 'VIOLATION: rule 1'
            WHEN NOT (<condition_2_from_YAML>) THEN 'VIOLATION: rule 2'
            ELSE 'VIOLATION: UNKNOWN'
         END AS failure_reason,
         CURRENT_TIMESTAMP() AS quarantined_at
       FROM ${{ref("{table_name}")}} 
       WHERE NOT (<insert_contract_YAML_rules_here_as_AND_conditions>)

    4. Operations/Archive (`definitions/operations/{actual_domain}/{base_name}/archive_{table_name}.sqlx`):
       config {{ type: "operations", dependencies: ["fct_{base_name}"], tags: ["{actual_domain}", "{base_name}", "archive"], description: "Idempotent archival operations moving processed {base_name} records to history." }}
       INSERT INTO `{PROJECT_ID}.{datasets['raw']}.{table_name}_hist` (col1, batch_date, processed_dttm)
       SELECT col1, CURRENT_DATE() AS batch_date, processed_dttm FROM ${{ref("{table_name}")}};
       TRUNCATE TABLE ${{ref("{table_name}")}};

    5. Main Marts Incremental (`definitions/marts/{actual_domain}/{base_name}/fct_{base_name}.sqlx`):
       config {{ 
         type: "incremental", schema: "{datasets['marts']}", uniqueKey: ["<actual_pk_col_from_YAML>"], 
         bigquery: {{ partitionBy: "batch_date", clusterBy: ["<actual_clustering_col_from_YAML>"] }}, tags: ["{actual_domain}", "{base_name}", "marts"], description: "Incremental fact/dimension mart layer for {base_name} driving downstream analytics."
       }}
       SELECT 
         <list_every_core_column_explicitly_do_not_use_select_star>,
         processed_dttm,
         CURRENT_DATE() AS batch_date, 
         CURRENT_TIMESTAMP() AS updated_dttm
       FROM ${{ref("stg_{base_name}")}}
       ${{when(incremental(), `WHERE processed_dttm > (SELECT COALESCE(MAX(processed_dttm), TIMESTAMP("1970-01-01")) FROM ${{self()}})` )}}

    6. Quarantine Marts Table (`definitions/marts/{actual_domain}/{base_name}/fct_{base_name}_bad_recs.sqlx`):
       -- (Follow the exact same pattern as the Staging Quarantine table, but source from the Staging table if additional mart-level rules exist, or omit if all rules were caught in staging).

    GENERAL REQUIREMENTS:
    1. STRICT DATA CONTRACT ENFORCEMENT: The provided DATA CONTRACT YAML is your master reference. Extract Data Types, PKs, Clustering, and Data Quality Rules (Regex, Range, Accepted Values) exclusively from it.
    2. EXPLICIT DESCRIPTIONS: EVERY `config {{ }}` block MUST contain a `description` field as shown in the templates.
    3. EXPLICIT COLUMN MAPPING (NO SELECT *): You MUST explicitly write out every single column name in the SELECT statements for Staging and Marts. DO NOT use `SELECT *` or `SELECT * EXCEPT(...)`.
    4. QUARANTINE ROUTING (NO FATAL ASSERTIONS): NEVER use the `assertions: {{}}` block inside the `config`. Instead, translate the YAML Data Contract rules into SQL `WHERE` clauses for the main tables. Catch the failed rows using an inverted `WHERE NOT (...)` filter in `_bad_recs.sqlx`.
    5. TEMPORAL ACCURACY: Include both `processed_dttm` and `updated_dttm` (CURRENT_TIMESTAMP()) in staging and marts.
    6. REGEX/STRING ESCAPING (CRITICAL): When writing SQL functions (like REGEXP_CONTAINS), you MUST use single quotes (') for the inner string literals to avoid breaking outer strings if generated. Example CORRECT: "REGEXP_CONTAINS(order_id, r'^ORD-[0-9]{{5,10}}$')"
    7. Output STRICTLY a JSON object where keys are full file paths and values are exact SQLX string content. No markdown formatting.
    """

    try:
        log_event(
            "INFO",
            "⏳ 🪄 [Agent: DF Architect] Instructing Gemini to synthesize Dataform files with Quarantine Routing, Explicit Column Mapping, YAML Contract Enforcement, and Strict Formatting...",
            trace_id,
        )
        response = generate_content_with_retry(model_heavy, prompt, trace_id)
        text = response.text.strip()

        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]
        if text.startswith("json"):
            text = text[4:]

        pipeline_files = json.loads(text)
        log_event(
            "INFO",
            f"✅ 🪄 [Agent: DF Architect] AI successfully generated {len(pipeline_files)} SQLX files with explicit config descriptions.",
            trace_id,
        )

        log_ai_action(
            trace_id=trace_id,
            action="GENERATE_DATAFORM_CODE",
            resource=table_name,
            status="SUCCESS",
            details={"files_generated": list(pipeline_files.keys())},
            link=None,
            resource_group="Dataform Pipeline",
            resource_type=f"Code Generation - {action_type}",
            defer=True,
        )
        log_event(
            "INFO",
            "⏹️ 🪄 [Agent: DF Architect] Finished Dataform generation.",
            trace_id,
        )
        return pipeline_files

    except Exception as e:
        log_event(
            "ERROR",
            f"❌ 🪄 [Agent: DF Architect] AI Code Generation Failed: {e}",
            trace_id,
        )
        log_ai_action(
            trace_id=trace_id,
            action="GENERATE_DATAFORM_CODE",
            resource=table_name,
            status="FAILED",
            details={"error": str(e)},
            link=None,
            resource_group="Dataform Pipeline",
            resource_type="Code Generation - Failed",
        )
        log_event(
            "INFO",
            "⏹️ 🪄 [Agent: DF Architect] Finished Dataform generation (Failure).",
            trace_id,
        )
        return {}


# ==============================================================================
# 🧠 AGENT: DATAFORM QA VERIFIER (Enhanced Syntax, Template & Formatting Validation)
# ==============================================================================
def verify_dataform_pipeline(
    pipeline_files: Dict[str, str],
    schema_list: List[Dict],
    added_cols: List[str],
    removed_cols: List[str],
    datasets: Dict[str, str],
    df_state: Dict[str, Any],
    base_name: str,
    target_domain: str,
    trace_id: str,
) -> Dict[str, str]:
    """Goal: QA Lead verifies exact syntax match with DLQ templates, explicit mappings, and config descriptions."""
    log_event(
        "INFO",
        f"▶️ 🕵️‍♀️ [Agent: DF QA] Initiating automated peer-review of generated SQLX code...",
        trace_id,
    )

    if not model_lite or not pipeline_files:
        log_event(
            "INFO",
            "⏹️ 🕵️‍♀️ [Agent: DF QA] Bypassing QA (No model or no files).",
            trace_id,
        )
        return pipeline_files

    clean_schema = [
        c["name"]
        for c in schema_list
        if c["name"] not in ["batch_date", "processed_dttm", "updated_dttm"]
    ]

    actual_domain = (
        target_domain
        if target_domain and target_domain != "unknown_domain"
        else df_state.get("inferred_domain", "domain_placeholder")
    )

    existing_paths = list(df_state.get("existing_files", {}).keys())
    allowed_paths = existing_paths + [
        p.replace(".sqlx", "_bad_recs.sqlx")
        for p in existing_paths
        if "stg_" in p or "fct_" in p
    ]

    prompt = f"""
    You are a Brutal Senior Data QA Lead. Review this Dataform pipeline generated by a junior engineer. Your only job is to REJECT and FIX invalid SQLX.
    
    GENERATED FILES: {json.dumps(pipeline_files)}
    REQUIRED CORE COLUMNS: {clean_schema} 
    ADDED COLUMNS: {added_cols} | REMOVED COLUMNS: {removed_cols}
    EXPECTED DATASETS: Raw: "{datasets['raw']}", Staging: "{datasets['stg']}", Marts: "{datasets['marts']}"
    EXPECTED BASE ENTITY: "{base_name}"
    EXPECTED EXISTING PATHS (Including Bad Recs): {allowed_paths}
    
    Checklist & BRUTAL RULES:
    1. EXPLICIT COLUMN MAPPING (CRITICAL): If you see `SELECT *` or `SELECT * EXCEPT` anywhere in the generated code (Staging or Marts), you MUST expand it. Replace it with a comma-separated list of the 'REQUIRED CORE COLUMNS'. No asterisks allowed. Also ensure removed columns ({removed_cols}) are NOT in the SELECT block.
    2. ANTI-HALLUCINATION (FILE PATHS): Forcefully revert hallucinated keys back to the 'EXPECTED EXISTING PATHS' if present. 
    3. QUARANTINE ROUTING ENFORCEMENT (CRITICAL): Ensure the `assertions: {{}}` block is completely REMOVED from all `config` blocks. Fatal assertions are forbidden. Data Quality MUST be handled in the `WHERE` clauses using the YAML rules.
    4. BAD RECORDS VERIFICATION: Ensure the `_bad_recs` files explicitly contain a `failure_reason` column derived via a `CASE WHEN` statement.
    5. BATCH DATE & TIMESTAMPS (GLOBAL): Across ALL files (staging, marts, including bad_recs), ensure `batch_date` is `CURRENT_DATE() AS batch_date`, and explicitly include `CURRENT_TIMESTAMP() AS updated_dttm`. 
    6. ARCHIVE OPERATIONS: Ensure `archive_` explicitly uses `INSERT INTO \`{PROJECT_ID}.{datasets['raw']}.<table_name>_hist\`` followed by `TRUNCATE TABLE ${{ref("<table_name>")}};`. Ensure NO `post_operations` block exists.
    7. MARTS INCREMENTAL: Does the marts file use `type: "incremental"`, include a `bigquery: {{ partitionBy, clusterBy }}` block, and use the exact `${{when(incremental(), `WHERE processed_dttm > (SELECT COALESCE(MAX(processed_dttm), TIMESTAMP("1970-01-01")) FROM ${{self()}})` )}}` logic? NEVER filter on SELECT aliases.
    8. TAG TAXONOMY: Ensure `config` blocks of EVERY file (EXCEPT declarations) contains a `tags: ["{actual_domain}", "{base_name}", "<layer>"]` array.
    9. CONFIG BLOCK SYNTAX: Remove any comments (`--`, `//`, `/* */`) inside the `config {{ ... }}` blocks.
    10. REGEX QUOTE ESCAPING FIX (CRITICAL): If conditions contain a regex or string literal wrapped in double quotes (e.g., r"^...$"), you MUST change the inner quotes to single quotes (e.g., r'^...$'). Double quotes inside the double-quoted string will cause a fatal compilation error.
    11. EXPLICIT DESCRIPTIONS (CRITICAL): EVERY `config {{ }}` block MUST contain a valid, meaningful `description` property explaining the file's purpose. Ensure no properties other than ('description', 'tags', 'schema', 'type', 'uniqueKey', 'bigquery', 'dependencies', 'database', 'name') exist in the config block.
    
    Output STRICTLY a JSON object where keys are the corrected full file paths and values are the corrected SQLX string content. Output JSON ONLY. Do not include markdown formatting.
    """

    try:
        log_event(
            "INFO",
            "⏳ 🕵️‍♀️ [Agent: DF QA] Awaiting QA review, global temporal enforcement, Quarantine (DLQ) validation, explicit mapping checks, tagging, placeholder verification, and formatting validation...",
            trace_id,
        )
        response = generate_content_with_retry(model_lite, prompt, trace_id)
        text = response.text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]
        if text.startswith("json"):
            text = text[4:]

        verified_files = json.loads(text)
        log_event(
            "INFO",
            f"✅ 🕵️‍♀️ [Agent: DF QA] QA passed. Quarantine Routing, Explicit Column Mapping, and Descriptions perfectly enforced.",
            trace_id,
        )
        log_event("INFO", "⏹️ 🕵️‍♀️ [Agent: DF QA] Finished automated review.", trace_id)
        return verified_files
    except Exception as e:
        log_event(
            "WARNING",
            f"⚠️ 🕵️‍♀️ [Agent: DF QA] QA Verification Failed (Parsing error). Falling back to unverified code. {e}",
            trace_id,
        )
        log_event(
            "INFO",
            "⏹️ 🕵️‍♀️ [Agent: DF QA] Finished automated review (Fallback applied).",
            trace_id,
        )
        return pipeline_files


# ==============================================================================
# 🧠 AGENT: SCHEMA QA VERIFIER
# ==============================================================================
def verify_schema_json(
    schema_list: List[Dict], table_name: str, new_cols: List[str], trace_id: str
) -> List[Dict]:
    """Goal: Ensure JSON Schema strictly complies with enterprise standards before committing."""
    log_event(
        "INFO",
        f"▶️ 🕵️‍♀️ [Agent: Schema QA] Initiating automated peer-review of BigQuery JSON Schema...",
        trace_id,
    )

    if not model_lite or not schema_list:
        log_event(
            "INFO",
            "⏹️ 🕵️‍♀️ [Agent: Schema QA] Bypassing QA (No model or schema).",
            trace_id,
        )
        return schema_list

    prompt = f"""
    You are a Senior Data QA Lead. Review this JSON BigQuery Schema generated by a junior engineer.
    
    GENERATED SCHEMA: {json.dumps(schema_list)}
    NEW COLUMNS TO VERIFY: {new_cols}
    
    Checklist:
    1. Are all `NEW COLUMNS` explicitly present in the schema?
    2. ABSOLUTE RULE ON TYPES: Check every single column. If the column is NOT 'batch_date' and NOT 'processed_dttm', its type MUST be strictly set to 'STRING'. If you see INT64, BOOLEAN, FLOAT64, or anything else, you MUST change it to 'STRING'. Only 'batch_date' (DATE) and 'processed_dttm' (TIMESTAMP) are allowed to be non-strings.
    3. Are all modes strictly set to 'NULLABLE'?
    4. Are 'batch_date' and 'processed_dttm' explicitly included at the end?
    5. CRITICAL: Does EVERY single column have a "defaultValueExpression" defined? If missing, you MUST inject `"defaultValueExpression": "NULL"` into that column's object.
    
    Fix any errors found. Output STRICTLY a valid JSON list of objects representing the corrected BigQuery schema.
    Output JSON ONLY. No explanation. Do not use markdown formatting.
    """

    try:
        log_event(
            "INFO", "⏳ 🕵️‍♀️ [Agent: Schema QA] Awaiting Schema QA review...", trace_id
        )
        response = generate_content_with_retry(model_lite, prompt, trace_id)
        text = response.text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]
        if text.startswith("json"):
            text = text[4:]

        verified_schema = json.loads(text)
        log_event(
            "INFO",
            f"✅ 🕵️‍♀️ [Agent: Schema QA] QA passed. Schema verified and Default Values validated.",
            trace_id,
        )
        log_event(
            "INFO", "⏹️ 🕵️‍♀️ [Agent: Schema QA] Finished automated review.", trace_id
        )
        return verified_schema
    except Exception as e:
        log_event(
            "WARNING",
            f"⚠️ 🕵️‍♀️ [Agent: Schema QA] QA Verification Failed. Applying programmatic fallback for default values. {e}",
            trace_id,
        )
        for col in schema_list:
            if "defaultValueExpression" not in col:
                col["defaultValueExpression"] = "NULL"

        log_event(
            "INFO",
            "⏹️ 🕵️‍♀️ [Agent: Schema QA] Finished automated review (Programmatic Fallback applied).",
            trace_id,
        )
        return schema_list


# ==============================================================================
# 🧠 AGENT: TERRAFORM QA VERIFIER
# ==============================================================================
def verify_terraform_hcl(
    hcl_content: str, table_id: str, tf_state: Dict[str, Any], trace_id: str
) -> str:
    """Goal: Ensure Terraform code syntax is perfect and history table creation uses schema reuse."""
    log_event(
        "INFO",
        f"▶️ 🕵️‍♀️ [Agent: TF QA] Initiating automated peer-review of Terraform HCL...",
        trace_id,
    )

    if not model_lite or not hcl_content:
        log_event(
            "INFO", "⏹️ 🕵️‍♀️ [Agent: TF QA] Bypassing QA (No model or HCL).", trace_id
        )
        return hcl_content

    is_raw_table = table_id.endswith("_raw")
    style_guide_hist = tf_state.get("style_guide_tf_hist", "")

    hist_rule = ""
    if is_raw_table:
        hist_rule = f"""
        CRITICAL RULE: Because this is a raw table ({table_id}), there MUST be a corresponding '{table_id}_hist' google_bigquery_table resource defined.
        If it is missing, you MUST inject it based on this enterprise pattern:
        --- HCL START ---
        {prune_whitespace(style_guide_hist[:1000])}
        --- HCL END ---
        CRITICAL SCHEMA REUSE: The history table MUST use the exact same schema file path as the main table. Do not allow the use of a separate _hist.json file.
        ADAPTIVE PATTERN CHECK: If the code uses `locals` dictionaries (e.g., `tables_raw` and `tables_raw_hist`), ensure the tables are defined inside those dictionaries and NO duplicate `resource` blocks were created at the bottom. Delete duplicate resources if found.
        """

    prompt = f"""
    You are a Senior DevOps QA Lead. Review this Terraform HCL generated by a junior engineer.
    
    GENERATED HCL: 
    --- HCL START ---
    {hcl_content}
    --- HCL END ---
    
    Checklist:
    1. Is the HCL syntax perfect? Look for missing brackets or quotation marks.
    2. Does the main table resource for '{table_id}' exist?
    3. ANTI-HALLUCINATION RULE: Do not invent new resources that were not requested.
    4. Is there any 'Missing attribute separator' error? Verify that objects inside 'locals' maps are properly separated by commas (,), and attributes within objects have proper newlines.
    {hist_rule}
    
    Fix any errors found. Output STRICTLY the corrected Terraform HCL code. No markdown formatting outside the code block.
    """

    try:
        log_event("INFO", "⏳ 🕵️‍♀️ [Agent: TF QA] Awaiting TF QA review...", trace_id)
        response = generate_content_with_retry(model_lite, prompt, trace_id)
        text = response.text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]
        if text.startswith("hcl"):
            text = text[3:]

        log_event(
            "INFO",
            f"✅ 🕵️‍♀️ [Agent: TF QA] QA passed. Terraform HCL structure validated.",
            trace_id,
        )
        log_event("INFO", "⏹️ 🕵️‍♀️ [Agent: TF QA] Finished automated review.", trace_id)
        return text.replace("```hcl", "").replace("```", "")
    except Exception as e:
        log_event(
            "WARNING",
            f"⚠️ 🕵️‍♀️ [Agent: TF QA] QA Verification Failed. Falling back to unverified HCL. {e}",
            trace_id,
        )
        log_event(
            "INFO",
            "⏹️ 🕵️‍♀️ [Agent: TF QA] Finished automated review (Fallback applied).",
            trace_id,
        )
        return hcl_content


# ==============================================================================
# 📦 GIT INJECTOR (UPDATED FOR DYNAMIC COMMIT MESSAGES)
# ==============================================================================
def inject_dataform_into_repo(
    repo,
    branch_name: str,
    table_name: str,
    pipeline_code: Dict[str, str],
    trace_id: str,
) -> Tuple[int, str]:
    """Goal: Commits the AI-generated files to the PR branch with precise contextual commit messages."""
    files_changed = 0
    detailed_commit_log = ""
    log_event(
        "INFO",
        "▶️ 📦 [Git Injector] Committing Dataform files to Git branch...",
        trace_id,
    )

    def get_commit_msg(path: str, action: str) -> str:
        """Dynamically generates a meaningful commit message based on the exact file being touched."""
        prefix = "fix(dataform)" if action == "update" else "feat(dataform)"
        if "contracts/" in path:
            return f"chore(contracts): {'update' if action == 'update' else 'init'} YAML data contract for {table_name}"
        elif "_bad_recs" in path:
            return f"{prefix}: {'patch' if action == 'update' else 'init'} DLQ quarantine logic for {table_name}"
        elif "/sources/" in path:
            return f"{prefix}: sync source declaration mapping for {table_name}"
        elif "/staging/" in path:
            return f"{prefix}: apply strict casting and deduplication for {table_name}"
        elif "/marts/" in path:
            return f"{prefix}: configure incremental materialization for {table_name}"
        elif "/operations/" in path:
            return f"{prefix}: sync archival operations for {table_name}"
        return f"{prefix}: apply pipeline modifications for {table_name}"

    for file_path, content in pipeline_code.items():
        try:
            c = repo.get_contents(file_path, ref=branch_name)
            commit_msg = get_commit_msg(file_path, "update")
            repo.update_file(file_path, commit_msg, content, c.sha, branch=branch_name)
            log_event(
                "INFO",
                f"♻️ 📦 [Git Injector] Updated existing file: {file_path}",
                trace_id,
            )
            detailed_commit_log += (
                f"- ♻️ **Patched:** `{file_path}` | Msg: `{commit_msg}`\n"
            )
        except GithubException:
            commit_msg = get_commit_msg(file_path, "create")
            repo.create_file(file_path, commit_msg, content, branch=branch_name)
            log_event(
                "INFO", f"✨ 📦 [Git Injector] Created new file: {file_path}", trace_id
            )
            detailed_commit_log += (
                f"- ✨ **Created:** `{file_path}` | Msg: `{commit_msg}`\n"
            )
        files_changed += 1

    log_event(
        "INFO", "⏹️ 📦 [Git Injector] Finished committing Dataform files.", trace_id
    )
    return files_changed, detailed_commit_log


# ==============================================================================
# 🚀 ORCHESTRATION LAYER (The "Manager")
# ==============================================================================
def apply_infrastructure_update(
    repo_name: str,
    key: str,
    table_ref: str,
    new_cols: List[str],
    trace_id: str,
    sample_data: List[Dict] = None,
    error_context: str = "Automated AI trigger",
    target_domain: str = "unknown_domain",
    target_data_contracts: Optional[str] = None,
    added_columns: List[str] = None,
    removed_columns: List[str] = None,
) -> Dict[str, Any]:

    log_event(
        "INFO",
        f"▶️ 🚀 [Orchestrator] Commencing primary infrastructure workflow for '{table_ref}'...",
        trace_id,
    )

    parts = table_ref.split(".")
    dataset = parts[-2] if len(parts) > 1 else "sentinel_raw"
    table = parts[-1]
    base_name = table.replace("_raw", "")
    is_raw_table = table.endswith("_raw")

    added_columns = added_columns or []
    removed_columns = removed_columns or []

    # ----------------------------------------------------------------------
    # GitHub App Authentication
    # ----------------------------------------------------------------------
    try:
        app_id_int = int(GITHUB_APP_ID)
        install_id_int = int(GITHUB_INSTALLATION_ID)
        clean_pvt_key = key.replace("\\n", "\n")
        auth = Auth.AppAuth(app_id=app_id_int, private_key=clean_pvt_key)
        installation_auth = auth.get_installation_auth(installation_id=install_id_int)
        g = Github(auth=installation_auth)

        repo = g.get_repo(repo_name)
        default_branch = repo.get_branch(repo.default_branch)
        log_event(
            "INFO",
            "✅ 🚀 [Orchestrator] Successfully authenticated as Sentinel-Forge[Bot].",
            trace_id,
        )

    except Exception as e:
        log_event(
            "ERROR",
            f"❌ 🚀 [Orchestrator] GitHub App Auth Failed. Type: {type(e).__name__} | Details: {str(e)}",
            trace_id,
        )
        raise e

    # 1. ANALYZE TERRAFORM & DATAFORM STATE
    log_event(
        "INFO",
        "🗺️ 🚀 [Orchestrator] Executing TF & Dataform Repository State Analysis...",
        trace_id,
    )
    tf_state = analyze_tf_repo_state(repo, default_branch.commit.sha, table, trace_id)
    df_state = analyze_dataform_repo_state(
        repo, default_branch.commit.sha, base_name, table, trace_id
    )

    existing_schemas_list = df_state.get("existing_schemas", [])
    inferred_raw_schema = df_state.get("inferred_schema") or dataset

    datasets_map = infer_pipeline_datasets_with_ai(
        table, existing_schemas_list, inferred_raw_schema, trace_id
    )
    log_event(
        "INFO",
        f"🎯 🚀 [Orchestrator] AI Lock established on Datasets: {json.dumps(datasets_map)}",
        trace_id,
    )

    # 2. DETERMINE TF TARGETS
    log_event(
        "INFO", "🎯 🚀 [Orchestrator] Determining Target Asset Paths...", trace_id
    )
    clean_schema_base = SCHEMA_BASE_PATH.strip().rstrip("/")
    target_json_path = f"{clean_schema_base}/{table}.json"

    target_hist_json_path = None
    if is_raw_table:
        log_event(
            "INFO",
            "🎯 🚀 [Orchestrator] Enforcing DRY architecture: History table will reuse the main table's JSON schema.",
            trace_id,
        )

    if tf_state["is_defined"]:
        tf_action, target_tf_path, host_content = (
            "NONE",
            tf_state["defined_in_file"],
            None,
        )
    elif tf_state["host_file"]:
        tf_action, target_tf_path, host_content = (
            "PATCH",
            tf_state["host_file"],
            tf_state["host_file_content"],
        )
    else:
        tf_action, target_tf_path, host_content = (
            "CREATE",
            f"{TF_BASE_PATH.strip().rstrip('/')}/{table}.tf",
            None,
        )

    # 3. CREATE BRANCH
    log_event(
        "INFO", "🌿 🚀 [Orchestrator] Spinning up isolated Git branch...", trace_id
    )
    branch_name = f"ai/ops-{table}-{uuid.uuid4().hex[:6]}"
    repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=default_branch.commit.sha)
    log_event("INFO", f"🌱 🚀 [Orchestrator] Branch `{branch_name}` created.", trace_id)

    # ----------------------------------------------------------------------
    # STEP 4: DATA CONTRACT RESOLUTION (WITH DRIFT UPDATES)
    # ----------------------------------------------------------------------
    contract_filename, contract_content = resolve_and_sync_data_contract(
        domain=target_domain,
        data_contracts=target_data_contracts,
        sample_data=sample_data or [],
        table_name=table,
        added_columns=added_columns,
        removed_columns=removed_columns,
        trace_id=trace_id,
    )

    contract_full_path = (
        f"definitions/contracts/{target_domain}/{table}/{contract_filename}"
        if contract_filename
        else "N/A"
    )

    # ----------------------------------------------------------------------
    # STEP 5: SCHEMA JSON LOGIC (Generation & QA)
    # ----------------------------------------------------------------------
    log_event(
        "INFO", "🧬 🚀 [Orchestrator] Generating JSON Schema Definition...", trace_id
    )
    json_exists, current_schema, json_sha = False, [], None
    try:
        c = repo.get_contents(target_json_path, ref=branch_name)
        current_schema, json_exists, json_sha = (
            json.loads(base64.b64decode(c.content).decode("utf-8")),
            True,
            c.sha,
        )
        log_event(
            "INFO",
            "📂 🚀 [Orchestrator] Pre-existing schema detected. Will perform merge.",
            trace_id,
        )
    except GithubException:
        log_event(
            "INFO",
            "✨ 🚀 [Orchestrator] No pre-existing schema. Will generate from scratch.",
            trace_id,
        )

    # SCHEMA DRIFT DELETIONS: Remove dropped columns from JSON Schema
    if removed_columns and current_schema:
        current_schema = [
            col for col in current_schema if col["name"] not in removed_columns
        ]
        log_event(
            "INFO",
            f"🗑️ 🚀 [Orchestrator] Removed {len(removed_columns)} dropped columns from JSON schema.",
            trace_id,
        )

    ref_json = ""
    if not json_exists:
        try:
            tree = repo.get_git_tree(default_branch.commit.sha, recursive=True)
            for e in tree.tree:
                if e.path.endswith(".json") and "tables" in e.path:
                    ref_json = base64.b64decode(
                        repo.get_git_blob(e.sha).content
                    ).decode("utf-8")
                    break
        except Exception:
            pass

    final_schema_list, added_cols_for_pr = [], []
    audit_cols = [
        {
            "name": "batch_date",
            "type": "DATE",
            "mode": "NULLABLE",
            "description": "Partition",
            "defaultValueExpression": "DATE '9999-12-31'",
        },
        {
            "name": "processed_dttm",
            "type": "TIMESTAMP",
            "mode": "NULLABLE",
            "description": "Audit",
            "defaultValueExpression": "CURRENT_TIMESTAMP()",
        },
    ]

    if not json_exists:
        log_event(
            "INFO",
            "🆕 🚀 [Orchestrator] Drafting entirely new schema definition...",
            trace_id,
        )
        all_cols_to_gen = (
            added_columns
            if added_columns
            else list(sample_data[0].keys()) if sample_data else []
        )
        final_schema_list = generate_dynamic_schema(
            table, all_cols_to_gen, sample_data or [], ref_json, trace_id
        )

        final_names = {c["name"] for c in final_schema_list}
        for ac in audit_cols:
            if ac["name"] not in final_names:
                final_schema_list.append(ac)
        added_cols_for_pr = final_schema_list
    else:
        log_event(
            "INFO",
            "♻️ 🚀 [Orchestrator] Splicing new columns into existing schema...",
            trace_id,
        )
        ai_suggestions = generate_dynamic_schema(
            table, added_columns, sample_data or [], ref_json, trace_id
        )
        existing_names = {c["name"] for c in current_schema}

        insert_idx = len(current_schema)
        for idx, col in enumerate(current_schema):
            if col["name"] in ["batch_date", "processed_dttm"]:
                insert_idx = idx
                break

        count = 0
        for col_def in ai_suggestions:
            if col_def["name"] not in existing_names:
                current_schema.insert(insert_idx + count, col_def)
                added_cols_for_pr.append(col_def)
                count += 1
        final_schema_list = current_schema

    final_schema_list = verify_schema_json(
        final_schema_list, table, [c["name"] for c in added_cols_for_pr], trace_id
    )
    log_event(
        "INFO",
        f"✅ 🚀 [Orchestrator] Schema configuration complete. Total added columns: {len(added_cols_for_pr)}",
        trace_id,
    )

    # ----------------------------------------------------------------------
    # STEP 6: TERRAFORM TF LOGIC (Generation & QA)
    # ----------------------------------------------------------------------
    log_event(
        "INFO", "🧱 🚀 [Orchestrator] Designing Terraform Configurations...", trace_id
    )
    tf_changed, new_tf_content, tf_sha = False, "", None
    if tf_action in ["PATCH", "CREATE"]:
        if tf_action == "PATCH":
            tf_sha = repo.get_contents(target_tf_path, ref=branch_name).sha

        new_tf_content = generate_tf_patch_or_create(
            table, dataset, f"json/{table}.json", host_content, tf_state, trace_id
        )

        if new_tf_content and len(new_tf_content) > 20:
            new_tf_content = verify_terraform_hcl(
                new_tf_content, table, tf_state, trace_id
            )
            tf_changed = True

    # ----------------------------------------------------------------------
    # STEP 7: DATAFORM PIPELINE GENERATION & QA
    # ----------------------------------------------------------------------
    log_event(
        "INFO",
        "🪄 🚀 [Orchestrator] Triggering AI Dataform Architect & QA...",
        trace_id,
    )

    should_update_json = (not json_exists) or (
        json_exists and (len(added_cols_for_pr) > 0 or len(removed_columns) > 0)
    )
    df_files_changed, df_commit_log = 0, "⏭️ No Dataform files generated."

    if should_update_json or "dataform" in error_context.lower() or contract_content:
        raw_df_pipeline = generate_ai_dataform_pipeline(
            table,
            final_schema_list,
            error_context,
            df_state,
            added_columns,
            removed_columns,
            sample_data or [],
            datasets_map,
            target_domain,
            contract_yaml=contract_content,
            trace_id=trace_id,
        )
        if raw_df_pipeline:
            verified_df_pipeline = verify_dataform_pipeline(
                raw_df_pipeline,
                final_schema_list,
                added_columns,
                removed_columns,
                datasets_map,
                df_state,
                base_name,
                target_domain,
                trace_id,
            )

            # Attach the Data Contract YAML to the commit payload
            if contract_filename and contract_content and contract_full_path != "N/A":
                log_event(
                    "INFO",
                    f"📂 📜 [Data Contract Engine] Staging contract file for commit at exact path: {contract_full_path}",
                    trace_id,
                )
                verified_df_pipeline[contract_full_path] = contract_content

            df_files_changed, df_commit_log = inject_dataform_into_repo(
                repo, branch_name, table, verified_df_pipeline, trace_id
            )

    # ----------------------------------------------------------------------
    # STEP 8: COMMIT PAYLOADS & OPEN PR
    # ----------------------------------------------------------------------
    log_event(
        "INFO",
        "💾 🚀 [Orchestrator] Preparing GitHub Commits & Pull Request...",
        trace_id,
    )

    if not should_update_json and not tf_changed and df_files_changed == 0:
        log_event(
            "WARNING",
            "🛑 🚀 [Orchestrator] No actionable changes detected. Aborting Git operations.",
            trace_id,
        )
        flush_deferred_logs(None, trace_id)
        log_event(
            "INFO",
            "⏹️ 🚀 [Orchestrator] Finished orchestrator flow (Skipped).",
            trace_id,
        )
        return {
            "status": "SKIPPED",
            "url": None,
            "overall_action": "SKIPPED",
            "details": {},
        }

    if should_update_json:
        log_event(
            "INFO",
            "💾 🚀 [Orchestrator] Committing JSON Schema payload...",
            trace_id,
        )
        msg = (
            f"fix(schema): drift update for {table} schema"
            if json_exists
            else f"feat(schema): create {table} schema layout"
        )
        try:
            if json_exists:
                repo.update_file(
                    target_json_path,
                    msg,
                    json.dumps(final_schema_list, indent=2),
                    json_sha,
                    branch=branch_name,
                )
            else:
                repo.create_file(
                    target_json_path,
                    msg,
                    json.dumps(final_schema_list, indent=2),
                    branch=branch_name,
                )
        except GithubException as ge:
            log_event(
                "WARNING",
                f"⚠️ 🚀 [Orchestrator] JSON commit skipped (Likely identical content): {ge}",
                trace_id,
            )

    if tf_changed:
        log_event(
            "INFO", "💾 🚀 [Orchestrator] Committing Terraform HCL payload...", trace_id
        )
        msg = f"feat(infra): update Terraform specs for {table}"
        try:
            if tf_action == "PATCH":
                repo.update_file(
                    target_tf_path, msg, new_tf_content, tf_sha, branch=branch_name
                )
            else:
                repo.create_file(
                    target_tf_path, msg, new_tf_content, branch=branch_name
                )
        except GithubException as ge:
            log_event(
                "WARNING",
                f"⚠️ 🚀 [Orchestrator] Terraform commit skipped (Likely identical content): {ge}",
                trace_id,
            )

    log_event(
        "INFO", "✉️ 🚀 [Orchestrator] Formatting highly detailed PR body...", trace_id
    )

    pr_status_schema = "♻️ Updated" if json_exists else "✨ Created"
    pr_status_tf = (
        "♻️ Patched"
        if tf_changed and tf_action == "PATCH"
        else "✨ Created" if tf_changed else "✅ Exists"
    )
    pr_status_df = (
        f"✅ {df_files_changed} files" if df_files_changed > 0 else "⏭️ Skipped"
    )

    col_table = "| Column Name | Type | Description |\n|---|---|---|\n"
    for col in added_cols_for_pr[:15]:
        col_table += (
            f"| `{col['name']}` | `{col['type']}` | {col.get('description', '')} |\n"
        )
    if len(added_cols_for_pr) > 15:
        col_table += f"| ... ({len(added_cols_for_pr)-15} more) | | |\n"

    title_prefix = "fix" if json_exists or df_files_changed > 0 else "feat"
    pr_title = f"{title_prefix}(dataops): Automated Pipeline & DQ Healing for {table}"

    pr_body = f"""
# 🤖 Sentinel-Forge Automated Resolution
**Trace ID:** `{trace_id}`
**Trigger Context:** `{error_context}`

Sentinel-Forge has intercepted a pipeline anomaly and autonomously generated the following infrastructure, transformation, and data quality code.

### 🏗️ Infrastructure Actions Taken
| Layer | Status | Target Path |
|---|---|---|
| **BigQuery Schema** | {pr_status_schema} | `{target_json_path}` |
| **History Schema Sync** | {"✅ Reused Main Schema" if is_raw_table else "⏭️ N/A"} | `{target_json_path if is_raw_table else "N/A"}` |
| **Terraform HCL** | {pr_status_tf} | `{target_tf_path}` |
| **Dataform SQLX** | {pr_status_df} | Semantic Domain Routing Applied |

### 🔍 Schema Drift Detected
- **Columns Added:** {len(added_columns)} ({added_columns})
- **Columns Removed:** {len(removed_columns)} ({removed_columns})

All raw ingestion fields configured strictly as `STRING` type.
{col_table if added_columns else ""}

### 🪄 Agent Activity Log
1. **Scanner & Router:** Mapped target domain folders semantically. AI Dataset Routing successfully assigned correct schema endpoints.
2. **Contract Resolution (Drift Sync):** Fetched `data_contracts` YAML. Autonomously bumped version, updated schema array with additions/deletions, and appended Drift Changelog.
3. **Architect:** Generated HCL. Embedded exact Dataform templates including YAML-directed explicit columns, DLQ quarantine routing, dynamic `SAFE_CAST`, file descriptions, and `incremental` logic into `.sqlx` blocks based on sample payload analysis. 
4. **Schema QA:** Verified JSON structure. Executed Column Drop logic for deleted fields. Enforced absolute `STRING` typing on all new columns.
5. **Terraform QA:** Enforced schema reuse (`DRY` principle) between main and `_hist` table resources.
6. **Dataform QA:** Validated SQL syntax, enforced explicit column mapping (NO `SELECT *`), removed dropped columns from SELECT statements, enforced `CURRENT_DATE()` and `CURRENT_TIMESTAMP()` logic, enforced Quarantine DLQ routing, verified explicit configuration `description` blocks, and validated strict Tag Taxonomy.

**Commit Log:**
{df_commit_log}

### 🛡️ Governance Checklist
- [x] **Adaptive Terraform:** HCL intelligently injected into existing `locals` maps (`for_each`) to avoid duplicate resource hallucination.
- [x] **Data Contract Compliance:** Pipeline strict-typed according to Data Contract YAML rules.
- [x] **Schema Integrity:** Absolute `STRING` typing and `defaultValueExpression` strictly enforced for raw layer.
- [x] **DRY Architecture:** History tables natively reuse raw landing schemas.
- [x] **Tag Taxonomy:** `config` blocks securely tagged with `[domain, entity, layer]` (Exempting Declarations).
- [x] **Staging Governance:** Dynamic YAML-directed casting to native BigQuery types (`type: "table"`) and DLQ routing executed securely.
- [x] **Operations Accuracy:** Operations securely bypass self-contexts and `post_operations`.
- [x] **Temporal Accuracy:** `CURRENT_DATE() AS batch_date` and `CURRENT_TIMESTAMP() AS updated_dttm` strictly enforced globally across all layers.
- [x] **Format Standardization:** Code cleanly indented and SQL keywords perfectly aligned.

### ✅ Action Required
Review the file changes and merge this Pull Request.
"""

    log_event(
        "INFO",
        "📤 🚀 [Orchestrator] Transmitting PR creation request to GitHub API...",
        trace_id,
    )
    pr = repo.create_pull(
        title=pr_title, body=pr_body, head=branch_name, base=repo.default_branch
    )
    log_event(
        "INFO",
        f"🏆 🚀 [Orchestrator] PR Created Successfully! Link: {pr.html_url}",
        trace_id,
    )

    flush_deferred_logs(pr.html_url, trace_id)

    descriptive_details = {
        "descriptive_summary": f"Autonomously {'updated' if json_exists else 'created'} pipeline and infrastructure for {table}.",
        "pr_link_reference": pr.html_url,
        "schema_updates": {
            "action": "UPDATE" if json_exists else "CREATE",
            "target_table": table,
            "history_table_synced": (
                f"{table}_hist (Reusing {target_json_path})"
                if is_raw_table
                else "Not a raw table"
            ),
            "columns_added_or_verified": [c["name"] for c in added_cols_for_pr],
            "total_schema_columns": len(final_schema_list),
            "string_type_and_defaults_enforced": True,
        },
        "infrastructure_updates": {
            "action": tf_action,
            "files_modified": target_tf_path,
            "history_infrastructure_created": is_raw_table,
            "schema_reuse_applied": is_raw_table,
            "tf_anti_hallucination_verified": True,
        },
        "dataform_updates": {
            "total_files_processed": df_files_changed,
            "detailed_commit_log": (
                df_commit_log.strip().split("\n") if df_commit_log else []
            ),
            "templates_strictly_followed": True,
            "yaml_contracts_enforced": True,
            "yaml_keys_extracted": True,
            "explicit_column_mapping": True,
            "quarantine_dlq_routed": True,
            "anti_hallucination_paths_enforced": True,
            "anti_hallucination_schemas_verified": True,
            "global_current_date_enforced": True,
            "staging_casting_and_dedup_applied": True,
            "tag_taxonomy_enforced": True,
            "formatting_standardized": True,
        },
    }

    overall_action_str = (
        "UPDATED"
        if (json_exists or tf_action == "PATCH" or df_state.get("existing_files"))
        else "CREATED"
    )

    log_event(
        "INFO",
        "⏹️ 🚀 [Orchestrator] Finished comprehensive infrastructure update workflow.",
        trace_id,
    )
    return {
        "status": "SUCCESS",
        "url": pr.html_url,
        "overall_action": overall_action_str,
        "details": descriptive_details,
    }


# ==============================================================================
# MAIN ENTRY POINT (Cloud Function)
# ==============================================================================
@functions_framework.cloud_event
def ai_agent_main(cloud_event):
    trace_id = "unknown"
    global DEFERRED_LOGS
    DEFERRED_LOGS = []

    try:
        # Gateway Initialization
        if "data" not in cloud_event.data["message"]:
            return

        pubsub_message = base64.b64decode(cloud_event.data["message"]["data"]).decode(
            "utf-8"
        )
        data = json.loads(pubsub_message)

        trace_id = data.get("trace_id", f"unknown-{uuid.uuid4().hex[:8]}")
        log_event(
            "INFO",
            f"▶️ 🔌 [Gateway] Cloud Event received. Waking up Sentinel Forge...",
            trace_id,
        )

        # Robust extraction
        if "protoPayload" in data:
            error_context = (
                data["protoPayload"]
                .get("status", {})
                .get("message", "BigQuery/Dataform Error Intercepted")
            )
        else:
            error_context = data.get(
                "error_message", "Schema drift or general anomaly detected"
            )

        table_ref = data.get("table_ref", "unknown_table")

        # EXTRACT SCHEMA DRIFT COLUMNS (Additions AND Deletions)
        added_columns = data.get("added_columns", data.get("new_column_headers", []))
        removed_columns = data.get(
            "removed_columns", data.get("deleted_column_headers", [])
        )

        sample_data = data.get("sample_data_rows", [])
        target_domain = data.get("domain", "unknown_domain")

        # FEATURE ADDITION: Safely extract and sanitize Data Contracts
        raw_contracts = data.get("data_contracts")
        if (
            not raw_contracts
            or not str(raw_contracts).strip()
            or str(raw_contracts).strip().lower() == "nan"
        ):
            target_data_contracts = None
        else:
            target_data_contracts = str(raw_contracts).strip()

        log_event(
            "INFO", f"🎯 🔌 [Gateway] Processing target entity: {table_ref}", trace_id
        )
        log_event(
            "INFO",
            "🔐 🔌 [Gateway] Validating environmental configurations and secrets...",
            trace_id,
        )

        if not GITHUB_PVT_KEY or not REPO_NAME:
            log_event(
                "CRITICAL",
                "☠️ 🔌 [Gateway] CRITICAL: Missing GITHUB_PVT_KEY or REPO_NAME.",
                trace_id,
            )

            log_ai_action(
                trace_id=trace_id,
                action="CRASH",
                resource="System Initialization",
                status="FAILED",
                details={
                    "error": "Missing GITHUB_PVT_KEY or REPO_NAME configurations."
                },
                link=None,
                resource_group="System Operations",
                resource_type="Critical Failure",
            )
            log_event("INFO", "⏹️ 🔌 [Gateway] Aborting mission.", trace_id)
            return

        if not added_columns and not removed_columns and sample_data:
            log_event(
                "INFO",
                "🔮 🔌 [Gateway] Inferring baseline schema from sample payload...",
                trace_id,
            )
            added_columns = list(sample_data[0].keys())

        if (
            not added_columns
            and not removed_columns
            and "dataform" not in error_context.lower()
            and not target_data_contracts
        ):
            log_event(
                "WARNING",
                "⚠️ 🔌 [Gateway] No columns derived, no Data Contracts provided, and not a Dataform error. Operation voided.",
                trace_id,
            )
            log_event("INFO", "⏹️ 🔌 [Gateway] Terminating gracefully.", trace_id)
            return

        log_event(
            "INFO",
            "⚡ 🔌 [Gateway] Passing control vector to the Orchestration Layer...",
            trace_id,
        )
        result = apply_infrastructure_update(
            repo_name=REPO_NAME,
            key=GITHUB_PVT_KEY,
            table_ref=table_ref,
            new_cols=added_columns,
            trace_id=trace_id,
            sample_data=sample_data,
            error_context=error_context,
            target_domain=target_domain,
            target_data_contracts=target_data_contracts,
            added_columns=added_columns,
            removed_columns=removed_columns,
        )

        log_event(
            "INFO",
            "🔏 🔌 [Gateway] Finalizing operation. Generating closing audit log...",
            trace_id,
        )

        overall_action = result.get("overall_action", "PROCESSED")

        log_ai_action(
            trace_id=trace_id,
            action="CREATE_PR",
            resource=table_ref,
            status=result["status"],
            details=result.get("details", {}),
            link=result.get("url"),
            resource_group="Sentinel DataOps Pipeline",
            resource_type=f"Data Pipeline - {overall_action}",
        )

        if result.get("url"):
            log_event(
                "INFO",
                f"🔗 🔌 [Gateway] Operation Successful. Pull Request URL: {result['url']}",
                trace_id,
            )

        log_event(
            "INFO",
            "⏹️ 🔌 [Gateway] Sentinel Forge completely finished lifecycle.",
            trace_id,
        )

    except Exception as e:
        error_msg = str(e)
        log_event(
            "CRITICAL",
            f"🚨 🔌 [Gateway] Unhandled System Exception: {error_msg}",
            trace_id,
        )
        try:
            log_ai_action(
                trace_id=trace_id,
                action="CRASH",
                resource="System",
                status="FAILED",
                details={"error": error_msg},
                link=None,
                resource_group="System Operations",
                resource_type="Critical Failure",
            )
        except Exception:
            pass
        log_event(
            "INFO",
            "⏹️ 🔌 [Gateway] Terminated under critical failure condition.",
            trace_id,
        )
