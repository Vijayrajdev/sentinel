import base64
import json
import os
import uuid
import datetime
import logging
import re
from typing import List, Dict, Any, Optional, Union, Tuple

import functions_framework
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig
from github import Github, GithubException
from google.cloud import bigquery

# ==============================================================================
# CONFIGURATION
# ==============================================================================
PROJECT_ID = os.environ.get("GCP_PROJECT")
REGION = os.environ.get("GCP_REGION", "us-central1")
REPO_NAME = os.environ.get("REPO_NAME")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
AI_AUDIT_TABLE = os.environ.get("AI_AUDIT_TABLE", "sentinel_audit.ai_ops_log")

# 📂 PATH CONFIGURATION (Must end with /)
SCHEMA_BASE_PATH = os.environ.get("SCHEMA_BASE_PATH", "infra/bigquery/tables/json/")
TF_BASE_PATH = os.environ.get("TF_BASE_PATH", "infra/bigquery/tables/")

bq_client: Optional[bigquery.Client] = None

# Initialize Vertex AI
try:
    if PROJECT_ID:
        vertexai.init(project=PROJECT_ID, location=REGION)
        model = GenerativeModel("gemini-2.5-pro")
except Exception as e:
    logging.error(f"Failed to initialize Vertex AI: {e}")
    model = None

logging.basicConfig(level=logging.INFO)


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================
def get_bq_client():
    global bq_client
    if not bq_client:
        bq_client = bigquery.Client()
    return bq_client


def log_event(severity: str, message: str, trace_id: str, **kwargs):
    """Structured JSON logging for Cloud Logging."""
    entry = {
        "severity": severity,
        "message": message,
        "logging.googleapis.com/trace": trace_id,
        "component": "sentinel-forge",
        **kwargs,
    }
    print(json.dumps(entry))


def log_ai_action(
    trace_id: str,
    action: str,
    resource: str,
    status: str,
    details: Any,
    link: Optional[str] = None,
):
    """Writes a permanent record of the AI's actions to BigQuery."""
    client = get_bq_client()

    row = {
        "operation_id": uuid.uuid4().hex,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "ai_agent_id": "Sentinel-Forge-v2",
        "resource_group": "GitHub Repo",
        "resource_type": "Schema & TF",
        "resource_id": resource,
        "action_type": action,
        "change_payload": json.dumps(details),
        "outcome_status": status,
        "reference_link": link,
    }

    try:
        errors = client.insert_rows_json(AI_AUDIT_TABLE, [row])
        if errors:
            log_event("ERROR", f"Failed to write audit log: {errors}", trace_id)
    except Exception as e:
        log_event("ERROR", f"Audit Log Crash: {e}", trace_id)


def find_defining_tf_file(repo, branch_sha, table_id, trace_id) -> Optional[str]:
    """
    Scans ALL .tf files in TF_BASE_PATH to see if the table is already defined.
    Returns the path of the file if found, otherwise None.
    """
    log_event(
        "INFO",
        f"🔍 Deep Scan: Searching for existing definition of '{table_id}'...",
        trace_id,
    )

    try:
        tree = repo.get_git_tree(branch_sha, recursive=True)
        # Filter for .tf files in the base path
        tf_files = [
            e
            for e in tree.tree
            if e.path.startswith(TF_BASE_PATH) and e.path.endswith(".tf")
        ]

        for element in tf_files:
            blob = repo.get_git_blob(element.sha)
            content = base64.b64decode(blob.content).decode("utf-8")

            # Smart Regex: Look for 'table_id = "my_table"' OR 'table_id="my_table"'
            # Matches: resource "..." { table_id = "xyz" } OR module "..." { table_id = "xyz" }
            pattern = rf'table_id\s*=\s*["\']{re.escape(table_id)}["\']'

            if re.search(pattern, content):
                log_event(
                    "INFO", f"✅ Found existing definition in: {element.path}", trace_id
                )
                return element.path

    except Exception as e:
        log_event("WARNING", f"Deep Scan failed: {e}", trace_id)

    log_event("INFO", f"⚪ Table definition NOT found. Will create new file.", trace_id)
    return None


# ==============================================================================
# 🕵️ REPO INTELLIGENCE
# ==============================================================================
def fetch_reference_files(repo, branch_sha, trace_id) -> Tuple[str, str]:
    """
    Scans the repo to find ONE existing .tf file and ONE existing .json file
    to use as 'Style Guides'.
    """
    log_event("INFO", "📚 Scanning repo for Reference Style Guides...", trace_id)
    ref_tf_content = ""
    ref_json_content = ""

    try:
        tree = repo.get_git_tree(branch_sha, recursive=True)

        # 1. Find a Reference Terraform File
        for element in tree.tree:
            if element.path.startswith(TF_BASE_PATH) and element.path.endswith(".tf"):
                blob = repo.get_git_blob(element.sha)
                ref_tf_content = base64.b64decode(blob.content).decode("utf-8")
                log_event("INFO", f"✅ Found Reference TF: {element.path}", trace_id)
                break

        # 2. Find a Reference Schema File
        for element in tree.tree:
            if element.path.startswith(SCHEMA_BASE_PATH) and element.path.endswith(
                ".json"
            ):
                blob = repo.get_git_blob(element.sha)
                ref_json_content = base64.b64decode(blob.content).decode("utf-8")
                log_event("INFO", f"✅ Found Reference JSON: {element.path}", trace_id)
                break

    except Exception as e:
        log_event("WARNING", f"Reference fetch warning: {e}", trace_id)

    return ref_tf_content, ref_json_content


# ==============================================================================
# 🧠 CORE INTELLIGENCE
# ==============================================================================
def generate_dynamic_schema(
    table_name: str,
    new_cols: List[str],
    sample_data: List[Dict],
    reference_json: str,
    trace_id: str,
) -> List[Dict]:
    """
    Generates JSON schema by mimicking the style of 'reference_json'.
    """
    if not model:
        return [{"name": c, "type": "STRING", "mode": "NULLABLE"} for c in new_cols]

    log_event("INFO", f"🧠 Generating Schema for {len(new_cols)} columns...", trace_id)

    # If no reference JSON exists, use a simple prompt
    style_instruction = ""
    if reference_json:
        style_instruction = f"""
    STYLE GUIDE (Follow this existing file's format exactly):
    ```json
    {reference_json[:2000]} 
    ```
    """

    prompt = f"""
    You are a Data Architect.
    
    Task: Generate a BigQuery Schema JSON for table "{table_name}".
    
    Input Data:
    - New Columns to Add: {new_cols}
    - Sample Data: {json.dumps(sample_data[:3])}
    
    {style_instruction}
    
    Requirements:
    1. Output strictly a JSON list of objects.
    2. All types must be 'STRING' (Raw Layer Standard).
    3. Mode must be 'NULLABLE'.
    4. Add descriptions based on the column name.
    
    Output:
    JSON Array only.
    """

    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]
        return json.loads(text)
    except Exception as e:
        log_event("ERROR", f"Schema Generation Failed: {e}", trace_id)
        return [{"name": c, "type": "STRING", "mode": "NULLABLE"} for c in new_cols]


def generate_dynamic_tf(
    table_id: str, dataset_id: str, schema_path: str, reference_tf: str, trace_id: str
) -> str:
    """
    Generates Terraform code by mimicking the style of 'reference_tf'.
    """
    if not model:
        return ""

    log_event("INFO", f"🏗️ Generating Terraform Resource for {table_id}...", trace_id)

    prompt = f"""
    You are a Terraform Expert.
    
    Task: Write the Terraform configuration for a new BigQuery table.
    
    Context:
    - Resource Name: {table_id.replace('.', '_').replace('-', '_')}
    - Table ID: {table_id}
    - Dataset ID: {dataset_id}
    - Schema File Path: {schema_path}
    
    REFERENCE STYLE (Copy this coding style exactly):
    ```hcl
    {reference_tf[:3000]}
    ```
    
    Instructions:
    1. Copy the indentation, variable usage, and labeling strategy.
    2. Ensure `deletion_protection = false`.
    3. Include `time_partitioning` (DAY) if the reference has it.
    
    Output:
    Return ONLY the HCL code block. No markdown.
    """

    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]

        text = text.replace("```hcl", "").replace("```", "")
        return text
    except Exception as e:
        log_event("ERROR", f"TF Generation Failed: {e}", trace_id)
        return ""


# ==============================================================================
# 🚀 ORCHESTRATION (The "Hands")
# ==============================================================================
def apply_infrastructure_update(
    repo_name: str,
    token: str,
    table_ref: str,
    new_cols: List[str],
    trace_id: str,
    sample_data: List[Dict] = None,
) -> Union[str, None]:

    parts = table_ref.split(".")
    dataset = parts[-2]
    table = parts[-1]

    log_event("INFO", f"🐙 Connecting to GitHub Repo: {repo_name}", trace_id)
    g = Github(token)
    repo = g.get_repo(repo_name)
    default_branch = repo.get_branch(repo.default_branch)

    # 1. FETCH REFERENCES
    ref_tf, ref_json = fetch_reference_files(repo, default_branch.commit.sha, trace_id)

    # 2. DETERMINE PATHS (Deep Scan Logic)
    clean_schema_base = SCHEMA_BASE_PATH.strip().rstrip("/")
    target_json_path = f"{clean_schema_base}/{table}.json"

    # 🕵️ Check if TF is already defined in ANY file
    existing_tf_path = find_defining_tf_file(
        repo, default_branch.commit.sha, table, trace_id
    )

    if existing_tf_path:
        target_tf_path = existing_tf_path
        tf_already_defined = True
        tf_status_msg = "✅ Exists (Unchanged)"
    else:
        # If not found, default to standard naming
        clean_tf_base = TF_BASE_PATH.strip().rstrip("/")
        target_tf_path = f"{clean_tf_base}/{table}.tf"
        tf_already_defined = False
        tf_status_msg = "✨ Created"

    log_event(
        "INFO", f"🎯 Targets: JSON={target_json_path}, TF={target_tf_path}", trace_id
    )

    # 3. BRANCH
    branch_name = f"ai/ops-{table}-{uuid.uuid4().hex[:6]}"
    repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=default_branch.commit.sha)
    log_event("INFO", f"🌿 Created Branch: {branch_name}", trace_id)

    # ----------------------------------------------------------------------
    # STEP A: SCHEMA JSON
    # ----------------------------------------------------------------------
    json_exists = False
    current_schema = []
    json_sha = None

    try:
        c = repo.get_contents(target_json_path, ref=branch_name)
        current_schema = json.loads(base64.b64decode(c.content).decode("utf-8"))
        json_exists = True
        json_sha = c.sha
        log_event("INFO", "📂 Schema file exists. Operation: UPDATE.", trace_id)
    except GithubException:
        json_exists = False
        log_event(
            "WARNING", "✨ Schema file missing. Operation: CREATE NEW TABLE.", trace_id
        )

    # AI Generation / Merging
    final_schema_list = []
    added_cols_for_pr = []

    # 🛡️ AUDIT COLUMNS
    audit_cols = [
        {
            "name": "batch_date",
            "type": "DATE",
            "mode": "NULLABLE",
            "description": "Partition",
            "default_value_expression": "'9999-12-31'",
        },
        {
            "name": "processed_dttm",
            "type": "TIMESTAMP",
            "mode": "NULLABLE",
            "description": "Audit",
            "default_value_expression": "CURRENT_TIMESTAMP()",
        },
    ]

    if not json_exists:
        # BRAND NEW TABLE
        all_cols_to_gen = new_cols if new_cols else list(sample_data[0].keys())
        final_schema_list = generate_dynamic_schema(
            table, all_cols_to_gen, sample_data, ref_json, trace_id
        )

        # Enforce Audit
        final_names = {c["name"] for c in final_schema_list}
        for ac in audit_cols:
            if ac["name"] not in final_names:
                final_schema_list.append(ac)

        added_cols_for_pr = final_schema_list

    else:
        # UPDATE EXISTING
        ai_suggestions = generate_dynamic_schema(
            table, new_cols, sample_data, ref_json, trace_id
        )
        existing_names = {c["name"] for c in current_schema}

        insert_idx = len(current_schema)
        for idx, col in enumerate(current_schema):
            if col["name"] in ["batch_date", "processed_dttm"]:
                insert_idx = idx
                break

        added_count = 0
        for col_def in ai_suggestions:
            if col_def["name"] not in existing_names:
                current_schema.insert(insert_idx + added_count, col_def)
                added_cols_for_pr.append(col_def)
                added_count += 1

        final_schema_list = current_schema

    # ----------------------------------------------------------------------
    # STEP B: TERRAFORM TF
    # ----------------------------------------------------------------------
    tf_changed = False
    new_tf_content = ""

    # Logic: Only create TF if it DOES NOT EXIST (tf_already_defined is False)
    # If it already exists, we assume the TF is fine and we just updated the schema JSON it points to.

    if tf_already_defined:
        log_event(
            "INFO",
            "✅ Terraform resource already exists. No TF changes needed.",
            trace_id,
        )
    else:
        # We need to check if the file target_tf_path physically exists (double check in case Deep Scan missed a phantom file)
        try:
            repo.get_contents(target_tf_path, ref=branch_name)
            tf_exists_physical = True
            tf_status_msg = "✅ Exists (Unchanged)"
        except GithubException:
            tf_exists_physical = False

        if not tf_exists_physical:
            log_event(
                "INFO", "✨ Terraform missing. Generating from scratch...", trace_id
            )
            rel_schema_path = f"./json/{table}.json"
            new_tf_content = generate_dynamic_tf(
                table, dataset, rel_schema_path, ref_tf, trace_id
            )
            if new_tf_content and len(new_tf_content.strip()) > 10:
                tf_changed = True
                tf_status_msg = "✨ Created"

    # ----------------------------------------------------------------------
    # STEP C: COMMIT & PR
    # ----------------------------------------------------------------------
    should_update_json = (not json_exists) or (
        json_exists and len(added_cols_for_pr) > 0
    )

    if not should_update_json and not tf_changed:
        log_event(
            "WARNING", "🛑 No actionable changes detected. Aborting PR.", trace_id
        )
        return None

    if should_update_json:
        content_str = json.dumps(final_schema_list, indent=2)
        msg = (
            f"fix: update {table} schema"
            if json_exists
            else f"feat: create {table} schema"
        )
        if json_exists:
            repo.update_file(
                target_json_path, msg, content_str, json_sha, branch=branch_name
            )
        else:
            repo.create_file(target_json_path, msg, content_str, branch=branch_name)
        log_event("INFO", f"💾 JSON Schema committed to {target_json_path}", trace_id)

    if tf_changed:
        repo.create_file(
            target_tf_path,
            f"feat: create {table} terraform",
            new_tf_content,
            branch=branch_name,
        )
        log_event(
            "INFO", f"💾 Terraform Resource committed to {target_tf_path}", trace_id
        )

    # ----------------------------------------------------------------------
    # STEP D: PR TEMPLATES
    # ----------------------------------------------------------------------
    col_table = "| Column Name | Type | Description |\n|---|---|---|\n"
    for col in added_cols_for_pr[:15]:
        col_table += (
            f"| `{col['name']}` | `{col['type']}` | {col.get('description', '')} |\n"
        )
    if len(added_cols_for_pr) > 15:
        col_table += f"| ... ({len(added_cols_for_pr)-15} more) | | |\n"

    # Determine Title & Header
    if not json_exists:
        pr_title = f"feat(data): Create New Table {table}"
        header = f"# 🚀 Sentinel Forge: New Table Creation\n*Trace ID:* `{trace_id}`"
        summary = "The AI Agent has detected a new data entity and generated the full infrastructure code."
    else:
        pr_title = f"fix(data): Schema Drift in {table}"
        header = f"# 🚨 Sentinel Forge: Schema Drift Detected\n*Trace ID:* `{trace_id}`"
        summary = f"New columns were detected in the ingestion source for `{table}`."

    pr_body = f"""
{header}

### 📦 Summary
{summary}

| Resource | Status | Path |
|---|---|---|
| **Terraform** | {tf_status_msg} | `{target_tf_path}` |
| **Schema** | {'✨ Created' if not json_exists else '♻️ Updated'} | `{target_json_path}` |

### 🔍 Schema Definition ({len(added_cols_for_pr)} Cols)
{col_table}

### 🛡️ Governance Enforcement
* **Partitioning:** `batch_date` (Default: `9999-12-31`)
* **Audit:** `processed_dttm` (Default: `CURRENT_TIMESTAMP()`)
* **Type Safety:** All data columns enforced as `STRING`.

### ✅ Action Required
Merge this PR. Terraform will apply the changes.
"""

    pr = repo.create_pull(
        title=pr_title, body=pr_body, head=branch_name, base=repo.default_branch
    )

    log_event("INFO", f"✅ PR Created Successfully: {pr.html_url}", trace_id)
    return pr.html_url


# ==============================================================================
# MAIN ENTRY POINT (Cloud Function)
# ==============================================================================
@functions_framework.cloud_event
def ai_agent_main(cloud_event):
    """
    Triggered by Pub/Sub message from Sentinel Ingestor.
    """
    trace_id = "unknown"
    table_ref = "unknown"

    try:
        # 1. Parse Pub/Sub Message
        if "data" in cloud_event.data["message"]:
            pubsub_message = base64.b64decode(
                cloud_event.data["message"]["data"]
            ).decode("utf-8")
            data = json.loads(pubsub_message)
        else:
            log_event("ERROR", "Received empty Pub/Sub message.", trace_id)
            return

        trace_id = data.get("trace_id", f"unknown-{uuid.uuid4()}")
        table_ref = data.get("table_ref")
        new_cols = data.get("new_column_headers", [])
        sample_data = data.get("sample_data_rows", [])

        log_event("INFO", f"🤖 AI Agent Waking Up. Target: {table_ref}", trace_id)

        # 2. Validation
        if not GITHUB_TOKEN or not REPO_NAME:
            log_event("CRITICAL", "Missing GITHUB_TOKEN or REPO_NAME.", trace_id)
            return

        # 3. Handle Empty Column List (Case: Ingestor saw Table Missing, sent samples)
        if not new_cols:
            if sample_data:
                new_cols = list(sample_data[0].keys())
                log_event(
                    "INFO",
                    f"Inferred {len(new_cols)} columns from sample data.",
                    trace_id,
                )
            else:
                log_event(
                    "WARNING", "No columns or sample data provided. Aborting.", trace_id
                )
                return

        # 4. EXECUTE INFRASTRUCTURE UPDATE
        # Note: We pass 'new_cols' directly. The decision to ask Gemini happens inside apply_infrastructure_update
        # depending on whether we are creating a new table (Full Gen) or appending (Drift).
        pr_url = apply_infrastructure_update(
            REPO_NAME, GITHUB_TOKEN, table_ref, new_cols, trace_id, sample_data
        )

        # 5. Final Audit Log
        status = "SUCCESS" if pr_url else "SKIPPED"
        log_ai_action(
            trace_id=trace_id,
            action="CREATE_PR",
            resource=table_ref,
            status=status,
            details={"cols_processed": len(new_cols)},
            link=pr_url,
        )

    except Exception as e:
        error_msg = str(e)
        log_event("CRITICAL", f"Unhandled AI Agent Error: {error_msg}", trace_id)
        try:
            log_ai_action(trace_id, "CRASH", "System", "FAILED", error_msg, None)
        except:
            pass
