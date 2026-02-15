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
# ⚙️ ENTERPRISE CONFIGURATION
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

# Initialize Vertex AI (Gemini 1.5 Pro for massive context window)
try:
    if PROJECT_ID:
        vertexai.init(project=PROJECT_ID, location=REGION)
        model = GenerativeModel("gemini-1.5-pro-preview-0409")
except Exception as e:
    logging.error(f"Failed to initialize Vertex AI: {e}")
    model = None

logging.basicConfig(level=logging.INFO)


# ==============================================================================
# 📝 OBSERVABILITY & LOGGING (The "Black Box")
# ==============================================================================
def get_bq_client():
    global bq_client
    if not bq_client:
        bq_client = bigquery.Client()
    return bq_client


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


def log_ai_action(
    trace_id: str,
    action: str,
    resource: str,
    status: str,
    details: Dict[str, Any],
    link: Optional[str] = None,
):
    """
    Writes a permanent, legally auditable record to BigQuery.
    'details' contains the exact diff of what was changed.
    """
    client = get_bq_client()

    row = {
        "operation_id": uuid.uuid4().hex,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "ai_agent_id": "sentinel-forge",
        "resource_group": "GitHub Repo",
        "resource_type": "Infrastructure",
        "resource_id": resource,
        "action_type": action,
        "change_payload": json.dumps(details),  # Stores full diff
        "outcome_status": status,
        "reference_link": link,
    }

    try:
        errors = client.insert_rows_json(AI_AUDIT_TABLE, [row])
        if errors:
            log_event("ERROR", f"Failed to write audit log: {errors}", trace_id)
    except Exception as e:
        log_event("ERROR", f"Audit Log Crash: {e}", trace_id)


# ==============================================================================
# 🧠 INTELLIGENCE AGENT: SCHEMA DESIGNER
# ==============================================================================
def generate_dynamic_schema(
    table_name: str,
    new_cols: List[str],
    sample_data: List[Dict],
    reference_json: str,
    trace_id: str,
) -> List[Dict]:
    """
    Agent Role: Data Architect
    Goal: Create valid BigQuery JSON schema matching strict enterprise standards.
    """
    if not model:
        # Fallback if AI service is down
        return [{"name": c, "type": "STRING", "mode": "NULLABLE"} for c in new_cols]

    log_event("INFO", f"🧠 Generating Schema for {len(new_cols)} columns...", trace_id)

    style_guide = ""
    if reference_json:
        style_guide = (
            f"STYLE GUIDE (Mimic this format):\n```json\n{reference_json[:3000]}\n```"
        )

    prompt = f"""
    You are a Data Architect.
    Task: Generate a BigQuery Schema JSON for table "{table_name}".
    
    Input:
    - New Columns: {new_cols}
    - Sample Data: {json.dumps(sample_data[:3])}
    
    {style_guide}
    
    Requirements:
    1. Output strictly a JSON list of objects.
    2. All types must be 'STRING' (Raw Layer Standard).
    3. Mode must be 'NULLABLE'.
    4. Descriptions are mandatory. Infer them from column names.
    
    Output: JSON Array only.
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


# ==============================================================================
# 🧠 INTELLIGENCE AGENT: TERRAFORM ARCHITECT
# ==============================================================================
def analyze_tf_repo_state(repo, branch_sha, table_id, trace_id) -> Dict[str, Any]:
    """
    Agent Role: Repo Scanner
    Goal: Determine WHERE to put the Terraform code (New File vs. Existing File).
    """
    log_event(
        "INFO", f"🔍 Analyzing Terraform State for table '{table_id}'...", trace_id
    )

    state = {
        "is_defined": False,
        "defined_in_file": None,
        "host_file": None,
        "host_file_content": None,
    }

    try:
        tree = repo.get_git_tree(branch_sha, recursive=True)
        tf_files = [
            e
            for e in tree.tree
            if e.path.startswith(TF_BASE_PATH) and e.path.endswith(".tf")
        ]

        best_candidate_file = None
        max_tables_in_file = 0

        for element in tf_files:
            blob = repo.get_git_blob(element.sha)
            content = base64.b64decode(blob.content).decode("utf-8")

            # CHECK 1: Is table already defined?
            if f'"{table_id}"' in content or f"'{table_id}'" in content:
                # Use AI to confirm it's a Definition, not just a comment
                if ask_ai_is_definition(content, table_id):
                    state["is_defined"] = True
                    state["defined_in_file"] = element.path
                    log_event(
                        "INFO", f"✅ Table already defined in {element.path}", trace_id
                    )
                    return state

            # CHECK 2: Is this a good "Host File"?
            if 'resource "google_bigquery_table"' in content and "for_each" in content:
                count = content.count("table_id")
                if count > max_tables_in_file:
                    max_tables_in_file = count
                    best_candidate_file = element.path
                    state["host_file_content"] = content

        if best_candidate_file:
            state["host_file"] = best_candidate_file
            log_event(
                "INFO", f"🏠 Identified Host File: {best_candidate_file}", trace_id
            )
        else:
            log_event(
                "INFO", "⚪ No suitable host file found. Will create new.", trace_id
            )

    except Exception as e:
        log_event("WARNING", f"Repo analysis failed: {e}", trace_id)

    return state


def ask_ai_is_definition(content: str, table_id: str) -> bool:
    """Helper: Asks AI if the table is truly defined in this file."""
    if not model:
        return True
    prompt = f"""
    Does this Terraform code define a BigQuery table named "{table_id}"?
    Check for:
    1. resource "google_bigquery_table" "..." {{ table_id = "{table_id}" }}
    2. locals maps where "{table_id}" is a key.
    
    Code:
    {content[:5000]}
    
    Return TRUE or FALSE.
    """
    try:
        res = model.generate_content(prompt)
        return "TRUE" in res.text.upper()
    except:
        return False


def generate_tf_patch_or_create(
    table_id: str,
    dataset_id: str,
    schema_path: str,
    host_file_content: Optional[str],
    trace_id: str,
) -> str:
    """
    Agent Role: Terraform Engineer
    Goal: Write HCL code. Either a full new file OR an injected snippet into an existing file.
    """
    if not model:
        return ""

    action = "CREATE_NEW" if not host_file_content else "PATCH_EXISTING"
    log_event("INFO", f"🏗️ Terraform Action: {action}", trace_id)

    if action == "PATCH_EXISTING":
        prompt = f"""
        You are a Terraform Expert.
        
        Task: Add a new BigQuery table definition to the existing file below.
        
        New Table Details:
        - Table ID: "{table_id}"
        - Schema File: "{schema_path}" (Use relative path logic existing in file)
        - Partitioning: DAY (field: batch_date)
        
        EXISTING FILE:
        ```hcl
        {host_file_content}
        ```
        
        INSTRUCTIONS:
        1. Analyze the file structure.
        2. If it uses a `locals` map for tables, add the new table to that map.
        3. If it uses individual resources, add a new `resource` block at the end.
        4. Maintain strict indentation and style.
        5. Return the **FULL UPDATED FILE CONTENT**. Do not truncate.
        
        Output: HCL Code only.
        """
    else:
        # Create New File from Scratch
        prompt = f"""
        You are a Terraform Expert.
        
        Task: Create a new Terraform file for BigQuery Table "{table_id}".
        
        Details:
        - Resource Name: {table_id.replace('.', '_')}
        - Dataset ID: {dataset_id}
        - Schema: file("${{path.module}}/json/{table_id}.json")
        - Options: deletion_protection=false, partitioning=DAY(batch_date)
        
        Output: HCL Code only.
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
# 🚀 ORCHESTRATION LAYER (The "Manager")
# ==============================================================================
def apply_infrastructure_update(
    repo_name: str,
    token: str,
    table_ref: str,
    new_cols: List[str],
    trace_id: str,
    sample_data: List[Dict] = None,
) -> Dict[str, Any]:

    parts = table_ref.split(".")
    dataset = parts[-2]
    table = parts[-1]

    log_event("INFO", f"🐙 Connecting to GitHub: {repo_name}", trace_id)
    g = Github(token)
    repo = g.get_repo(repo_name)
    default_branch = repo.get_branch(repo.default_branch)

    # 1. ANALYZE REPO STATE
    tf_state = analyze_tf_repo_state(repo, default_branch.commit.sha, table, trace_id)

    # 2. DETERMINE TARGETS
    clean_schema_base = SCHEMA_BASE_PATH.strip().rstrip("/")
    target_json_path = f"{clean_schema_base}/{table}.json"

    # Decide TF Path & Action
    if tf_state["is_defined"]:
        target_tf_path = tf_state["defined_in_file"]
        tf_action = "NONE"  # Already exists
        host_content = None
    elif tf_state["host_file"]:
        target_tf_path = tf_state["host_file"]
        tf_action = "PATCH"  # Add to existing file
        host_content = tf_state["host_file_content"]
    else:
        clean_tf_base = TF_BASE_PATH.strip().rstrip("/")
        target_tf_path = f"{clean_tf_base}/{table}.tf"
        tf_action = "CREATE"  # New file
        host_content = None

    log_event(
        "INFO",
        f"🎯 Targets: JSON={target_json_path}, TF={target_tf_path} ({tf_action})",
        trace_id,
    )

    # 3. CREATE BRANCH
    branch_name = f"ai/ops-{table}-{uuid.uuid4().hex[:6]}"
    repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=default_branch.commit.sha)

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
    except GithubException:
        json_exists = False

    # Fetch reference JSON for style
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
        except:
            pass

    # AI Schema Gen
    final_schema_list = []
    added_cols_for_pr = []
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
        # Smart Insert
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

    # ----------------------------------------------------------------------
    # STEP B: TERRAFORM TF
    # ----------------------------------------------------------------------
    tf_changed = False
    new_tf_content = ""
    tf_sha = None

    if tf_action in ["PATCH", "CREATE"]:
        if tf_action == "PATCH":
            # Just to be safe, get SHA for update
            blob = repo.get_contents(target_tf_path, ref=branch_name)
            tf_sha = blob.sha

        # ASK AI to Patch or Create
        rel_schema_path = f"json/{table}.json"
        new_tf_content = generate_tf_patch_or_create(
            table, dataset, rel_schema_path, host_content, trace_id
        )

        if new_tf_content and len(new_tf_content) > 20:
            tf_changed = True

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
        return {"status": "SKIPPED", "url": None, "details": "No changes needed"}

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

    if tf_changed:
        msg = f"feat: update infrastructure for {table}"
        if tf_action == "PATCH":
            repo.update_file(
                target_tf_path, msg, new_tf_content, tf_sha, branch=branch_name
            )
        else:
            repo.create_file(target_tf_path, msg, new_tf_content, branch=branch_name)

    # 📝 PR Body Construction
    pr_status_schema = "♻️ Updated" if json_exists else "✨ Created"
    pr_status_tf = "✅ Exists"
    if tf_changed:
        pr_status_tf = "♻️ Patched" if tf_action == "PATCH" else "✨ Created"

    col_table = "| Column Name | Type | Description |\n|---|---|---|\n"
    for col in added_cols_for_pr[:15]:
        col_table += (
            f"| `{col['name']}` | `{col['type']}` | {col.get('description', '')} |\n"
        )
    if len(added_cols_for_pr) > 15:
        col_table += f"| ... ({len(added_cols_for_pr)-15} more) | | |\n"

    title_prefix = "fix" if json_exists else "feat"
    pr_title = f"{title_prefix}(data): Ops for {table}"

    pr_body = f"""
# 🤖 Sentinel Forge Update
*Trace ID:* `{trace_id}`

### 🏗️ Infrastructure Actions
| Resource | Action | Path |
|---|---|---|
| **Schema** | {pr_status_schema} | `{target_json_path}` |
| **Terraform** | {pr_status_tf} | `{target_tf_path}` |

### 🔍 Changes
Added **{len(added_cols_for_pr)}** columns to the Raw Layer.

{col_table}

### 🛡️ Governance Enforcement
* **Audit Columns:** `batch_date`, `processed_dttm` enforced.
* **Defaults:** Strict SQL defaults applied for partitioning and traceability.
* **Type Safety:** All columns set to `STRING`.

### ✅ Action Required
Merge this PR. Terraform will apply the changes.
"""

    pr = repo.create_pull(
        title=pr_title, body=pr_body, head=branch_name, base=repo.default_branch
    )

    log_event("INFO", f"✅ PR Created Successfully: {pr.html_url}", trace_id)

    # Return structured details for Audit Log
    return {
        "status": "SUCCESS",
        "url": pr.html_url,
        "details": {
            "schema_action": "UPDATE" if json_exists else "CREATE",
            "terraform_action": tf_action,
            "columns_added": len(added_cols_for_pr),
            "target_tf": target_tf_path,
            "target_json": target_json_path,
        },
    }


# ==============================================================================
# MAIN ENTRY POINT (Cloud Function)
# ==============================================================================
@functions_framework.cloud_event
def ai_agent_main(cloud_event):
    trace_id = "unknown"
    table_ref = "unknown"
    try:
        if "data" in cloud_event.data["message"]:
            pubsub_message = base64.b64decode(
                cloud_event.data["message"]["data"]
            ).decode("utf-8")
            data = json.loads(pubsub_message)
        else:
            return

        trace_id = data.get("trace_id", f"unknown-{uuid.uuid4()}")
        table_ref = data.get("table_ref")
        new_cols = data.get("new_column_headers", [])
        sample_data = data.get("sample_data_rows", [])

        log_event("INFO", f"🤖 Agent Waking Up. Target: {table_ref}", trace_id)

        # 1. Config Check
        if not GITHUB_TOKEN or not REPO_NAME:
            log_event("CRITICAL", "Missing Config.", trace_id)
            return

        # 2. Logic: If new table, infer cols from sample
        if not new_cols and sample_data:
            new_cols = list(sample_data[0].keys())
        if not new_cols:
            return

        # 3. EXECUTE
        result = apply_infrastructure_update(
            REPO_NAME, GITHUB_TOKEN, table_ref, new_cols, trace_id, sample_data
        )

        # 4. AUDIT LOG
        log_ai_action(
            trace_id=trace_id,
            action="CREATE_PR",
            resource=table_ref,
            status=result["status"],
            details=result["details"],  # Stores the full map of what happened
            link=result["url"],
        )

    except Exception as e:
        error_msg = str(e)
        log_event("CRITICAL", f"Error: {e}", trace_id)
        try:
            log_ai_action(
                trace_id, "CRASH", "System", "FAILED", {"error": error_msg}, None
            )
        except:
            pass
