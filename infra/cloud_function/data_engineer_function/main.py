import base64
import json
import os
import uuid
import datetime
import logging
from typing import List, Dict, Any, Optional, Union

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

bq_client: Optional[bigquery.Client] = None

# Initialize Vertex AI
try:
    if PROJECT_ID:
        vertexai.init(project=PROJECT_ID, location=REGION)
        # Using the specific stable version you requested
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
        "component": "sentinel-ai-agent",
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
        "ai_agent_id": "Gemini-Autonomous-v2",
        "resource_group": "GitHub Repo",
        "resource_type": "Schema File",
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


# ==============================================================================
# NEW FEATURE: REPO INTELLIGENCE
# ==============================================================================
def get_repo_map(repo, branch_sha) -> List[str]:
    """
    Recursively lists files, prioritizing Schema/BigQuery locations.
    """
    all_files = []
    priority_files = []

    try:
        tree = repo.get_git_tree(branch_sha, recursive=True)
        for element in tree.tree:
            if element.type == "blob":
                path = element.path
                # Only care about config/code files
                if path.endswith((".json", ".tf")):
                    all_files.append(path)

                    # PRIORITY: If it looks like a BQ schema, put it at the top
                    if "bigquery" in path or "schema" in path or "tables" in path:
                        priority_files.append(path)

    except Exception as e:
        print(f"Repo scan warning: {e}")

    # Combine: Priority files first, then the rest (up to limit)
    # This ensures the AI sees "infra/bigquery/tables/json/..." first
    final_list = list(set(priority_files + all_files))
    return final_list[:300]


def ask_gemini_for_path(table_name: str, file_list: List[str], trace_id: str) -> str:
    """
    Asks AI to pick the best file path based on the existing repo structure.
    """
    if not model:
        return f"infra/bigquery/tables/json/{table_name}.json"

    prompt = f"""
    You are a DevOps Architect.
    
    Task: Determine the EXACT file path for a new BigQuery Schema JSON file.
    
    Context:
    1. Target Table: "{table_name}"
    2. Existing Repo Files (Reference these patterns): 
    {json.dumps(file_list, indent=2)} 

    Instructions:
    - FIND THE PATTERN: Look at where other JSON schema files are stored.
    - COPY THE STRUCTURE: If you see 'infra/bigquery/tables/json/some_table.json', then your output MUST be 'infra/bigquery/tables/json/{table_name}.json'.
    - DO NOT invent new folders like 'schemas/' if a 'json/' folder already exists.
    - DO NOT return double slashes like '//'.
    
    Output:
    Return ONLY the file path string.
    """

    try:
        response = model.generate_content(prompt)
        path = response.text.strip().replace("`", "").replace("json", "", 1).strip()

        # Cleanup quotes
        if (path.startswith("'") and path.endswith("'")) or (
            path.startswith('"') and path.endswith('"')
        ):
            path = path[1:-1]

        # 🛡️ FIX: Clean up double slashes (The error you saw)
        path = path.replace("//", "/")

        log_event("INFO", f"🧠 AI decided file path: {path}", trace_id)
        return path
    except Exception as e:
        log_event("ERROR", f"AI Path Decision failed: {e}", trace_id)
        # Default safe path
        return f"infra/bigquery/tables/json/{table_name}.json"


# ==============================================================================
# CORE INTELLIGENCE: GEMINI
# ==============================================================================
def ask_gemini_for_schema(
    table_name: str, new_cols: List[str], sample_data: List[Dict], trace_id: str
) -> List[Dict[str, str]]:
    """
    Uses LLM to generate descriptions and PII tags.
    """
    if not model:
        log_event("ERROR", "Vertex AI model not initialized.", trace_id)
        return [
            {
                "name": col,
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "Auto-added column",
            }
            for col in new_cols
        ]

    log_event(
        "INFO", f"🧠 Asking Gemini to analyze {len(new_cols)} columns...", trace_id
    )

    prompt = f"""
    You are a Senior Data Governance Engineer.
    
    Task: Analyze the new columns detected in a raw data ingestion file.
    Target System: Google BigQuery (Raw Layer).
    
    CRITICAL RULES:
    1. ALL columns MUST be of type 'STRING'.
    2. Generate a professional 'description' based on the column name and the provided sample data.
    3. Privacy Check: If the data looks like PII (email, phone, SSN), prefix the description with "[PII]".
    
    Input Context:
    - Table: {table_name}
    - New Columns: {new_cols}
    - Sample Data (First 5 rows): 
      {json.dumps(sample_data, indent=2)}
    
    Output Format:
    Return strictly a JSON Array of Objects.
    Example:
    [ {{"name": "email", "type": "STRING", "mode": "NULLABLE", "description": "[PII] User email"}} ]
    """

    generation_config = GenerationConfig(
        response_mime_type="application/json",
        temperature=0.1,
    )

    try:
        response = model.generate_content(prompt, generation_config=generation_config)
        text_response = response.text.strip()
        if text_response.startswith("```"):
            text_response = text_response.split("\n", 1)[1].rsplit("\n", 1)[0]

        schema_list = json.loads(text_response)

        # FINAL SAFETY CHECK
        for col in schema_list:
            col["type"] = "STRING"
            col["mode"] = "NULLABLE"

        return schema_list

    except Exception as e:
        log_event("ERROR", f"Gemini Analysis Failed: {e}", trace_id)
        return [
            {
                "name": col,
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "Auto-added column (AI Error)",
            }
            for col in new_cols
        ]


# ==============================================================================
# INFRASTRUCTURE OPS: GITHUB (AUTONOMOUS)
# ==============================================================================
def apply_schema_update(
    repo_name: str,
    token: str,
    table_ref: str,
    ai_suggestions: List[Dict],
    trace_id: str,
) -> Union[str, None]:
    """
    Autonomous function: Scans repo -> Finds path -> Creates/Updates file -> Opens PR.
    """
    dataset, table = table_ref.split(".")[-2:]

    try:
        g = Github(token)
        repo = g.get_repo(repo_name)

        # 1. Get Default Branch (Handling Main/Master)
        default_branch_name = repo.default_branch
        try:
            default_branch_obj = repo.get_branch(default_branch_name)
        except GithubException:
            log_event("CRITICAL", "Repo is empty! Initialize with a README.", trace_id)
            return None

        # 2. SCAN: Map the repo structure
        log_event("INFO", "🛰️ Scanning repository structure...", trace_id)
        repo_files = get_repo_map(repo, default_branch_obj.commit.sha)

        # 3. DECIDE: Ask AI for the target file path
        target_file_path = ask_gemini_for_path(table, repo_files, trace_id)

        # 4. Create Feature Branch
        branch_name = f"ai/autonomous-fix-{table}-{uuid.uuid4().hex[:6]}"
        repo.create_git_ref(
            ref=f"refs/heads/{branch_name}", sha=default_branch_obj.commit.sha
        )
        log_event("INFO", f"🌿 Created branch: {branch_name}", trace_id)

        # 5. Read or Create File Content
        current_schema = []
        file_sha = None
        file_exists = False

        try:
            # Check if file exists in the NEW branch
            contents = repo.get_contents(target_file_path, ref=branch_name)
            file_content = base64.b64decode(contents.content).decode("utf-8")
            current_schema = json.loads(file_content)
            file_sha = contents.sha
            file_exists = True
            log_event("INFO", f"📂 Found existing file: {target_file_path}", trace_id)
        except GithubException:
            log_event(
                "INFO",
                f"✨ File not found. Will create new: {target_file_path}",
                trace_id,
            )
            file_exists = False
            current_schema = []

        # 6. Merge Logic (Idempotency)
        existing_cols = {col["name"] for col in current_schema}
        added_cols = []

        for col_def in ai_suggestions:
            if col_def["name"] not in existing_cols:
                current_schema.append(col_def)
                added_cols.append(col_def)

        if not added_cols:
            log_event(
                "WARNING", "No new columns to add (already exist in code).", trace_id
            )
            return None

        # 7. Commit & Push
        new_content = json.dumps(current_schema, indent=2)
        commit_message = f"feat(data): AI autonomous update for {table}"

        if file_exists:
            repo.update_file(
                path=target_file_path,
                message=commit_message,
                content=new_content,
                sha=file_sha,
                branch=branch_name,
            )
        else:
            repo.create_file(
                path=target_file_path,
                message=commit_message,
                content=new_content,
                branch=branch_name,
            )

        log_event("INFO", f"💾 Changes committed to {target_file_path}", trace_id)

        # 8. Open Pull Request
        pr_body = f"""
# 🤖 Sentinel AI Auto-Fix
*Trace ID:* `{trace_id}`
*Target File:* `{target_file_path}`

### 🧠 Intelligence Report
    1. *Path Discovery:* I scanned the repo and determined `{target_file_path}` is the correct location.
    2. *Action:** {'Updated existing file' if file_exists else 'Created new file'}.
    3. *Changes:* Added *{len(added_cols)}* new columns.

### 📝 Schema Updates
```json
{json.dumps(added_cols, indent=2)}
```

### ✅ Action Required
Merge this PR to apply changes to BigQuery via Terraform.
"""

        # FIX: Pass the STRING name of the base branch
        pr = repo.create_pull(
            title=f"feat(data): Fix Schema Drift in {table}",
            body=pr_body,
            head=branch_name,
            base=default_branch_name,
        )

        log_event("INFO", f"✅ PR Created: {pr.html_url}", trace_id)
        return pr.html_url

    except Exception as e:
        log_event("ERROR", f"GitHub Operation Failed: {e}", trace_id)
        raise e


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

        # 2. Configuration Validation
        if not GITHUB_TOKEN or not REPO_NAME:
            error_msg = "Missing GITHUB_TOKEN or REPO_NAME configuration."
            log_event("CRITICAL", error_msg, trace_id)
            return

        if not new_cols:
            log_event(
                "WARNING",
                "Received drift event but no new columns specified.",
                trace_id,
            )
            return

        # 3. Ask The Brain (Gemini)
        ai_suggestions = ask_gemini_for_schema(
            table_ref, new_cols, sample_data, trace_id
        )

        # 4. Execute Infrastructure Code (Autonomous GitHub Ops)
        pr_url = apply_schema_update(
            REPO_NAME, GITHUB_TOKEN, table_ref, ai_suggestions, trace_id
        )

        # 5. Final Audit Log
        status = "SUCCESS" if pr_url else "SKIPPED"
        log_ai_action(
            trace_id=trace_id,
            action="CREATE_PR",
            resource=table_ref,
            status=status,
            details=ai_suggestions,
            link=pr_url,
        )

    except Exception as e:
        error_msg = str(e)
        log_event("CRITICAL", f"Unhandled AI Agent Error: {error_msg}", trace_id)
        try:
            log_ai_action(trace_id, "CRASH", "System", "FAILED", error_msg, None)
        except:
            pass
