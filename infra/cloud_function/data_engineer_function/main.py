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
# 1. Environment Variables (Injected by Terraform)
PROJECT_ID = os.environ.get("GCP_PROJECT")
REGION = os.environ.get("GCP_REGION", "us-central1")
REPO_NAME = os.environ.get("REPO_NAME")
# GITHUB_TOKEN is injected securely via Secret Manager as an Env Var
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
AI_AUDIT_TABLE = os.environ.get("AI_AUDIT_TABLE", "sentinel_audit.ai_ops_log")

# 2. Client Initialization (Lazy Loading Pattern)
bq_client: Optional[bigquery.Client] = None

# Initialize Vertex AI once (Global Scope)
try:
    vertexai.init(project=PROJECT_ID, location=REGION)
    # Using the latest stable Gemini Pro model
    model = GenerativeModel("gemini-1.5-pro-preview-0409")
except Exception as e:
    logging.error(f"Failed to initialize Vertex AI: {e}")
    model = None

# Configure Logging
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
    """
    Writes a permanent record of the AI's "thought process" to BigQuery.
    """
    client = get_bq_client()

    row = {
        "operation_id": uuid.uuid4().hex,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "ai_agent_id": "Gemini-Raw-Architect-v1",
        "resource_group": "Terraform",
        "resource_type": "BigQuery Schema",
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
# CORE INTELLIGENCE: GEMINI 1.5 PRO
# ==============================================================================
def ask_gemini_for_schema(
    table_name: str, new_cols: List[str], sample_data: List[Dict], trace_id: str
) -> List[Dict[str, str]]:
    """
    Uses LLM to generate descriptions and PII tags.
    STRICTLY ENFORCES 'STRING' TYPE.
    """
    if not model:
        log_event("ERROR", "Vertex AI model not initialized.", trace_id)
        # Fallback: Return basic schema without AI descriptions
        return [
            {
                "name": col,
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "Auto-added column (AI Unavailable)",
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
    1. ALL columns MUST be of type 'STRING'. Do not use INTEGER, FLOAT, or TIMESTAMP.
    2. Generate a professional 'description' based on the column name and the provided sample data.
    3. Privacy Check: If the data looks like PII (email, phone, SSN, names), prefix the description with "[PII]".
    
    Input Context:
    - Table: {table_name}
    - New Columns: {new_cols}
    - Sample Data (First 5 rows): 
      {json.dumps(sample_data, indent=2)}
    
    Output Format:
    Return strictly a JSON Array of Objects. No markdown formatting.
    Example:
    [
      {{"name": "customer_id", "type": "STRING", "mode": "NULLABLE", "description": "Unique identifier for the customer."}},
      {{"name": "email_address", "type": "STRING", "mode": "NULLABLE", "description": "[PII] Customer contact email."}}
    ]
    """

    generation_config = GenerationConfig(
        response_mime_type="application/json",
        temperature=0.1,  # Low temperature for consistent, deterministic output
        max_output_tokens=2048,
    )

    try:
        response = model.generate_content(prompt, generation_config=generation_config)
        # Clean response (sometimes models add ```json ... ```)
        text_response = response.text.strip()
        if text_response.startswith("```json"):
            text_response = text_response[7:-3]

        schema_list = json.loads(text_response)

        # FINAL SAFETY CHECK: Force types to STRING regardless of what AI said
        for col in schema_list:
            col["type"] = "STRING"
            col["mode"] = "NULLABLE"

        return schema_list

    except Exception as e:
        log_event("ERROR", f"Gemini Analysis Failed: {e}", trace_id)
        # Fallback logic
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
# INFRASTRUCTURE OPS: GITHUB
# ==============================================================================
def apply_schema_update(
    repo_name: str,
    token: str,
    table_ref: str,
    ai_suggestions: List[Dict],
    trace_id: str,
) -> Union[str, None]:
    """
    Clones repo, creates branch, updates JSON, pushes, opens PR.
    Returns: PR URL if successful, None otherwise.
    """
    dataset, table = table_ref.split(".")[-2:]
    file_path = f"terraform/schemas/{table}.json"  # Convention: schemas/table_name.json

    try:
        g = Github(token)
        repo = g.get_repo(repo_name)

        # 1. Get the repo's default branch name from GitHub settings
        base_branch_name = repo.default_branch

        # 2. Get the actual branch object
        try:
            main_branch = repo.get_branch(base_branch_name)
        except GithubException:
            log_event(
                "CRITICAL",
                f"Repo is empty! Please initialize {repo_name} with a README.",
                trace_id,
            )
            return None

        # 2. Create New Feature Branch
        branch_name = f"ai/schema-drift-{table}-{uuid.uuid4().hex[:6]}"
        repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=main_branch.commit.sha)
        log_event("INFO", f"🌿 Created branch: {branch_name}", trace_id)

        # 3. Get Existing Schema File
        try:
            contents = repo.get_contents(file_path, ref=branch_name)
            current_schema = json.loads(
                base64.b64decode(contents.content).decode("utf-8")
            )
        except GithubException:
            log_event("ERROR", f"Schema file not found: {file_path}", trace_id)
            return None

        # 4. Merge Changes (Idempotency Check)
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

        # 5. Commit & Push
        commit_message = f"feat(data): AI auto-update schema for {table}"
        repo.update_file(
            path=file_path,
            message=commit_message,
            content=json.dumps(current_schema, indent=2),
            sha=contents.sha,
            branch=branch_name,
        )
        log_event("INFO", "💾 Changes committed to GitHub.", trace_id)

        # 6. Open Pull Request
        pr_body = f"""
        ## 🤖 Sentinel AI Auto-Fix
        **Trace ID:** `{trace_id}`
        **Table:** `{table_ref}`

        ### 📝 Summary
        I detected **{len(added_cols)}** new columns in the incoming data that were missing from the Terraform definition.
        I have analyzed the data samples and generated the following schema updates:
        {json.dumps(added_cols, indent=2)}
        
        ### 🛡️ Safety Check
        - **Data Type:** Enforced as `STRING` (Raw Layer Standard).
        - **PII Detection:** Checked. See descriptions for `[PII]` tags.

        ### ✅ Action Required
        1. Review the descriptions.
        2. Merge this PR.
        3. Terraform Cloud will apply the changes to BigQuery.
        """
        # Create the PR
        pr = repo.create_pull(
            title=f"feat(data): Fix Schema Drift in {table}",
            body=pr_body,
            head=branch_name,
            base="main",  # Ensure this matches your repo's default branch
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
        # The message comes in as a base64 encoded bytestring
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
            log_ai_action(trace_id, "ABORT", table_ref, "FAILED", error_msg, None)
            return

        if not new_cols:
            log_event(
                "WARNING",
                "Received drift event but no new columns specified.",
                trace_id,
            )
            return

        # 3. Ask The Brain (Gemini)
        # Returns list of dicts: [{'name': 'x', 'type': 'STRING', ...}]
        ai_suggestions = ask_gemini_for_schema(
            table_ref, new_cols, sample_data, trace_id
        )

        # 4. Execute Infrastructure Code (Terraform via GitHub)
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
        # Catch-all to prevent function crash loops, but log heavily
        error_msg = str(e)
        log_event("CRITICAL", f"Unhandled AI Agent Error: {error_msg}", trace_id)
        # Attempt to log the failure to BigQuery if possible
        try:
            log_ai_action(trace_id, "CRASH", "System", "FAILED", error_msg, None)
        except:
            pass
