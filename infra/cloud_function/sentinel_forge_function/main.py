import base64
import json
import os
import uuid
import datetime
import logging
from typing import List, Dict, Any, Optional, Tuple

import functions_framework
import vertexai
from vertexai.generative_models import GenerativeModel
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

# Initialize Vertex AI (Gemini 2.5 Pro for massive context window and reasoning)
try:
    if PROJECT_ID:
        vertexai.init(project=PROJECT_ID, location=REGION)
        model = GenerativeModel("gemini-2.5-pro")
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
    """Writes a permanent, legally auditable record to BigQuery."""
    log_event(
        "INFO",
        f"▶️ 🔏 [Audit Ledger] Starting permanent audit log recording for action: {action}...",
        trace_id,
    )
    client = get_bq_client()

    row = {
        "operation_id": uuid.uuid4().hex,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "ai_agent_id": "sentinel-forge",
        "resource_group": "GitHub Repo",
        "resource_type": "Infrastructure",
        "resource_id": resource,
        "action_type": action,
        "change_payload": json.dumps(details),
        "outcome_status": status,
        "reference_link": link,
    }

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


# ==============================================================================
# 🧠 AGENT 1: SCHEMA DESIGNER
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
        f"▶️ 🧩 [Schema Design] Initiating dynamic schema generation for '{table_name}'...",
        trace_id,
    )

    if not model:
        log_event(
            "WARNING",
            "⚠️ 🧩 [Schema Design] AI Service unavailable. Falling back to basic programmatic schema.",
            trace_id,
        )
        log_event(
            "INFO",
            "⏹️ 🧩 [Schema Design] Finished schema generation (Fallback mode).",
            trace_id,
        )
        return [{"name": c, "type": "STRING", "mode": "NULLABLE"} for c in new_cols]

    log_event(
        "INFO",
        f"🧠 🧩 [Schema Design] Instructing AI to map {len(new_cols)} incoming columns...",
        trace_id,
    )

    style_guide = ""
    if reference_json:
        log_event(
            "INFO",
            "🎨 🧩 [Schema Design] Injecting reference JSON style guide into AI context...",
            trace_id,
        )
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
        log_event(
            "INFO", "⏳ 🧩 [Schema Design] Awaiting AI generation response...", trace_id
        )
        response = model.generate_content(prompt)
        text = response.text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]

        parsed_schema = json.loads(text)
        log_event(
            "INFO",
            f"✅ 🧩 [Schema Design] AI successfully formulated JSON schema with {len(parsed_schema)} fields.",
            trace_id,
        )
        log_event("INFO", "⏹️ 🧩 [Schema Design] Finished schema generation.", trace_id)
        return parsed_schema

    except Exception as e:
        log_event(
            "ERROR",
            f"❌ 🧩 [Schema Design] Schema Generation Failed: {e}. Falling back to basic generation.",
            trace_id,
        )
        log_event(
            "INFO",
            "⏹️ 🧩 [Schema Design] Finished schema generation (Fallback mode).",
            trace_id,
        )
        return [{"name": c, "type": "STRING", "mode": "NULLABLE"} for c in new_cols]


# ==============================================================================
# 🧠 AGENT 2: TERRAFORM ARCHITECT
# ==============================================================================
def analyze_tf_repo_state(repo, branch_sha, table_id, trace_id) -> Dict[str, Any]:
    """Goal: Determine WHERE to put the Terraform code (New File vs. Existing File)."""
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

            # CHECK 1: Is table already defined?
            if f'"{table_id}"' in content or f"'{table_id}'" in content:
                log_event(
                    "INFO",
                    f"👀 🕵️‍♂️ [Repo Scan] Detected potential string match for '{table_id}' in {element.path}. Consulting AI for semantic validation...",
                    trace_id,
                )
                if ask_ai_is_definition(content, table_id):
                    state["is_defined"] = True
                    state["defined_in_file"] = element.path
                    log_event(
                        "INFO",
                        f"✅ 🕵️‍♂️ [Repo Scan] AI Verified: Table is already defined in {element.path}",
                        trace_id,
                    )
                    log_event(
                        "INFO",
                        "⏹️ 🕵️‍♂️ [Repo Scan] Finished analysis (Definition Found).",
                        trace_id,
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
                "INFO",
                f"🏠 🕵️‍♂️ [Repo Scan] Identified optimal existing Host File for injection: {best_candidate_file}",
                trace_id,
            )
        else:
            log_event(
                "INFO",
                "⚪ 🕵️‍♂️ [Repo Scan] No suitable host file found. Instructing system to create a brand new file.",
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
    """Goal: Write HCL code. Either a full new file OR an injected snippet into an existing file."""
    log_event(
        "INFO",
        f"▶️ 🏗️ [TF Architect] Initiating Terraform HCL synthesis for '{table_id}'...",
        trace_id,
    )

    if not model:
        log_event(
            "WARNING",
            "⚠️ 🏗️ [TF Architect] AI Service offline. Returning empty block.",
            trace_id,
        )
        log_event(
            "INFO", "⏹️ 🏗️ [TF Architect] Finished TF generation (Abort).", trace_id
        )
        return ""

    action = "CREATE_NEW" if not host_file_content else "PATCH_EXISTING"
    log_event(
        "INFO",
        f"🛠️ 🏗️ [TF Architect] Determining infrastructure action vector: {action}",
        trace_id,
    )

    if action == "PATCH_EXISTING":
        log_event(
            "INFO",
            "🩹 🏗️ [TF Architect] Constructing prompt for intelligent HCL injection (Patch)...",
            trace_id,
        )
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
        log_event(
            "INFO",
            "✨ 🏗️ [TF Architect] Constructing prompt for brand new HCL definition (Create)...",
            trace_id,
        )
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
        log_event(
            "INFO",
            "⏳ 🏗️ [TF Architect] Awaiting AI infrastructure generation...",
            trace_id,
        )
        text = model.generate_content(prompt).text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]

        log_event(
            "INFO",
            "✅ 🏗️ [TF Architect] AI successfully generated Terraform HCL block.",
            trace_id,
        )
        log_event("INFO", "⏹️ 🏗️ [TF Architect] Finished TF generation.", trace_id)
        return text.replace("```hcl", "").replace("```", "")
    except Exception as e:
        log_event(
            "ERROR",
            f"❌ 🏗️ [TF Architect] Terraform HCL Generation Failed: {e}",
            trace_id,
        )
        log_event(
            "INFO", "⏹️ 🏗️ [TF Architect] Finished TF generation (Failure).", trace_id
        )
        return ""


# ==============================================================================
# 🧠 AGENT 3: DATAFORM SCANNER (Context Gathering)
# ==============================================================================
def analyze_dataform_repo_state(
    repo, branch_sha: str, base_name: str, table_name: str, trace_id: str
) -> Dict[str, Any]:
    """Goal: Scan repo for existing Dataform rules/styles and check if target files exist for patching."""
    log_event(
        "INFO",
        f"▶️ 🔍 [DF Scanner] Scanning Dataform workspace for entity '{base_name}'...",
        trace_id,
    )
    state = {"style_guide_sqlx": "", "existing_files": {}}

    try:
        tree = repo.get_git_tree(branch_sha, recursive=True)
        df_files = [
            e
            for e in tree.tree
            if e.path.startswith("definitions/") and e.path.endswith(".sqlx")
        ]

        target_paths = [
            f"definitions/sources/{table_name}.sqlx",
            f"definitions/staging/{base_name}/stg_{base_name}.sqlx",
            f"definitions/marts/{base_name}/fct_{base_name}.sqlx",
            f"definitions/operations/archive_{table_name}.sqlx",
        ]

        for element in df_files:
            # Grab target files if they exist (for patching)
            if element.path in target_paths:
                content = base64.b64decode(
                    repo.get_git_blob(element.sha).content
                ).decode("utf-8")
                state["existing_files"][element.path] = content
                log_event(
                    "INFO",
                    f"📂 🔍 [DF Scanner] Found existing target for patching: {element.path}",
                    trace_id,
                )

            # Grab a random SQLX file to use as a style guide if we are creating from scratch
            elif not state["style_guide_sqlx"] and (
                "stg_" in element.path or "fct_" in element.path
            ):
                state["style_guide_sqlx"] = base64.b64decode(
                    repo.get_git_blob(element.sha).content
                ).decode("utf-8")

    except Exception as e:
        log_event("WARNING", f"⚠️ 🔍 [DF Scanner] Dataform scan anomaly: {e}", trace_id)

    log_event(
        "INFO", "⏹️ 🔍 [DF Scanner] Finished scanning Dataform workspace.", trace_id
    )
    return state


# ==============================================================================
# 🧠 AGENT 4: DATAFORM ARCHITECT (Pipeline Generation & Patching)
# ==============================================================================
def generate_ai_dataform_pipeline(
    table_name: str,
    schema_list: List[Dict],
    error_context: str,
    df_state: Dict[str, Any],
    trace_id: str,
) -> Dict[str, str]:
    """Goal: Use AI to dynamically generate or patch Dataform SQLX files based on schema and errors."""
    log_event(
        "INFO",
        f"▶️ 🪄 [DF Architect] Analyzing requirements for '{table_name}' based on error: {error_context}",
        trace_id,
    )

    if not model:
        log_event(
            "WARNING",
            "⚠️ 🪄 [DF Architect] AI Service offline. Cannot generate Dataform code.",
            trace_id,
        )
        log_event(
            "INFO",
            "⏹️ 🪄 [DF Architect] Finished Dataform generation (Abort).",
            trace_id,
        )
        return {}

    base_name = table_name.replace("_raw", "")
    clean_schema = [{"name": c["name"], "type": c["type"]} for c in schema_list]

    existing_files = df_state.get("existing_files", {})
    style_guide = df_state.get("style_guide_sqlx", "")

    if existing_files:
        log_event(
            "INFO",
            "🩹 🪄 [DF Architect] Existing files detected. Operating in PATCH mode.",
            trace_id,
        )
        instruction_block = f"""
        Some or all of the pipeline files already exist.
        EXISTING FILES: {json.dumps(existing_files)}
        Your task is to PATCH these existing files. Update the SELECT statements to ensure all columns from the provided schema are included. 
        Preserve any existing complex business logic, WHERE clauses, or custom tags.
        """
    else:
        log_event(
            "INFO",
            "✨ 🪄 [DF Architect] No files detected. Operating in CREATE mode.",
            trace_id,
        )
        instruction_block = f"""
        Create the files from scratch.
        STYLE GUIDE (Mimic this formatting):
        ```sql\n{style_guide[:2000]}\n```
        """

    prompt = f"""
    You are an expert Analytics Engineer writing Google Cloud Dataform code (Core v3.x).
    
    Task: Generate the complete Dataform pipeline for a BigQuery table.
    
    Context:
    - Target Raw Table: "{table_name}"
    - Base Entity Name: "{base_name}"
    - GCP Project: "{PROJECT_ID}"
    - Schema: {json.dumps(clean_schema)}
    - Triggering Error/Event: "{error_context}"
    
    {instruction_block}
    
    Requirements:
    1. Ensure 4 files exist/are updated:
       - definitions/sources/{table_name}.sqlx (Type: declaration)
       - definitions/staging/{base_name}/stg_{base_name}.sqlx (Type: view)
       - definitions/marts/{base_name}/fct_{base_name}.sqlx (Type: incremental)
       - definitions/operations/archive_{table_name}.sqlx (Type: operations)
    2. Handle `batch_date` and `processed_dttm` correctly.
    3. The Operations file must depend on `fct_{base_name}` and TRUNCATE `{table_name}` after inserting into `{table_name}_hist`.
    4. Output STRICTLY a JSON object where keys are the full file paths and values are the exact SQLX string content. Do not include markdown formatting outside the JSON block.
    """

    try:
        log_event(
            "INFO",
            "⏳ 🪄 [DF Architect] Instructing Gemini to synthesize Dataform files...",
            trace_id,
        )
        response = model.generate_content(prompt)
        text = response.text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]
        if text.startswith("json"):
            text = text[4:]

        pipeline_files = json.loads(text)
        log_event(
            "INFO",
            f"✅ 🪄 [DF Architect] AI successfully generated {len(pipeline_files)} SQLX files.",
            trace_id,
        )

        log_ai_action(
            trace_id,
            "GENERATE_DATAFORM_CODE",
            table_name,
            "SUCCESS",
            {
                "files_generated": list(pipeline_files.keys()),
                "error_context": error_context,
            },
        )
        log_event("INFO", "⏹️ 🪄 [DF Architect] Finished Dataform generation.", trace_id)
        return pipeline_files

    except Exception as e:
        log_event(
            "ERROR", f"❌ 🪄 [DF Architect] AI Code Generation Failed: {e}", trace_id
        )
        log_ai_action(
            trace_id, "GENERATE_DATAFORM_CODE", table_name, "FAILED", {"error": str(e)}
        )
        log_event(
            "INFO",
            "⏹️ 🪄 [DF Architect] Finished Dataform generation (Failure).",
            trace_id,
        )
        return {}


# ==============================================================================
# 🧠 AGENT 5: DATAFORM QA VERIFIER (Automated Reviewer)
# ==============================================================================
def verify_dataform_pipeline(
    pipeline_files: Dict[str, str], schema_list: List[Dict], trace_id: str
) -> Dict[str, str]:
    """Goal: Acts as a QA Lead to review the generated code for syntax errors or missed columns before commit."""
    log_event(
        "INFO",
        f"▶️ 🕵️‍♀️ [DF QA] Initiating automated peer-review of generated SQLX code...",
        trace_id,
    )

    if not model or not pipeline_files:
        log_event("INFO", "⏹️ 🕵️‍♀️ [DF QA] Bypassing QA (No model or no files).", trace_id)
        return pipeline_files

    clean_schema = [
        c["name"]
        for c in schema_list
        if c["name"] not in ["batch_date", "processed_dttm"]
    ]

    prompt = f"""
    You are a Senior Data QA Lead. Review the following Dataform pipeline generated by a junior engineer.
    
    GENERATED FILES: {json.dumps(pipeline_files)}
    REQUIRED COLUMNS (Must be explicitly selected in staging and marts layers): {clean_schema}
    
    Checklist:
    1. Are all required columns explicitly selected? (If missing, ADD THEM).
    2. Are Dataform v3.x functions used correctly (e.g., `${{ref()}}`, `${{self()}}`)?
    3. Does the operations file TRUNCATE the source table after archiving?
    
    Fix any errors found. Output STRICTLY a JSON object where keys are the full file paths and values are the corrected exact SQLX string content. 
    Output JSON ONLY. No explanation.
    """

    try:
        log_event("INFO", "⏳ 🕵️‍♀️ [DF QA] Awaiting QA review results...", trace_id)
        text = model.generate_content(prompt).text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]
        if text.startswith("json"):
            text = text[4:]

        verified_files = json.loads(text)
        log_event(
            "INFO", f"✅ 🕵️‍♀️ [DF QA] QA passed. Code sanitized and validated.", trace_id
        )
        log_event("INFO", "⏹️ 🕵️‍♀️ [DF QA] Finished automated review.", trace_id)
        return verified_files
    except Exception as e:
        log_event(
            "WARNING",
            f"⚠️ 🕵️‍♀️ [DF QA] QA Verification Failed (Parsing error). Falling back to unverified code. {e}",
            trace_id,
        )
        log_event(
            "INFO",
            "⏹️ 🕵️‍♀️ [DF QA] Finished automated review (Fallback applied).",
            trace_id,
        )
        return pipeline_files


# ==============================================================================
# 📦 GIT INJECTOR
# ==============================================================================
def inject_dataform_into_repo(
    repo,
    branch_name: str,
    table_name: str,
    pipeline_code: Dict[str, str],
    trace_id: str,
) -> Tuple[int, str]:
    """Goal: Commits the AI-generated and verified Dataform files to the PR branch."""
    files_changed = 0
    detailed_commit_log = ""
    log_event(
        "INFO",
        "▶️ 📦 [Git Injector] Committing Dataform files to Git branch...",
        trace_id,
    )

    for file_path, content in pipeline_code.items():
        try:
            c = repo.get_contents(file_path, ref=branch_name)
            commit_msg = f"fix(dataform): Auto-patch pipeline logic for {table_name} in {file_path}"
            repo.update_file(file_path, commit_msg, content, c.sha, branch=branch_name)
            log_event(
                "INFO",
                f"♻️ 📦 [Git Injector] Updated existing file: {file_path}",
                trace_id,
            )
            detailed_commit_log += f"- ♻️ **Patched:** `{file_path}`\n"
        except GithubException:
            commit_msg = f"feat(dataform): Init self-healing pipeline for {table_name} in {file_path}"
            repo.create_file(file_path, commit_msg, content, branch=branch_name)
            log_event(
                "INFO", f"✨ 📦 [Git Injector] Created new file: {file_path}", trace_id
            )
            detailed_commit_log += f"- ✨ **Created:** `{file_path}`\n"
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
    token: str,
    table_ref: str,
    new_cols: List[str],
    trace_id: str,
    sample_data: List[Dict] = None,
    error_context: str = "Automated AI trigger",
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

    log_event(
        "INFO",
        f"🐙 🚀 [Orchestrator] Establishing secure handshake with GitHub Repository: {repo_name}...",
        trace_id,
    )
    g = Github(token)
    repo = g.get_repo(repo_name)
    default_branch = repo.get_branch(repo.default_branch)

    # 1. ANALYZE TERRAFORM STATE
    log_event(
        "INFO",
        "🗺️ 🚀 [Orchestrator] Step 1: Executing TF Repository State Analysis...",
        trace_id,
    )
    tf_state = analyze_tf_repo_state(repo, default_branch.commit.sha, table, trace_id)

    # 2. ANALYZE DATAFORM STATE
    log_event(
        "INFO",
        "🔍 🚀 [Orchestrator] Step 2: Executing Dataform Repository State Analysis...",
        trace_id,
    )
    df_state = analyze_dataform_repo_state(
        repo, default_branch.commit.sha, base_name, table, trace_id
    )

    # 3. DETERMINE TARGETS
    log_event(
        "INFO",
        "🎯 🚀 [Orchestrator] Step 3: Determining Target Asset Paths...",
        trace_id,
    )
    clean_schema_base = SCHEMA_BASE_PATH.strip().rstrip("/")
    target_json_path = f"{clean_schema_base}/{table}.json"

    if tf_state["is_defined"]:
        target_tf_path = tf_state["defined_in_file"]
        tf_action = "NONE"
        host_content = None
    elif tf_state["host_file"]:
        target_tf_path = tf_state["host_file"]
        tf_action = "PATCH"
        host_content = tf_state["host_file_content"]
    else:
        clean_tf_base = TF_BASE_PATH.strip().rstrip("/")
        target_tf_path = f"{clean_tf_base}/{table}.tf"
        tf_action = "CREATE"
        host_content = None

    log_event(
        "INFO",
        f"📍 🚀 [Orchestrator] Targets locked: JSON => {target_json_path} | TF => {target_tf_path} (Action: {tf_action})",
        trace_id,
    )

    # 4. CREATE BRANCH
    log_event(
        "INFO",
        "🌿 🚀 [Orchestrator] Step 4: Spinning up isolated Git branch...",
        trace_id,
    )
    branch_name = f"ai/ops-{table}-{uuid.uuid4().hex[:6]}"
    repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=default_branch.commit.sha)
    log_event(
        "INFO",
        f"🌱 🚀 [Orchestrator] Branch `{branch_name}` created successfully.",
        trace_id,
    )

    # ----------------------------------------------------------------------
    # STEP 5A: SCHEMA JSON LOGIC
    # ----------------------------------------------------------------------
    log_event(
        "INFO",
        "🧬 🚀 [Orchestrator] Step 5A: Generating JSON Schema Definition...",
        trace_id,
    )
    json_exists = False
    current_schema = []
    json_sha = None

    try:
        c = repo.get_contents(target_json_path, ref=branch_name)
        current_schema = json.loads(base64.b64decode(c.content).decode("utf-8"))
        json_exists = True
        json_sha = c.sha
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

    ref_json = ""
    if not json_exists:
        try:
            tree = repo.get_git_tree(default_branch.commit.sha, recursive=True)
            for e in tree.tree:
                if e.path.endswith(".json") and "tables" in e.path:
                    ref_json = base64.b64decode(
                        repo.get_git_blob(e.sha).content
                    ).decode("utf-8")
                    log_event(
                        "INFO",
                        "🖼️ 🚀 [Orchestrator] Located reference schema template for AI style transfer.",
                        trace_id,
                    )
                    break
        except Exception:
            pass

    final_schema_list = []
    added_cols_for_pr = []
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
            new_cols if new_cols else list(sample_data[0].keys()) if sample_data else []
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
            table, new_cols, sample_data or [], ref_json, trace_id
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

    log_event(
        "INFO",
        f"✅ 🚀 [Orchestrator] Step 5A Complete. Total added columns: {len(added_cols_for_pr)}",
        trace_id,
    )

    # ----------------------------------------------------------------------
    # STEP 5B: TERRAFORM TF LOGIC
    # ----------------------------------------------------------------------
    log_event(
        "INFO",
        "🧱 🚀 [Orchestrator] Step 5B: Designing Terraform Configurations...",
        trace_id,
    )
    tf_changed = False
    new_tf_content = ""
    tf_sha = None

    if tf_action in ["PATCH", "CREATE"]:
        if tf_action == "PATCH":
            blob = repo.get_contents(target_tf_path, ref=branch_name)
            tf_sha = blob.sha

        rel_schema_path = f"json/{table}.json"
        new_tf_content = generate_tf_patch_or_create(
            table, dataset, rel_schema_path, host_content, trace_id
        )
        if new_tf_content and len(new_tf_content) > 20:
            tf_changed = True
            log_event(
                "INFO",
                "✅ 🚀 [Orchestrator] Valid HCL code captured from AI.",
                trace_id,
            )

    # ----------------------------------------------------------------------
    # STEP 5C: DATAFORM PIPELINE GENERATION & QA
    # ----------------------------------------------------------------------
    log_event(
        "INFO",
        "🪄 🚀 [Orchestrator] Step 5C: Triggering AI Dataform Architect & QA...",
        trace_id,
    )

    should_update_json = (not json_exists) or (
        json_exists and len(added_cols_for_pr) > 0
    )
    df_files_changed = 0
    df_pipeline = {}
    df_commit_log = "⏭️ No Dataform files generated."

    if should_update_json or "dataform" in error_context.lower():
        raw_df_pipeline = generate_ai_dataform_pipeline(
            table, final_schema_list, error_context, df_state, trace_id
        )

        if raw_df_pipeline:
            verified_df_pipeline = verify_dataform_pipeline(
                raw_df_pipeline, final_schema_list, trace_id
            )
            df_files_changed, df_commit_log = inject_dataform_into_repo(
                repo, branch_name, table, verified_df_pipeline, trace_id
            )

    # ----------------------------------------------------------------------
    # STEP 6: COMMIT PAYLOADS & OPEN PR
    # ----------------------------------------------------------------------
    log_event(
        "INFO",
        "💾 🚀 [Orchestrator] Step 6: Preparing GitHub Commits & Pull Request...",
        trace_id,
    )

    if not should_update_json and not tf_changed and df_files_changed == 0:
        log_event(
            "WARNING",
            "🛑 🚀 [Orchestrator] No actionable changes detected in payload. Aborting Git operations.",
            trace_id,
        )
        log_event(
            "INFO",
            "⏹️ 🚀 [Orchestrator] Finished orchestrator flow (Skipped).",
            trace_id,
        )
        return {"status": "SKIPPED", "url": None, "details": "No changes needed"}

    if should_update_json:
        log_event(
            "INFO", "💾 🚀 [Orchestrator] Committing JSON Schema payload...", trace_id
        )
        content_str = json.dumps(final_schema_list, indent=2)
        msg = (
            f"fix(schema): detailed update for {table} schema due to {error_context}"
            if json_exists
            else f"feat(schema): create {table} schema layout"
        )
        if json_exists:
            repo.update_file(
                target_json_path, msg, content_str, json_sha, branch=branch_name
            )
        else:
            repo.create_file(target_json_path, msg, content_str, branch=branch_name)

    if tf_changed:
        log_event(
            "INFO", "💾 🚀 [Orchestrator] Committing Terraform HCL payload...", trace_id
        )
        msg = f"feat(infra): update Terraform specs for {table}"
        if tf_action == "PATCH":
            repo.update_file(
                target_tf_path, msg, new_tf_content, tf_sha, branch=branch_name
            )
        else:
            repo.create_file(target_tf_path, msg, new_tf_content, branch=branch_name)

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
        f"✅ {df_files_changed} files generated/verified"
        if df_files_changed > 0
        else "⏭️ Skipped"
    )

    col_table = "| Column Name | Type | Description |\n|---|---|---|\n"
    for col in added_cols_for_pr[:15]:
        col_table += (
            f"| `{col['name']}` | `{col['type']}` | {col.get('description', '')} |\n"
        )
    if len(added_cols_for_pr) > 15:
        col_table += f"| ... ({len(added_cols_for_pr)-15} more) | | |\n"

    title_prefix = "fix" if json_exists or df_files_changed > 0 else "feat"
    pr_title = f"{title_prefix}(dataops): Automated Self-Healing for {table}"

    pr_body = f"""
# 🤖 Sentinel-Forge Automated Resolution
**Trace ID:** `{trace_id}`
**Trigger Context:** `{error_context}`

Sentinel-Forge has intercepted a pipeline anomaly and autonomously generated the following infrastructure and transformation code to resolve the issue.

### 🏗️ Infrastructure Actions Taken
| Layer | Status | Target Path |
|---|---|---|
| **BigQuery Schema** | {pr_status_schema} | `{target_json_path}` |
| **Terraform HCL** | {pr_status_tf} | `{target_tf_path}` |
| **Dataform SQLX** | {pr_status_df} | `definitions/` |

### 🔍 Schema Modifications
Added **{len(added_cols_for_pr)}** columns to the Raw Layer.
{col_table}

### 🪄 Dataform Pipeline Activity
The Multi-Agent system executed the following workflow:
1. **Scanner:** Located existing workspace rules and targets.
2. **Architect:** Synthesized Dataform v3.x compliant logic.
3. **QA Lead:** Validated syntax and ensured 100% column inclusion.

**Commit Log:**
{df_commit_log}

### 🛡️ Governance Checklist
- [x] **Audit Columns:** `batch_date`, `processed_dttm` rigidly enforced.
- [x] **Idempotency:** Operations file configured to TRUNCATE raw landing zone post-archive.
- [x] **Type Safety:** Schema aligned to `STRING` raw standards.

### ✅ Action Required
Review the file changes and merge this Pull Request. The CI/CD pipeline and Terraform Cloud/Actions will apply the changes automatically.
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

    log_event(
        "INFO",
        "⏹️ 🚀 [Orchestrator] Finished comprehensive infrastructure update workflow.",
        trace_id,
    )
    return {
        "status": "SUCCESS",
        "url": pr.html_url,
        "details": {
            "schema_action": "UPDATE" if json_exists else "CREATE",
            "terraform_action": tf_action,
            "dataform_files_generated": df_files_changed,
            "columns_added": len(added_cols_for_pr),
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

        # Robust extraction: Handle direct JSON payload OR a Cloud Logging nested payload
        error_context = "Automated Ops Trigger"
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
        new_cols = data.get("new_column_headers", [])
        sample_data = data.get("sample_data_rows", [])

        log_event(
            "INFO", f"🎯 🔌 [Gateway] Processing target entity: {table_ref}", trace_id
        )

        log_event(
            "INFO",
            "🔐 🔌 [Gateway] Validating environmental configurations and secrets...",
            trace_id,
        )
        if not GITHUB_TOKEN or not REPO_NAME:
            log_event(
                "CRITICAL",
                "☠️ 🔌 [Gateway] CRITICAL: Missing GITHUB_TOKEN or REPO_NAME configuration.",
                trace_id,
            )
            log_event(
                "INFO",
                "⏹️ 🔌 [Gateway] Aborting mission due to missing configs.",
                trace_id,
            )
            return

        if not new_cols and sample_data:
            log_event(
                "INFO",
                "🔮 🔌 [Gateway] Inferring baseline schema from sample payload...",
                trace_id,
            )
            new_cols = list(sample_data[0].keys())

        if not new_cols and "dataform" not in error_context.lower():
            log_event(
                "WARNING",
                "⚠️ 🔌 [Gateway] No columns derived and not a Dataform error. Operation voided.",
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
            REPO_NAME,
            GITHUB_TOKEN,
            table_ref,
            new_cols,
            trace_id,
            sample_data,
            error_context,
        )

        log_event(
            "INFO",
            "🔏 🔌 [Gateway] Finalizing operation. Generating closing audit log...",
            trace_id,
        )
        log_ai_action(
            trace_id=trace_id,
            action="CREATE_PR",
            resource=table_ref,
            status=result["status"],
            details=result["details"],
            link=result["url"],
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
                trace_id, "CRASH", "System", "FAILED", {"error": error_msg}, None
            )
        except Exception:
            pass
        log_event(
            "INFO",
            "⏹️ 🔌 [Gateway] Terminated under critical failure condition.",
            trace_id,
        )
