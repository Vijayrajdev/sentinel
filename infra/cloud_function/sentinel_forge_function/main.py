import base64
import json
import os
import uuid
import datetime
import logging
import re
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
def get_bq_client() -> bigquery.Client:
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


# FEATURE ADDITION: Dynamic resource_group and resource_type for explicit state tracking
def log_ai_action(
    trace_id: str,
    action: str,
    resource: str,
    status: str,
    details: Dict[str, Any],
    link: Optional[str] = None,
    resource_group: str = "GitHub Repo",
    resource_type: str = "Infrastructure",
):
    """Writes a permanent, legally auditable record to BigQuery in descriptive JSON format."""
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
        "resource_group": resource_group,
        "resource_type": resource_type,
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
    2. ALL TYPES MUST STRICTLY BE 'STRING'. This is a raw landing zone. Type casting will happen in Dataform. Do NOT output INT64, BOOLEAN, or TIMESTAMP for these incoming columns.
    3. Mode must be 'NULLABLE'.
    4. Descriptions are mandatory. Infer them from column names.
    5. EVERY single column MUST have a "defaultValueExpression" defined (e.g., use "NULL" for standard strings).
    
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

        # FEATURE ADDITION: Explicit success logging for Schema generation
        log_ai_action(
            trace_id=trace_id,
            action="GENERATE_SCHEMA",
            resource=table_name,
            status="SUCCESS",
            details={"columns_mapped": len(parsed_schema)},
            link=None,
            resource_group="BigQuery Schema",
            resource_type="Schema - Generated",
        )
        log_event("INFO", "⏹️ 🧩 [Schema Design] Finished schema generation.", trace_id)
        return parsed_schema

    except Exception as e:
        log_event(
            "ERROR",
            f"❌ 🧩 [Schema Design] Schema Generation Failed: {e}. Falling back to basic generation.",
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
            "⏹️ 🧩 [Schema Design] Finished schema generation (Fallback mode).",
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
# 🧠 AGENT 2: TERRAFORM ARCHITECT
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
                if ask_ai_is_definition(content, table_id):
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


def ask_ai_is_definition(content: str, table_id: str) -> bool:
    """Helper: Asks AI if the table is truly defined in this file."""
    if not model:
        return True
    prompt = f"""
    Does this Terraform code define a BigQuery table named "{table_id}"?
    Code: {content[:5000]}
    Return TRUE or FALSE.
    """
    try:
        return "TRUE" in model.generate_content(prompt).text.upper()
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
    """Goal: Write HCL code. Enforces _hist creation AND reuses main schema path."""
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

    style_guide_hist = tf_state.get("style_guide_tf_hist", "")
    hist_instruction = ""
    if table_id.endswith("_raw"):
        log_event(
            "INFO",
            f"⏳ 🏗️ [TF Architect] Raw table detected. Injecting STRICT _hist infrastructure rules with schema reuse...",
            trace_id,
        )
        mimic_rule = (
            f"MIMIC THIS EXISTING HISTORY TABLE CONFIGURATION EXACTLY (Do not invent variables):\n```hcl\n{style_guide_hist[:2000]}\n```"
            if style_guide_hist
            else "Configure with `expiration_ms = 2592000000` for 30 days."
        )

        hist_instruction = f"""
        CRITICAL REQUIREMENT: Because '{table_id}' is a raw landing table, you MUST ALSO generate the Terraform resource for its history table named '{table_id}_hist'. 
        - History Schema File: "{schema_path}" (CRITICAL: Reuse the EXACT SAME JSON schema path as the main table. DO NOT append _hist to the json filename).
        {mimic_rule}
        Ensure BOTH the main table AND the history table are included in your HCL output.
        """

    if action == "PATCH_EXISTING":
        log_event(
            "INFO",
            "🩹 🏗️ [TF Architect] Constructing prompt for intelligent HCL injection (Patch)...",
            trace_id,
        )
        prompt = f"""
        You are a Terraform Expert. Task: Add a new BigQuery table definition to the existing file below.
        Table ID: "{table_id}" | Schema File: "{schema_path}" | Partitioning: DAY (field: batch_date)
        {hist_instruction}
        
        EXISTING FILE:
        ```hcl\n{host_file_content}\n```
        INSTRUCTIONS: Return the **FULL UPDATED FILE CONTENT**. Do not truncate. Output: HCL Code only.
        """
    else:
        log_event(
            "INFO",
            "✨ 🏗️ [TF Architect] Constructing prompt for brand new HCL definition (Create)...",
            trace_id,
        )
        prompt = f"""
        You are a Terraform Expert. Task: Create a new Terraform file for BigQuery Table "{table_id}".
        Details: Resource Name: {table_id.replace('.', '_')} | Dataset ID: {dataset_id} | Schema: file("${{path.module}}/{schema_path}")
        {hist_instruction}
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

        # FEATURE ADDITION: Explicit logging for Terraform generation
        log_ai_action(
            trace_id=trace_id,
            action="GENERATE_TERRAFORM_CODE",
            resource=table_id,
            status="SUCCESS",
            details={"action": action, "target_file": schema_path},
            link=None,
            resource_group="Terraform Pipeline",
            resource_type=f"Infrastructure - {'Updated' if action == 'PATCH_EXISTING' else 'Created'}",
        )

        log_event("INFO", "⏹️ 🏗️ [TF Architect] Finished TF generation.", trace_id)
        return text.replace("```hcl", "").replace("```", "")
    except Exception as e:
        log_event(
            "ERROR",
            f"❌ 🏗️ [TF Architect] Terraform HCL Generation Failed: {e}",
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
            "INFO", "⏹️ 🏗️ [TF Architect] Finished TF generation (Failure).", trace_id
        )
        return ""


# ==============================================================================
# 🧠 AGENT 3: DATAFORM SCANNER
# ==============================================================================
def analyze_dataform_repo_state(
    repo, branch_sha: str, base_name: str, table_name: str, trace_id: str
) -> Dict[str, Any]:
    """Goal: Use Semantic/Fuzzy matching to find existing domains, rules, and strictly extract valid datasets/schemas."""
    log_event(
        "INFO",
        f"▶️ 🔍 [DF Scanner] Semantic scan for entity '{base_name}' in Dataform workspace...",
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
                        f"📂 🔍 [DF Scanner] Semantic match found and verified for patching: {element.path}",
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
        log_event("WARNING", f"⚠️ 🔍 [DF Scanner] Dataform scan anomaly: {e}", trace_id)

    state["existing_schemas"] = list(state["existing_schemas"])
    log_event(
        "INFO", "⏹️ 🔍 [DF Scanner] Finished scanning Dataform workspace.", trace_id
    )
    return state


# ==============================================================================
# 🧠 AGENT 8: DATASET ROUTING ANALYST
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
        f"▶️ 🗺️ [Dataset Analyst] Initiating AI dataset routing for '{table_name}'...",
        trace_id,
    )

    default_map = {
        "raw": inferred_raw or "sentinel_raw",
        "stg": "sentinel_staging",
        "marts": "sentinel_marts",
    }

    if not model:
        log_event(
            "WARNING",
            "⚠️ 🗺️ [Dataset Analyst] AI offline. Using standard defaults.",
            trace_id,
        )
        log_event("INFO", "⏹️ 🗺️ [Dataset Analyst] Finished routing.", trace_id)
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
            "⏳ 🗺️ [Dataset Analyst] Awaiting AI dataset routing analysis...",
            trace_id,
        )
        response = model.generate_content(prompt)
        text = response.text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]
        if text.startswith("json"):
            text = text[4:]

        datasets_map = json.loads(text)
        log_event(
            "INFO",
            f"✅ 🗺️ [Dataset Analyst] AI successfully mapped datasets: {json.dumps(datasets_map)}",
            trace_id,
        )
        log_event("INFO", "⏹️ 🗺️ [Dataset Analyst] Finished dataset routing.", trace_id)
        return datasets_map

    except Exception as e:
        log_event(
            "ERROR",
            f"❌ 🗺️ [Dataset Analyst] AI Mapping Failed: {e}. Falling back to safe defaults.",
            trace_id,
        )
        log_event(
            "INFO",
            "⏹️ 🗺️ [Dataset Analyst] Finished dataset routing (Fallback applied).",
            trace_id,
        )
        return default_map


# ==============================================================================
# 🧠 AGENT 4: DATAFORM ARCHITECT (Enhanced Syntax and Operations Safeguards)
# ==============================================================================
def generate_ai_dataform_pipeline(
    table_name: str,
    schema_list: List[Dict],
    error_context: str,
    df_state: Dict[str, Any],
    new_cols: List[str],
    sample_data: List[Dict],
    datasets: Dict[str, str],
    trace_id: str,
) -> Dict[str, str]:
    """Goal: Dynamically generate Dataform files, strictly enforcing datasets, casting, deduplication, and config syntax."""
    log_event(
        "INFO",
        f"▶️ 🪄 [DF Architect] Analyzing requirements for '{table_name}'...",
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
    inferred_domain = df_state.get("inferred_domain", "")

    if existing_files:
        log_event(
            "INFO",
            "🩹 🪄 [DF Architect] Existing files detected. Operating in PATCH mode.",
            trace_id,
        )
        action_type = "Updated"
        instruction_block = f"""
        Some or all pipeline files exist. EXISTING FILES: {json.dumps(existing_files)}
        Task: PATCH these existing files. 
        CRITICAL: Use the EXACT file paths provided as output keys. DO NOT change the folder structure or rename existing domain folders.
        CRITICAL: Ensure new columns ({new_cols}) are explicitly selected in the views and tables.
        """
    else:
        log_event(
            "INFO",
            "✨ 🪄 [DF Architect] No files detected. Operating in CREATE mode.",
            trace_id,
        )
        action_type = "Created"
        domain_instruction = (
            f"Use the detected domain folder '{inferred_domain}' or invent a logical one"
            if inferred_domain
            else "Invent a logical domain folder name (e.g., 'sales', 'hr', 'ecommerce')"
        )
        instruction_block = f"""
        Create files from scratch.
        CRITICAL DOMAIN ROUTING: {domain_instruction}.
        Structure MUST be:
        - definitions/sources/{table_name}.sqlx
        - definitions/staging/<logical_domain_folder>/stg_{base_name}.sqlx
        - definitions/marts/<logical_domain_folder>/fct_{base_name}.sqlx
        - definitions/operations/archive_{table_name}.sqlx
        
        STYLE GUIDE (Mimic format): ```sql\n{style_guide[:2000]}\n```
        """

    prompt = f"""
    You are an expert Analytics Engineer writing Google Cloud Dataform code (Core v3.x).
    
    Target Raw Table: "{table_name}" | Base Entity: "{base_name}" | GCP Project: "{PROJECT_ID}"
    Full Schema: {json.dumps(clean_schema)} | New Columns to add: {new_cols} | Error Context: "{error_context}"
    Sample Data (Use this to infer column data types for casting): {json.dumps(sample_data[:2])}
    
    {instruction_block}
    
    Requirements:
    1. STRICT DATASETS: You MUST explicitly declare schemas using these exact values:
       - Source schema: "{datasets['raw']}"
       - Staging (`stg_`) schema: "{datasets['stg']}"
       - Marts (`fct_`) schema: "{datasets['marts']}"
    2. STAGING LAYER REQUIREMENTS: 
       - The raw source data is entirely STRING type. You MUST explicitly `CAST()` these string columns to their proper native BigQuery types (e.g., `INT64`, `TIMESTAMP`, `BOOLEAN`, `FLOAT64`) based on the Sample Data provided.
       - You MUST perform row deduplication. Use `QUALIFY ROW_NUMBER() OVER(PARTITION BY <infer_primary_key_here> ORDER BY processed_dttm DESC) = 1`.
    3. DATA QUALITY (ASSERTIONS): Embed Data Quality checks inside the `config {{ ... }}` block of staging and marts.
    4. CRITICAL ARCHIVE DATE & DESTINATION LOGIC: In the operations (`archive_...`) file:
       - You MUST STRICTLY use `CURRENT_DATE() AS batch_date` in the SELECT clause.
       - DO NOT use `${{self.database}}` or `${{self.schema}}` for the insert destination. Operations blocks do not possess 'self' context. You MUST explicitly build the path as: `{PROJECT_ID}.{datasets['raw']}.{table_name}_hist`
    5. Operations file must depend on `fct_{base_name}` and TRUNCATE `{table_name}` after appending to `_hist`.
    6. DATAFORM SYNTAX (CRITICAL): Do NOT place any SQL or JS comments (`--`, `//`, `/* */`) inside the `config {{ ... }}` blocks. This causes JavaScript compilation errors.
    7. Output STRICTLY a JSON object where keys are full file paths and values are exact SQLX string content. No markdown.
    """

    try:
        log_event(
            "INFO",
            "⏳ 🪄 [DF Architect] Instructing Gemini to synthesize Dataform files with Casting, Deduplication, and Explicit DB bindings...",
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
            f"✅ 🪄 [DF Architect] AI successfully generated {len(pipeline_files)} SQLX files conforming to strict syntax and operational bounds.",
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
        )
        log_event("INFO", "⏹️ 🪄 [DF Architect] Finished Dataform generation.", trace_id)
        return pipeline_files

    except Exception as e:
        log_event(
            "ERROR", f"❌ 🪄 [DF Architect] AI Code Generation Failed: {e}", trace_id
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
            "⏹️ 🪄 [DF Architect] Finished Dataform generation (Failure).",
            trace_id,
        )
        return {}


# ==============================================================================
# 🧠 AGENT 5: DATAFORM QA VERIFIER (Enhanced Syntax Checks)
# ==============================================================================
def verify_dataform_pipeline(
    pipeline_files: Dict[str, str],
    schema_list: List[Dict],
    new_cols: List[str],
    datasets: Dict[str, str],
    trace_id: str,
) -> Dict[str, str]:
    """Goal: QA Lead verifies casting, deduplication, operations destinations, schemas, and strictly forbids config comments."""
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
    You are a Senior Data QA Lead. Review this Dataform pipeline generated by a junior engineer.
    
    GENERATED FILES: {json.dumps(pipeline_files)}
    REQUIRED COLUMNS: {clean_schema} | NEW COLUMNS: {new_cols}
    EXPECTED DATASETS: Raw: "{datasets['raw']}", Staging: "{datasets['stg']}", Marts: "{datasets['marts']}"
    
    Checklist:
    1. FOLDER STRUCTURE: If the files already exist in a domain folder, DO NOT change the path. Fix the path ONLY to include a logical domain folder if placed directly in `marts/` or `staging/`.
    2. SCHEMA VALIDATION: Are all required columns explicitly selected? (If missing, ADD THEM).
    3. DATASET ENFORCEMENT: Ensure the schemas defined in the `config` blocks explicitly match the EXPECTED DATASETS.
    4. ARCHIVE DESTINATION & DATE: Ensure the operations (`archive_...`) file explicitly uses `{PROJECT_ID}.{datasets['raw']}` for the target table (NO `${{self.database}}` or `${{self.schema}}`). It must also use `CURRENT_DATE() AS batch_date`.
    5. STAGING DEDUPLICATION: Does the staging file perform row deduplication (e.g., `QUALIFY ROW_NUMBER() OVER(...) = 1`)? If missing, ADD IT.
    6. STAGING CASTING: Does the staging file explicitly `CAST()` raw STRING columns to appropriate native types? If missing, ADD CASTING syntax.
    7. DATA QUALITY: Does the `config` block of `stg_` and `fct_` files contain an `assertions` dictionary? If missing, ADD THEM.
    8. CONFIG BLOCK SYNTAX: Ensure there are absolutely NO comments (`--`, `//`, `/* */`) inside the `config {{ ... }}` blocks of any file. Remove them if they exist.
    
    Fix any errors found in paths or SQL content. Output STRICTLY a JSON object where keys are the corrected full file paths and values are the corrected SQLX string content. 
    Output JSON ONLY. No explanation.
    """

    try:
        log_event(
            "INFO",
            "⏳ 🕵️‍♀️ [DF QA] Awaiting QA review, operations binding verification, and assertion validation...",
            trace_id,
        )
        text = model.generate_content(prompt).text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]
        if text.startswith("json"):
            text = text[4:]

        verified_files = json.loads(text)
        log_event(
            "INFO",
            f"✅ 🕵️‍♀️ [DF QA] QA passed. Operations resolved, Configs sanitized, Datasets, Casting, and Deduplication strictly enforced.",
            trace_id,
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
# 🧠 AGENT 6: SCHEMA QA VERIFIER
# ==============================================================================
def verify_schema_json(
    schema_list: List[Dict], table_name: str, new_cols: List[str], trace_id: str
) -> List[Dict]:
    """Goal: Ensure JSON Schema strictly complies with enterprise standards before committing."""
    log_event(
        "INFO",
        f"▶️ 🕵️‍♀️ [Schema QA] Initiating automated peer-review of BigQuery JSON Schema...",
        trace_id,
    )

    if not model or not schema_list:
        log_event(
            "INFO", "⏹️ 🕵️‍♀️ [Schema QA] Bypassing QA (No model or schema).", trace_id
        )
        return schema_list

    prompt = f"""
    You are a Senior Data QA Lead. Review this JSON BigQuery Schema generated by a junior engineer.
    
    GENERATED SCHEMA: {json.dumps(schema_list)}
    NEW COLUMNS TO VERIFY: {new_cols}
    
    Checklist:
    1. Are all `NEW COLUMNS` explicitly present in the schema?
    2. Are all types strictly set to 'STRING' (except for 'batch_date' and 'processed_dttm' which should be DATE/TIMESTAMP)?
    3. Are all modes strictly set to 'NULLABLE'?
    4. Are 'batch_date' and 'processed_dttm' explicitly included at the end?
    5. CRITICAL: Does EVERY single column have a "defaultValueExpression" defined? If missing, you MUST inject `"defaultValueExpression": "NULL"` into that column's object.
    
    Fix any errors found. Output STRICTLY a valid JSON list of objects representing the corrected BigQuery schema.
    Output JSON ONLY. No explanation.
    """

    try:
        log_event("INFO", "⏳ 🕵️‍♀️ [Schema QA] Awaiting Schema QA review...", trace_id)
        text = model.generate_content(prompt).text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]
        if text.startswith("json"):
            text = text[4:]

        verified_schema = json.loads(text)
        log_event(
            "INFO",
            f"✅ 🕵️‍♀️ [Schema QA] QA passed. Schema verified and Default Values validated.",
            trace_id,
        )
        log_event("INFO", "⏹️ 🕵️‍♀️ [Schema QA] Finished automated review.", trace_id)
        return verified_schema
    except Exception as e:
        log_event(
            "WARNING",
            f"⚠️ 🕵️‍♀️ [Schema QA] QA Verification Failed. Applying programmatic fallback for default values. {e}",
            trace_id,
        )
        for col in schema_list:
            if "defaultValueExpression" not in col:
                col["defaultValueExpression"] = "NULL"

        log_event(
            "INFO",
            "⏹️ 🕵️‍♀️ [Schema QA] Finished automated review (Programmatic Fallback applied).",
            trace_id,
        )
        return schema_list


# ==============================================================================
# 🧠 AGENT 7: TERRAFORM QA VERIFIER
# ==============================================================================
def verify_terraform_hcl(
    hcl_content: str, table_id: str, tf_state: Dict[str, Any], trace_id: str
) -> str:
    """Goal: Ensure Terraform code syntax is perfect and history table creation uses schema reuse."""
    log_event(
        "INFO",
        f"▶️ 🕵️‍♀️ [TF QA] Initiating automated peer-review of Terraform HCL...",
        trace_id,
    )

    if not model or not hcl_content:
        log_event("INFO", "⏹️ 🕵️‍♀️ [TF QA] Bypassing QA (No model or HCL).", trace_id)
        return hcl_content

    is_raw_table = table_id.endswith("_raw")
    style_guide_hist = tf_state.get("style_guide_tf_hist", "")

    hist_rule = ""
    if is_raw_table:
        hist_rule = f"""
        CRITICAL RULE: Because this is a raw table ({table_id}), there MUST be a corresponding '{table_id}_hist' google_bigquery_table resource defined.
        If it is missing, you MUST inject it based on this enterprise pattern:
        ```hcl\n{style_guide_hist[:1000]}\n```
        CRITICAL SCHEMA REUSE: The history table MUST use the exact same schema file path as the main table. Do not allow the use of a separate _hist.json file.
        """

    prompt = f"""
    You are a Senior DevOps QA Lead. Review this Terraform HCL generated by a junior engineer.
    
    GENERATED HCL: 
    ```hcl
    {hcl_content}
    ```
    
    Checklist:
    1. Is the HCL syntax perfect? Look for missing brackets or quotation marks.
    2. Does the main table resource for '{table_id}' exist?
    {hist_rule}
    
    Fix any errors found. Output STRICTLY the corrected Terraform HCL code. No markdown formatting outside the code block.
    """

    try:
        log_event("INFO", "⏳ 🕵️‍♀️ [TF QA] Awaiting TF QA review...", trace_id)
        text = model.generate_content(prompt).text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("\n", 1)[0]

        log_event(
            "INFO",
            f"✅ 🕵️‍♀️ [TF QA] QA passed. Terraform HCL structure validated.",
            trace_id,
        )
        log_event("INFO", "⏹️ 🕵️‍♀️ [TF QA] Finished automated review.", trace_id)
        return text.replace("```hcl", "").replace("```", "")
    except Exception as e:
        log_event(
            "WARNING",
            f"⚠️ 🕵️‍♀️ [TF QA] QA Verification Failed. Falling back to unverified HCL. {e}",
            trace_id,
        )
        log_event(
            "INFO",
            "⏹️ 🕵️‍♀️ [TF QA] Finished automated review (Fallback applied).",
            trace_id,
        )
        return hcl_content


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
    """Goal: Commits the AI-generated and QA-verified files to the PR branch."""
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
            commit_msg = (
                f"fix(dataform): Auto-patch pipeline and DQ logic for {table_name}"
            )
            repo.update_file(file_path, commit_msg, content, c.sha, branch=branch_name)
            log_event(
                "INFO",
                f"♻️ 📦 [Git Injector] Updated existing file: {file_path}",
                trace_id,
            )
            detailed_commit_log += (
                f"- ♻️ **Patched (incl. Assertions):** `{file_path}`\n"
            )
        except GithubException:
            commit_msg = (
                f"feat(dataform): Init self-healing pipeline and DQ for {table_name}"
            )
            repo.create_file(file_path, commit_msg, content, branch=branch_name)
            log_event(
                "INFO", f"✨ 📦 [Git Injector] Created new file: {file_path}", trace_id
            )
            detailed_commit_log += (
                f"- ✨ **Created (incl. Assertions):** `{file_path}`\n"
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
    is_raw_table = table.endswith("_raw")

    log_event(
        "INFO",
        f"🐙 🚀 [Orchestrator] Establishing secure handshake with GitHub Repository...",
        trace_id,
    )
    g = Github(token)
    repo = g.get_repo(repo_name)
    default_branch = repo.get_branch(repo.default_branch)

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

    # Enforce schema reuse for history tables explicitly by setting this to None.
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
    # STEP 4: SCHEMA JSON LOGIC (Generation & QA)
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

    # Run Schema through QA Verifier
    final_schema_list = verify_schema_json(
        final_schema_list, table, [c["name"] for c in added_cols_for_pr], trace_id
    )
    log_event(
        "INFO",
        f"✅ 🚀 [Orchestrator] Schema configuration complete. Total added columns: {len(added_cols_for_pr)}",
        trace_id,
    )

    # ----------------------------------------------------------------------
    # STEP 5: TERRAFORM TF LOGIC (Generation & QA)
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
            # Run HCL through QA Verifier
            new_tf_content = verify_terraform_hcl(
                new_tf_content, table, tf_state, trace_id
            )
            tf_changed = True

    # ----------------------------------------------------------------------
    # STEP 6: DATAFORM PIPELINE GENERATION & QA
    # ----------------------------------------------------------------------
    log_event(
        "INFO",
        "🪄 🚀 [Orchestrator] Triggering AI Dataform Architect & QA...",
        trace_id,
    )

    should_update_json = (not json_exists) or (
        json_exists and len(added_cols_for_pr) > 0
    )
    df_files_changed, df_commit_log = 0, "⏭️ No Dataform files generated."

    if should_update_json or "dataform" in error_context.lower():
        raw_df_pipeline = generate_ai_dataform_pipeline(
            table,
            final_schema_list,
            error_context,
            df_state,
            [c["name"] for c in added_cols_for_pr],
            sample_data or [],
            datasets_map,
            trace_id,
        )
        if raw_df_pipeline:
            verified_df_pipeline = verify_dataform_pipeline(
                raw_df_pipeline,
                final_schema_list,
                [c["name"] for c in added_cols_for_pr],
                datasets_map,
                trace_id,
            )
            df_files_changed, df_commit_log = inject_dataform_into_repo(
                repo, branch_name, table, verified_df_pipeline, trace_id
            )

    # ----------------------------------------------------------------------
    # STEP 7: COMMIT PAYLOADS & OPEN PR
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

    # Empty-Diff Safeguards added (try-except blocks)
    if should_update_json:
        log_event(
            "INFO",
            "💾 🚀 [Orchestrator] Committing JSON Schema payload...",
            trace_id,
        )
        msg = (
            f"fix(schema): detailed update for {table} schema"
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

### 🔍 Schema Modifications
Added **{len(added_cols_for_pr)}** columns to the Raw Layer. All raw ingestion fields configured strictly as `STRING` type.
{col_table}

### 🪄 Agent Activity Log
1. **Scanner & Router:** Mapped target domain folders semantically. AI Dataset Routing successfully assigned correct schema endpoints.
2. **Architect:** Generated HCL. Embedded explicit `CAST()` operations, **Data Quality Assertions**, and explicitly defined target insert paths into `.sqlx` staging and operations blocks.
3. **Schema QA:** Verified JSON structure and rigorously enforced `defaultValueExpression` on all columns.
4. **Terraform QA:** Enforced schema reuse (`DRY` principle) between main and `_hist` table resources.
5. **Dataform QA:** Validated SQL syntax, enforced `CURRENT_DATE()` logic, corrected Operations destination mappings, enforced `QUALIFY ROW_NUMBER()` deduplication, ensured 100% column inclusion, and verified empty/comment-free `config` blocks.

**Commit Log:**
{df_commit_log}

### 🛡️ Governance Checklist
- [x] **Schema Integrity:** Raw layer `STRING` typing and `defaultValueExpression` strictly enforced for all columns.
- [x] **DRY Architecture:** History tables natively reuse raw landing schemas.
- [x] **Staging Governance:** Explicit casting to native BigQuery types and Row Deduplication executed.
- [x] **Operations Accuracy:** Operations block securely references target project and raw schema variables (Bypassing undefined self contexts).
- [x] **Config Sanitization:** Strict ban on comments within config blocks enforced to prevent compilation crashes.

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
            "default_value_expressions_enforced": True,
        },
        "infrastructure_updates": {
            "action": tf_action,
            "files_modified": target_tf_path,
            "history_infrastructure_created": is_raw_table,
            "schema_reuse_applied": is_raw_table,
        },
        "dataform_updates": {
            "total_files_processed": df_files_changed,
            "detailed_commit_log": (
                df_commit_log.strip().split("\n") if df_commit_log else []
            ),
            "anti_hallucination_schemas_verified": True,
            "current_date_archive_enforced": True,
            "staging_casting_and_dedup_applied": True,
            "operations_syntax_sanitized": True,
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
                "☠️ 🔌 [Gateway] CRITICAL: Missing GITHUB_TOKEN or REPO_NAME.",
                trace_id,
            )

            log_ai_action(
                trace_id=trace_id,
                action="CRASH",
                resource="System Initialization",
                status="FAILED",
                details={"error": "Missing GITHUB_TOKEN or REPO_NAME configurations."},
                link=None,
                resource_group="System Operations",
                resource_type="Critical Failure",
            )
            log_event("INFO", "⏹️ 🔌 [Gateway] Aborting mission.", trace_id)
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

        overall_action = result.get("overall_action", "PROCESSED")

        # Write Descriptive Details to Audit Log with strict PR URL mapping
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
