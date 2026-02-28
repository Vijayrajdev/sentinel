import os
import json
import uuid
import asyncio
import datetime
import logging
import time
import random
from typing import Dict, Any, Optional, Tuple, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from google.cloud import bigquery

import vertexai
from vertexai.generative_models import GenerativeModel

# ==============================================================================
# ⚙️ ENTERPRISE CONFIGURATION
# ==============================================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sentinel-insight")

app = FastAPI(title="Sentinel-Insight")
app.mount("/static", StaticFiles(directory="static"), name="static")

PROJECT_ID = os.environ.get("GCP_PROJECT")
REGION = os.environ.get("GCP_REGION", "global")
AUDIT_TABLE = os.environ.get(
    "AUDIT_TABLE", f"{PROJECT_ID}.sentinel_audit.insight_audit_log"
)

AI_MODEL_HEAVY_NAME = os.environ.get("AI_MODEL_HEAVY", "gemini-2.5-flash")
AI_MODEL_LITE_NAME = os.environ.get("AI_MODEL_LITE", "gemini-2.5-flash-lite")

COST_PER_TB = 6.25  # Standard BigQuery On-Demand Pricing

bq_client: Optional[bigquery.Client] = None
model_heavy = None
model_lite = None

try:
    if PROJECT_ID:
        bq_client = bigquery.Client(project=PROJECT_ID)
        vertexai.init(project=PROJECT_ID, location=REGION)
        model_heavy = GenerativeModel(AI_MODEL_HEAVY_NAME)
        model_lite = GenerativeModel(AI_MODEL_LITE_NAME)
        logger.info(
            f"✅ [Init] Vertex AI Online. Heavy: {AI_MODEL_HEAVY_NAME} | Lite: {AI_MODEL_LITE_NAME}"
        )
except Exception as e:
    logger.error(f"❌ [Init Fail] Failed to initialize GCP/Vertex clients: {e}")


# ==============================================================================
# 📝 OBSERVABILITY & TRAFFIC SMOOTHING
# ==============================================================================
async def stream_log(
    ws: WebSocket, severity: str, message: str, trace_id: str, is_error: bool = False
):
    """Streams emoji-rich logs directly to the UI and prints to stdout."""
    log_entry = f"[{trace_id}] {severity} - {message}"
    if is_error:
        logger.error(log_entry)
        await ws.send_json({"status": "error", "message": message})
    else:
        logger.info(log_entry)
        await ws.send_json({"status": "thinking", "message": message})


def smooth_traffic(trace_id: str):
    """Mitigates Vertex AI 429 Too Many Requests errors by pacing API calls."""
    logger.info(
        f"⏳ 🚦 [Traffic] Pacing AI request (3s) to prevent 429 Rate Limit burst... [{trace_id}]"
    )
    time.sleep(3)


def generate_content_with_retry(
    model_instance, prompt: str, trace_id: str, max_retries: int = 5
):
    """Executes Vertex AI generation with Exponential Backoff for 429 errors."""
    base_delay = 5
    for attempt in range(max_retries):
        try:
            smooth_traffic(trace_id)
            response = model_instance.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            error_msg = str(e).lower()
            if "429" in error_msg or "resource exhausted" in error_msg:
                if attempt == max_retries - 1:
                    logger.error(
                        f"🚨 🚦 [Traffic] Max retries reached for 429 error. Failing. [{trace_id}]"
                    )
                    raise e
                sleep_time = (base_delay * (2**attempt)) + random.uniform(0, 2)
                logger.warning(
                    f"⚠️ 🚦 [Traffic] 429 Quota Hit. Retrying in {sleep_time:.2f}s... [{trace_id}]"
                )
                time.sleep(sleep_time)
            else:
                raise e


def prune_markdown(text: str, lang: str = "sql") -> str:
    """Strips markdown formatting from AI responses."""
    if text.startswith("```"):
        try:
            return text.split("\n", 1)[1].rsplit("\n", 1)[0]
        except IndexError:
            return text.replace(f"```{lang}", "").replace("```", "")
    return text


def log_to_audit_table(
    trace_id: str, intent: str, status: str, event_log: dict, user_email: str
):
    """Writes telemetry directly to the BigQuery Audit table."""
    if not bq_client:
        return
    row = {
        "trace_id": trace_id,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "user_intent": intent,
        "status": status,
        "event_log": json.dumps({"user": user_email, **event_log}),
    }
    try:
        bq_client.insert_rows_json(AUDIT_TABLE, [row])
    except Exception as e:
        logger.error(f"❌ [Audit Fail] Failed to write ledger: {e}")


# ==============================================================================
# 🧠 INTENT ROUTER & EXECUTION
# ==============================================================================
async def agent_classify_intent(intent: str, trace_id: str, ws: WebSocket) -> str:
    """Agent 0: Classifies the user's intent into GREETING, EXPLORE, or GET_SQL."""
    await stream_log(
        ws, "INFO", "▶️ 🧠 [Agent 0: Router] Analyzing cognitive intent...", trace_id
    )
    prompt = f"""
    Analyze the user's intent: "{intent}"
    Classify into exactly one of these categories:
    - GREETING: If they say hi, hello, help, or ask what you can do.
    - GET_SQL: If they explicitly ask for the SQL code, a .sql file, to build a table, or give them the query.
    - EXPLORE: If they are asking for data, insights, or just running a query to see results/charts.
    Output ONLY the exact category word.
    """
    category = await asyncio.to_thread(
        generate_content_with_retry, model_lite, prompt, trace_id
    )
    category = category.strip().upper()
    await stream_log(
        ws, "INFO", f"⏹️ 🧠 [Agent 0: Router] Intent classified as: {category}", trace_id
    )
    return category if category in ["GREETING", "GET_SQL", "EXPLORE"] else "EXPLORE"


async def execute_and_format_results(sql: str, trace_id: str, ws: WebSocket) -> dict:
    """Executes the dry-run-validated SQL and formats it into HTML and JSON for Charts."""
    await stream_log(
        ws,
        "INFO",
        "▶️ 📊 [Execution] Querying BigQuery engine and formatting results...",
        trace_id,
    )
    try:
        safe_sql = f"SELECT * FROM ({sql}) LIMIT 2000"
        query_job = await asyncio.to_thread(bq_client.query, safe_sql)
        results = list(await asyncio.to_thread(query_job.result))

        # HTML Table adapted for Light/Dark mode
        html = "<div class='overflow-x-auto w-full mt-4 rounded-xl border border-slate-300 dark:border-white/10 shadow-lg max-h-96 bg-white/50 dark:bg-black/20'>"
        html += "<table class='w-full text-left text-xs text-slate-700 dark:text-gray-300 border-collapse'>"

        if not results:
            return {
                "html": "<div class='text-sm text-slate-500 dark:text-gray-400 italic mt-2'>Query returned zero results.</div>",
                "raw_data": [],
            }

        headers = [field.name for field in query_job.schema]
        html += "<thead class='bg-slate-200 dark:bg-white/5 text-emerald-600 dark:text-emerald-400 font-mono sticky top-0 shadow-sm'><tr>"
        for header in headers:
            html += f"<th class='px-4 py-3 border-b border-slate-300 dark:border-white/10'>{header}</th>"
        html += "</tr></thead><tbody>"

        raw_data = []
        for row in results:
            row_dict = dict(row.items())
            raw_data.append(row_dict)
            html += "<tr class='hover:bg-slate-100 dark:hover:bg-white/5 transition-colors duration-200'>"
            for val in row:
                html += f"<td class='px-4 py-3 border-b border-slate-200 dark:border-white/5 whitespace-nowrap'>{str(val)}</td>"
            html += "</tr>"

        html += "</tbody></table></div>"

        await stream_log(
            ws,
            "INFO",
            "⏹️ 📊 [Execution] Results successfully fetched and formatted.",
            trace_id,
        )
        return {"html": html, "raw_data": raw_data}
    except Exception as e:
        raise Exception(f"Failed to fetch results: {e}")


# ==============================================================================
# 🧠 THE AGENT SWARM FUNCTIONS
# ==============================================================================
async def agent_draft_sql(
    intent: str, chat_history: List[Dict], trace_id: str, ws: WebSocket
) -> str:
    """Agent 1: Writes the initial SQL based on intent, history, and strict data typing."""
    await stream_log(
        ws,
        "INFO",
        "▶️ 🧠 [Agent 1: SQL Architect] Initiating translation of business intent to SQL...",
        trace_id,
    )
    history_context = "\n".join(
        [f"{msg['role'].upper()}: {msg['content']}" for msg in chat_history[-5:]]
    )

    prompt = f"""
    You are an expert BigQuery Data Analyst. 
    Recent Conversation History:
    {history_context}
    
    Current Business Intent: "{intent}"
    
    Task: Write a highly optimized, complex BigQuery Standard SQL SELECT statement to fulfill this intent.
    
    CRITICAL REQUIREMENTS:
    1. TARGET DATASET: Assume standard schema naming conventions (e.g., `sentinel_marts.orders`, `sentinel_marts.products`). 
    2. PRIORITIZATION: If the user explicitly mentions a table or column, YOU MUST USE IT exactly as typed.
    3. MINIMALISM: Query ONLY the necessary columns required to answer the specific intent. Do not use `SELECT *` unless absolutely necessary.
    4. STRICT TYPING: Ensure calculated metrics are cast cleanly using SAFE_CAST.
    
    Output ONLY the raw SQL code. No markdown formatting.
    """
    sql = await asyncio.to_thread(
        generate_content_with_retry, model_heavy, prompt, trace_id
    )
    sql = prune_markdown(sql, "sql")
    await stream_log(
        ws,
        "INFO",
        "⏹️ 🧠 [Agent 1: SQL Architect] Successfully drafted strictly-typed SQL logic.",
        trace_id,
    )
    return sql


async def sandbox_dry_run_and_count(
    sql: str, trace_id: str, ws: WebSocket
) -> Tuple[bool, str, float, int, list]:
    """Validates SQL, calculates cost, extracts schema, and checks row bounds."""
    await stream_log(
        ws,
        "INFO",
        "▶️ 🧪 [Sandbox] Initiating BigQuery Dry-Run and Cost Analysis...",
        trace_id,
    )

    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    try:
        dry_run_job = await asyncio.to_thread(
            bq_client.query, sql, job_config=job_config
        )
        bytes_processed = dry_run_job.total_bytes_processed or 0
        cost = (bytes_processed / (1024**4)) * COST_PER_TB
        schema = [
            {"name": field.name, "type": field.field_type}
            for field in dry_run_job.schema
        ]

        await stream_log(
            ws,
            "INFO",
            f"⏹️ 🧪 [Sandbox] Dry-run passed. Bytes processed: {bytes_processed}. Checking row limits...",
            trace_id,
        )

        count_sql = f"SELECT COUNT(*) as cnt FROM ({sql})"
        count_job = await asyncio.to_thread(bq_client.query, count_sql)
        res = list(count_job.result())
        row_count = res[0].cnt if res else 0

        return True, "Valid", cost, row_count, schema
    except Exception as e:
        error_msg = str(e)
        await stream_log(
            ws,
            "WARNING",
            f"⚠️ 🧪 [Sandbox] FAILED: Target anomaly detected -> {error_msg}",
            trace_id,
        )
        return False, error_msg, 0.0, 0, []


async def agent_suggest_schema(
    intent: str, bad_sql: str, error_msg: str, trace_id: str, ws: WebSocket
) -> dict:
    """Agent: Analyzes 'Not Found' errors and suggests closely matching tables/columns."""
    await stream_log(
        ws,
        "INFO",
        "▶️ 🔎 [Agent: Schema Advisor] Analyzing failure for fuzzy schema matching...",
        trace_id,
    )
    prompt = f"""
    A user's query failed because a table or column does not exist.
    Intent: "{intent}"
    Failed SQL: {bad_sql}
    Error Message: {error_msg}
    
    Analyze the error. Deduce what the user ACTUALLY meant based on standard enterprise naming conventions.
    
    Output strictly a JSON object with this structure:
    {{
        "message": "It looks like the table/column was not found. Did you mean one of these?",
        "suggestions": ["suggestion_1", "suggestion_2", "suggestion_3"]
    }}
    """
    response = await asyncio.to_thread(
        generate_content_with_retry, model_lite, prompt, trace_id
    )
    response_clean = prune_markdown(response, "json")
    try:
        return json.loads(response_clean)
    except:
        return {
            "message": "Schema error detected. Please verify your table or column names.",
            "suggestions": [],
        }


async def agent_heal_sql(
    intent: str, bad_sql: str, error_msg: str, trace_id: str, ws: WebSocket
) -> str:
    """Agent 1b: Heals the SQL based on BigQuery error messages (Syntax errors, not missing tables)."""
    await stream_log(
        ws,
        "INFO",
        "▶️ 🚑 [Agent 1: Healer] Attempting autonomous SQL repair based on sandbox telemetry...",
        trace_id,
    )
    prompt = f"""
    You are an expert BigQuery Data Engineer. Your previous SQL failed a dry-run.
    Business Intent: "{intent}"
    Failed SQL: {bad_sql}
    BigQuery Error: {error_msg}
    Task: Fix the SQL query so it successfully executes. Output ONLY the raw SQL code. No markdown.
    """
    healed_sql = await asyncio.to_thread(
        generate_content_with_retry, model_heavy, prompt, trace_id
    )
    return prune_markdown(healed_sql, "sql")


# ==============================================================================
# FASTAPI ENDPOINTS
# ==============================================================================
@app.get("/")
async def get():
    with open("static/index.html", "r") as f:
        return HTMLResponse(f.read())


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    trace_id = uuid.uuid4().hex[:8]

    headers = dict(websocket.headers)
    user_email = headers.get(
        "x-goog-authenticated-user-email", "business.user@sentinel.ai"
    ).replace("accounts.google.com:", "")

    try:
        while True:
            data = await websocket.receive_text()
            payload = json.loads(data)

            intent = payload.get("intent", "").strip()
            chat_history = payload.get("history", [])
            action_override = payload.get("action", None)
            target_sql = payload.get("sql", None)

            logger.info(f"[{trace_id}] Received Intent: {intent} from {user_email}")
            log_to_audit_table(
                trace_id,
                intent,
                "STARTED",
                {"action": "User submitted query"},
                user_email,
            )

            try:
                # ---------------------------------------------------------
                # EXPLICIT CONFIRMATION LOGIC
                # ---------------------------------------------------------
                if action_override == "CONFIRM_EXPLORE" and target_sql:
                    results_dict = await execute_and_format_results(
                        target_sql, trace_id, websocket
                    )
                    await websocket.send_json(
                        {
                            "status": "explore_result",
                            "message": "✅ Heavy query execution confirmed. Here are your insights:",
                            "html": results_dict["html"],
                            "raw_data": results_dict["raw_data"],
                            "sql": target_sql,
                        }
                    )
                    log_to_audit_table(
                        trace_id,
                        "CONFIRM_EXPLORE",
                        "SUCCESS",
                        {"action": "Heavy Data Explored"},
                        user_email,
                    )
                    continue

                elif action_override == "CONFIRM_GET_SQL" and target_sql:
                    await websocket.send_json(
                        {
                            "status": "sql_only",
                            "message": "✅ Here is the validated SQL you requested. You can execute this directly in the BigQuery Console.",
                            "sql": target_sql,
                        }
                    )
                    log_to_audit_table(
                        trace_id,
                        "CONFIRM_GET_SQL",
                        "SUCCESS",
                        {"action": "SQL File Provided"},
                        user_email,
                    )
                    continue

                # ---------------------------------------------------------
                # STANDARD INTELLIGENCE LOOP
                # ---------------------------------------------------------
                intent_type = await agent_classify_intent(intent, trace_id, websocket)

                if intent_type == "GREETING":
                    greeting_msg = (
                        f"Hello `{user_email}`! 👋 I am your autonomous Sentinel-Insight Analyst.\n\n"
                        "**My Capabilities:**\n"
                        "1️⃣ **Explore Data & Generate Charts:** Ask me to pull business metrics. I will write the SQL, execute it safely, show you the data, and automatically plot interactive charts that you can download.\n"
                        "2️⃣ **Smart Schema Routing:** If you misspell a table or column name, I will scan the data warehouse and suggest the closest matches to correct it.\n"
                        "3️⃣ **Generate SQL Files:** If you want to build a table or just need the code, ask me for the SQL. I will provide the perfectly formatted `.sql` block for you to run in the BigQuery console.\n\n"
                        "How can I assist your data journey today?"
                    )
                    await websocket.send_json(
                        {"status": "greeting", "message": greeting_msg}
                    )
                    continue

                sql = await agent_draft_sql(intent, chat_history, trace_id, websocket)

                is_valid, error_msg, cost, row_count, schema_fields = (
                    await sandbox_dry_run_and_count(sql, trace_id, websocket)
                )

                # SELF HEALING & SCHEMA SUGGESTION LOOP
                retry_count = 0
                while not is_valid and retry_count < 2:
                    if (
                        "not found" in error_msg.lower()
                        or "unrecognized name" in error_msg.lower()
                    ):
                        advisor_resp = await agent_suggest_schema(
                            intent, sql, error_msg, trace_id, websocket
                        )
                        await websocket.send_json(
                            {
                                "status": "clarification_needed",
                                "message": advisor_resp["message"],
                                "suggestions": advisor_resp["suggestions"],
                                "sql": sql,
                            }
                        )
                        log_to_audit_table(
                            trace_id,
                            intent,
                            "PAUSED",
                            {"action": "Waiting for schema clarification"},
                            user_email,
                        )
                        break

                    sql = await agent_heal_sql(
                        intent, sql, error_msg, trace_id, websocket
                    )
                    is_valid, error_msg, cost, row_count, schema_fields = (
                        await sandbox_dry_run_and_count(sql, trace_id, websocket)
                    )
                    retry_count += 1

                if not is_valid and "not found" not in error_msg.lower():
                    raise Exception(
                        f"Failed to validate SQL after {retry_count} attempts. BQ Error: {error_msg}"
                    )
                elif not is_valid:
                    continue

                # ---------------------------------------------------------
                # COST / ROW-LIMIT GUARDRAILS
                # ---------------------------------------------------------
                if row_count > 2000 or cost > 0.05:
                    warning_msg = f"⚠️ **High Volume Query Detected.**\nThis query will return **{row_count:,} records** and process data costing approximately **${cost:.4f}**.\nDo you want to proceed?"
                    await websocket.send_json(
                        {
                            "status": "confirmation_needed",
                            "message": warning_msg,
                            "sql": sql,
                            "intent_type": intent_type,
                        }
                    )
                    continue

                # ---------------------------------------------------------
                # EXECUTE INTENT
                # ---------------------------------------------------------
                if intent_type == "EXPLORE":
                    results_dict = await execute_and_format_results(
                        sql, trace_id, websocket
                    )
                    await websocket.send_json(
                        {
                            "status": "explore_result",
                            "message": "✅ Query executed safely. Here are your insights and charts:",
                            "html": results_dict["html"],
                            "raw_data": results_dict["raw_data"],
                            "sql": sql,
                        }
                    )
                    log_to_audit_table(
                        trace_id,
                        intent,
                        "SUCCESS",
                        {"action": "Data Explored"},
                        user_email,
                    )

                elif intent_type == "GET_SQL":
                    await websocket.send_json(
                        {
                            "status": "sql_only",
                            "message": "✅ Here is the validated SQL you requested. You can execute this directly in the BigQuery Console to build your table.",
                            "sql": sql,
                        }
                    )
                    log_to_audit_table(
                        trace_id,
                        intent,
                        "SUCCESS",
                        {"action": "SQL File Provided"},
                        user_email,
                    )

            except Exception as inner_e:
                error_str = str(inner_e)
                await stream_log(
                    websocket,
                    "ERROR",
                    f"☠️ [System Halt] Exception: {error_str}",
                    trace_id,
                    is_error=True,
                )
                log_to_audit_table(
                    trace_id, intent, "FAILED", {"error": error_str}, user_email
                )

    except WebSocketDisconnect:
        logger.info(f"[{trace_id}] Client disconnected.")
