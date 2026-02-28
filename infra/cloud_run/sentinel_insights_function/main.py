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

AI_MODEL_HEAVY_NAME = os.environ.get("AI_MODEL_HEAVY", "gemini-2.5-pro")
AI_MODEL_LITE_NAME = os.environ.get("AI_MODEL_LITE", "gemini-2.5-flash")

COST_PER_TB = 6.25
DEFAULT_DATASET = "sentinel_marts"

bq_client: Optional[bigquery.Client] = None

try:
    if PROJECT_ID:
        bq_client = bigquery.Client(project=PROJECT_ID)
        vertexai.init(project=PROJECT_ID, location=REGION)
        logger.info(
            f"✅ [Init] Vertex AI & BigQuery Clients Online. Default Engine: {AI_MODEL_HEAVY_NAME}"
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
    time.sleep(3)


def generate_content_with_retry(
    model_instance, prompt: str, trace_id: str, max_retries: int = 5
):
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
                    raise e
                sleep_time = (base_delay * (2**attempt)) + random.uniform(0, 2)
                time.sleep(sleep_time)
            else:
                raise e


def prune_markdown(text: str, lang: str = "sql") -> str:
    if text.startswith("```"):
        try:
            return text.split("\n", 1)[1].rsplit("\n", 1)[0]
        except IndexError:
            return text.replace(f"```{lang}", "").replace("```", "")
    return text


def log_to_audit_table(
    trace_id: str, intent: str, status: str, event_log: dict, user_email: str
):
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
# 🔍 ZERO-HALLUCINATION SCHEMA LOOKUP
# ==============================================================================
async def fetch_real_schema(dataset: str = DEFAULT_DATASET) -> str:
    """Queries BigQuery INFORMATION_SCHEMA so AI doesn't hallucinate tables/columns."""
    try:
        query = f"SELECT table_name, column_name FROM `{PROJECT_ID}.{dataset}.INFORMATION_SCHEMA.COLUMNS`"
        job = await asyncio.to_thread(bq_client.query, query)
        results = await asyncio.to_thread(job.result)

        schema_map = {}
        for row in results:
            if row.table_name not in schema_map:
                schema_map[row.table_name] = []
            if len(schema_map[row.table_name]) < 20:
                schema_map[row.table_name].append(row.column_name)

        return json.dumps(schema_map)
    except Exception as e:
        logger.warning(f"Schema Fetch Failed (Fallback to standard logic): {e}")
        return "{}"


# ==============================================================================
# 🧠 INTENT ROUTER & EXECUTION
# ==============================================================================
async def agent_classify_intent(
    intent: str, trace_id: str, ws: WebSocket, active_model
) -> str:
    await stream_log(
        ws,
        "INFO",
        f"▶️ 🧠 [Intent Router] Analyzing cognitive intent via {active_model._model_name}...",
        trace_id,
    )
    prompt = f"""
    Analyze the user's intent: "{intent}"
    Classify into exactly one of these categories:
    - GREETING: If they say hi, hello, help, or ask what you can do.
    - GET_SQL: ONLY if they EXPLICITLY demand "Give me the SQL", "Show me the query code", or "SQL file".
    - EXPLORE: If they are asking for data, insights, charts, or asking business questions (e.g., "Show me completed orders"). This is the default.
    Output ONLY the exact category word.
    """
    category = await asyncio.to_thread(
        generate_content_with_retry, active_model, prompt, trace_id
    )
    category = category.strip().upper()
    await stream_log(
        ws, "INFO", f"⏹️ 🧠 [Intent Router] Intent classified as: {category}", trace_id
    )
    return category if category in ["GREETING", "GET_SQL", "EXPLORE"] else "EXPLORE"


async def execute_and_format_results(sql: str, trace_id: str, ws: WebSocket) -> dict:
    await stream_log(
        ws,
        "INFO",
        "▶️ 📊 [Execution Engine] Querying BigQuery engine and formatting results...",
        trace_id,
    )
    try:
        safe_sql = f"SELECT * FROM ({sql}) LIMIT 2000"
        query_job = await asyncio.to_thread(bq_client.query, safe_sql)
        results = list(await asyncio.to_thread(query_job.result))

        html = "<div class='apple-glass overflow-x-auto w-full mt-4 rounded-2xl shadow-xl max-h-96 custom-scrollbar text-slate-800 dark:text-slate-200'>"
        html += "<table class='w-full text-left text-sm border-collapse'>"

        if not results:
            return {
                "html": "<div class='text-sm italic mt-2 opacity-70 text-slate-800 dark:text-slate-200'>Query returned zero results.</div>",
                "raw_data": [],
            }

        headers = [field.name for field in query_job.schema]
        html += "<thead class='bg-white/20 dark:bg-black/40 font-bold sticky top-0 backdrop-blur-xl z-10 text-emerald-700 dark:text-emerald-400 border-b border-black/10 dark:border-white/10'><tr>"
        for header in headers:
            html += f"<th class='px-4 py-3 tracking-wide'>{header}</th>"
        html += "</tr></thead><tbody>"

        raw_data = []
        for row in results:
            row_dict = dict(row.items())
            raw_data.append(row_dict)
            html += "<tr class='hover:bg-white/40 dark:hover:bg-white/10 transition-colors duration-300'>"
            for val in row:
                html += f"<td class='px-4 py-3 border-b border-black/5 dark:border-white/5 whitespace-nowrap'>{str(val)}</td>"
            html += "</tr>"

        html += "</tbody></table></div>"

        await stream_log(
            ws,
            "INFO",
            "⏹️ 📊 [Execution Engine] Results successfully fetched and formatted.",
            trace_id,
        )
        return {"html": html, "raw_data": raw_data}
    except Exception as e:
        raise Exception(f"Failed to fetch results: {e}")


# ==============================================================================
# 🧠 THE AGENT SWARM FUNCTIONS
# ==============================================================================
async def agent_draft_sql(
    intent: str, chat_history: List[Dict], trace_id: str, ws: WebSocket, active_model
) -> str:
    await stream_log(
        ws,
        "INFO",
        f"▶️ 🧠 [SQL Architect] Initiating semantic translation using {active_model._model_name}...",
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
    
    Task: Write a highly optimized BigQuery Standard SQL SELECT statement.
    
    CRITICAL INTELLIGENCE & ROUTING RULES:
    1. DEFAULT DATASET: You MUST pull tables from the `{DEFAULT_DATASET}` dataset (e.g., `{DEFAULT_DATASET}.orders`).
    2. EXPLICIT OVERRIDES: IF the user writes `dataset:custom_name` or `table:custom_name`, you MUST override the default and strictly query the requested dataset/table.
    3. SEMANTIC STATUS HEALING: Users speak in natural language. If they ask for "completed orders", "failed transactions", or "active users", DO NOT assume the database contains exactly that word. Use `REGEXP_CONTAINS(LOWER(status_column), r'complete|done|success')` or `LOWER(status_column) IN ('complete', 'completed', 'done', 'shipped')` to catch all semantic variations.
    
    CRITICAL CHART-READY FORMATTING:
    To ensure seamless UI chart generation, you MUST structure your SELECT statement as follows whenever aggregating data:
    - Column 1 MUST be the Primary Categorical Dimension (e.g., explicit STRING like `SAFE_CAST(region AS STRING) AS region_name`).
    - Column 2 MUST be the Primary Numerical Metric (e.g., explicitly cast as FLOAT64/INT64 like `SAFE_CAST(SUM(amount) AS FLOAT64) AS total_sales`).
    
    Output ONLY the raw SQL code. No markdown formatting. No conversational text.
    """
    sql = await asyncio.to_thread(
        generate_content_with_retry, active_model, prompt, trace_id
    )
    sql = prune_markdown(sql, "sql")
    await stream_log(
        ws,
        "INFO",
        "⏹️ 🧠 [SQL Architect] Successfully drafted chart-optimized SQL logic.",
        trace_id,
    )
    return sql


async def sandbox_dry_run_and_count(
    sql: str, trace_id: str, ws: WebSocket
) -> Tuple[bool, str, float, int, list]:
    await stream_log(
        ws,
        "INFO",
        "▶️ 🧪 [Sandbox Validator] Initiating BigQuery Dry-Run and Cost Analysis...",
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
            f"⏹️ 🧪 [Sandbox Validator] Dry-run passed. Bytes processed: {bytes_processed}.",
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
            f"⚠️ 🧪 [Sandbox Validator] FAILED: Target anomaly detected -> {error_msg}",
            trace_id,
        )
        return False, error_msg, 0.0, 0, []


async def agent_suggest_schema(
    intent: str,
    bad_sql: str,
    error_msg: str,
    trace_id: str,
    ws: WebSocket,
    active_model,
) -> dict:
    await stream_log(
        ws,
        "INFO",
        f"▶️ 🔎 [Schema Advisor] Scanning INFORMATION_SCHEMA for true mapping...",
        trace_id,
    )

    real_schema_json = await fetch_real_schema(DEFAULT_DATASET)

    prompt = f"""
    A user's query failed because a table or column does not exist.
    Intent: "{intent}"
    Failed SQL: {bad_sql}
    Error Message: {error_msg}
    
    CRITICAL: Here is the ACTUAL database schema for the default dataset:
    {real_schema_json}
    
    Task: Look at the actual schema provided above. Match what the user asked for with the REAL tables/columns that exist.
    
    Output strictly a JSON object with this structure:
    {{
        "message": "It looks like the table/column was not found. Based on the database, did you mean one of these?",
        "suggestions": ["suggestion_1", "suggestion_2", "suggestion_3"]
    }}
    """
    response = await asyncio.to_thread(
        generate_content_with_retry, active_model, prompt, trace_id
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
    intent: str,
    bad_sql: str,
    error_msg: str,
    trace_id: str,
    ws: WebSocket,
    active_model,
) -> str:
    await stream_log(
        ws,
        "INFO",
        f"▶️ 🚑 [SQL Healer] Attempting autonomous SQL syntax repair via {active_model._model_name}...",
        trace_id,
    )
    prompt = f"""
    You are an expert BigQuery Data Engineer. Your previous SQL failed a dry-run.
    Business Intent: "{intent}"
    Failed SQL: {bad_sql}
    BigQuery Error: {error_msg}
    Task: Fix the SQL query so it successfully executes. Ensure it remains chart-optimized (Categorical First, Metric Second). Output ONLY the raw SQL code. No markdown.
    """
    healed_sql = await asyncio.to_thread(
        generate_content_with_retry, active_model, prompt, trace_id
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
    raw_email = headers.get(
        "x-goog-authenticated-user-email", "business.user@sentinel.ai"
    )
    user_email = raw_email.split(":")[-1] if ":" in raw_email else raw_email

    clean_name = user_email.split("@")[0].replace(".", " ").replace("_", " ")
    user_name = (
        " ".join([word.capitalize() for word in clean_name.split()])
        if clean_name
        else "User"
    )

    await websocket.send_json(
        {
            "status": "init_info",
            "project_id": PROJECT_ID or "sentinel-local-dev",
            "dataset": DEFAULT_DATASET,
        }
    )

    greeting_msg = (
        f"Hello **{user_name}**! 👋 I am your autonomous Sentinel-Insight AI Analyst.\n\n"
        "**My God Level Capabilities:**\n"
        "✨ **Smart Insights & Seamless Charting:** Ask me for business metrics (e.g. 'Show me completed orders by region'). I understand database variations natively, write optimized SQL, and provide instant buttons to generate beautiful Bar, Line, or Pie charts.\n"
        "🎯 **True Schema Routing:** If you misspell a table, I query the real `INFORMATION_SCHEMA` to suggest precise corrections.\n"
        "📄 **SQL Generation:** Just ask 'give me the sql' at any time to extract the raw BigQuery code for your own pipelines.\n\n"
        "How can I assist your data journey today?"
    )
    await websocket.send_json({"status": "greeting", "message": greeting_msg})

    try:
        while True:
            data = await websocket.receive_text()
            payload = json.loads(data)

            chosen_model_name = payload.get("ai_model", AI_MODEL_HEAVY_NAME)
            active_model = GenerativeModel(chosen_model_name)
            active_model_lite = GenerativeModel(AI_MODEL_LITE_NAME)

            action_type = payload.get("action", None)
            intent = payload.get("intent", "").strip()
            chat_history = payload.get("history", [])
            target_sql = payload.get("sql", None)

            if not action_type or action_type not in [
                "CONFIRM_EXPLORE",
                "CONFIRM_GET_SQL",
            ]:
                logger.info(
                    f"[{trace_id}] Received Intent: {intent} from {user_email} via {chosen_model_name}"
                )
                log_to_audit_table(
                    trace_id,
                    intent,
                    "STARTED",
                    {"action": "User submitted query", "model": chosen_model_name},
                    user_email,
                )

            try:
                # ---------------------------------------------------------
                # EXPLICIT CONFIRMATION LOGIC
                # ---------------------------------------------------------
                if action_type == "CONFIRM_EXPLORE" and target_sql:
                    results_dict = await execute_and_format_results(
                        target_sql, trace_id, websocket
                    )
                    await websocket.send_json(
                        {
                            "status": "explore_result",
                            "message": "✅ Heavy query execution confirmed. Select visualization below:",
                            "html": results_dict["html"],
                            "raw_data": results_dict["raw_data"],
                            "sql": target_sql,
                            "trace_id": trace_id,
                        }
                    )
                    log_to_audit_table(
                        trace_id,
                        "CONFIRM_EXPLORE",
                        "SUCCESS",
                        {"action": "Heavy Data Explored", "model": chosen_model_name},
                        user_email,
                    )
                    continue

                elif action_type == "CONFIRM_GET_SQL" and target_sql:
                    await websocket.send_json(
                        {
                            "status": "sql_only",
                            "message": "✅ Here is the validated SQL you requested.",
                            "sql": target_sql,
                        }
                    )
                    log_to_audit_table(
                        trace_id,
                        "CONFIRM_GET_SQL",
                        "SUCCESS",
                        {"action": "SQL File Provided", "model": chosen_model_name},
                        user_email,
                    )
                    continue

                # ---------------------------------------------------------
                # STANDARD INTELLIGENCE LOOP
                # ---------------------------------------------------------
                intent_type = await agent_classify_intent(
                    intent, trace_id, websocket, active_model_lite
                )

                if intent_type == "GREETING":
                    await websocket.send_json(
                        {"status": "greeting", "message": greeting_msg}
                    )
                    continue

                sql = await agent_draft_sql(
                    intent, chat_history, trace_id, websocket, active_model
                )
                is_valid, error_msg, cost, row_count, schema_fields = (
                    await sandbox_dry_run_and_count(sql, trace_id, websocket)
                )

                retry_count = 0
                while not is_valid and retry_count < 2:
                    if (
                        "not found" in error_msg.lower()
                        or "unrecognized name" in error_msg.lower()
                    ):
                        advisor_resp = await agent_suggest_schema(
                            intent,
                            sql,
                            error_msg,
                            trace_id,
                            websocket,
                            active_model_lite,
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
                            {
                                "action": "Waiting for schema clarification",
                                "model": chosen_model_name,
                            },
                            user_email,
                        )
                        break

                    sql = await agent_heal_sql(
                        intent, sql, error_msg, trace_id, websocket, active_model
                    )
                    is_valid, error_msg, cost, row_count, schema_fields = (
                        await sandbox_dry_run_and_count(sql, trace_id, websocket)
                    )
                    retry_count += 1

                if not is_valid and "not found" not in error_msg.lower():
                    raise Exception(f"Failed to validate SQL: {error_msg}")
                elif not is_valid:
                    continue

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

                if intent_type == "EXPLORE":
                    results_dict = await execute_and_format_results(
                        sql, trace_id, websocket
                    )
                    await websocket.send_json(
                        {
                            "status": "explore_result",
                            "message": f"✅ Insights successfully generated via **{chosen_model_name}**. Select visualization below:",
                            "html": results_dict["html"],
                            "raw_data": results_dict["raw_data"],
                            "sql": sql,
                            "trace_id": trace_id,
                        }
                    )
                    log_to_audit_table(
                        trace_id,
                        intent,
                        "SUCCESS",
                        {"action": "Data Explored", "model": chosen_model_name},
                        user_email,
                    )

                elif intent_type == "GET_SQL":
                    await websocket.send_json(
                        {
                            "status": "sql_only",
                            "message": f"✅ Here is the validated SQL generated via **{chosen_model_name}**.",
                            "sql": sql,
                        }
                    )
                    log_to_audit_table(
                        trace_id,
                        intent,
                        "SUCCESS",
                        {"action": "SQL File Provided", "model": chosen_model_name},
                        user_email,
                    )

            except Exception as inner_e:
                await stream_log(
                    websocket,
                    "ERROR",
                    f"☠️ [System Halt] Exception: {str(inner_e)}",
                    trace_id,
                    is_error=True,
                )
                log_to_audit_table(
                    trace_id,
                    intent,
                    "FAILED",
                    {"error": str(inner_e), "model": chosen_model_name},
                    user_email,
                )

    except WebSocketDisconnect:
        logger.info(f"[{trace_id}] Client disconnected.")
