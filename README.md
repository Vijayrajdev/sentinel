# 🤖 Sentinel-Forge: Autonomous DataOps Agent

**Sentinel-Forge** is an enterprise-grade, event-driven Multi-Agent AI system designed to autonomously monitor, self-heal, and scale Google Cloud data pipelines. 

Operating as a virtual Data Engineer, Analytics Engineer, and QA Lead, Sentinel-Forge intercepts pipeline anomalies—specifically schema drifts and missing landing tables—and automatically generates the necessary BigQuery Schemas, Terraform infrastructure (HCL), and Dataform transformations (SQLX). It then peer-reviews its own code and submits a highly detailed Pull Request to your Git repository for final human approval.

---

## ✨ Core Capabilities

* **Autonomous Self-Healing:** Intercepts missing tables and schema drifts in real-time, completely eliminating manual pipeline bottlenecks.
* **Semantic Domain Routing:** Intelligently scans existing Dataform workspaces using fuzzy matching to route new tables (e.g., `orders_raw`) to existing semantic domain folders (e.g., `ecommerce`), maintaining strict repository taxonomy.
* **Zero-Shot History Table Generation:** Automatically provisions 30-day auto-expiring `_hist` BigQuery tables for any new `_raw` landing table by mimicking existing repository patterns via RAG (Retrieval-Augmented Generation).
* **Automated Data Quality:** Dynamically infers and injects Dataform `assertions` (e.g., `uniqueKey`, `nonNull`, `rowConditions`) into SQLX config blocks based on AI schema analysis.
* **Idempotent Operations:** Automatically generates `TRUNCATE` operations post-archival to maintain clean landing zones and optimize BigQuery costs.
* **Enterprise Audit Logging:** Logs every AI thought process, action, and detailed JSON payload to a BigQuery Audit table (`ai_ops_log`) with explicit PR link references.

---

## 🧠 The Multi-Agent Architecture

Sentinel-Forge employs a strict **Planner-Doer-Reviewer** pattern utilizing Vertex AI (Gemini 2.5 Pro). The workflow is parallelized across 7 specialized AI Agents orchestrated by a central gateway.

### 1. 🧩 Schema Designer (Agent 1)
* **Role:** Data Architect.
* **Function:** Generates BigQuery JSON schemas from raw sample data or extracts new columns.
* **Governance:** Strictly enforces `STRING` data types for the raw layer, sets all modes to `NULLABLE`, and seamlessly appends enterprise audit columns (`batch_date`, `processed_dttm`).

### 2. 🏗️ Terraform Architect (Agent 2)
* **Role:** Cloud Infrastructure Engineer.
* **Function:** Generates or patches `google_bigquery_table` Terraform HCL blocks.
* **Mimicry:** Scans the repo for existing `_hist` table configurations and perfectly replicates your enterprise's exact infrastructure style for history tables without hallucinating new variables.

### 3. 🔍 Dataform Scanner (Agent 3)
* **Role:** Repository Context Analyst.
* **Function:** Performs semantic matching to locate existing `.sqlx` files. If `orders_raw` triggers an event, it hunts for variations like `order` in `definitions/marts/` subdirectories to ensure patches are routed correctly and extracts existing SQL styles for RAG.

### 4. 🪄 Dataform Architect (Agent 4)
* **Role:** Analytics Engineer.
* **Function:** Synthesizes Google Cloud Dataform Core v3.x code, explicitly embedding Data Quality assertions.
* **Output:** Generates 4 distinct layers per entity:
    1.  `sources/` (Declaration)
    2.  `staging/` (View with DQ)
    3.  `marts/` (Incremental Fact Table with DQ)
    4.  `operations/` (Archive Insert & Truncate)

### 5. 🕵️‍♀️ Dataform QA Verifier (Agent 5)
* **Role:** Analytics QA Lead.
* **Function:** Peer-reviews Agent 4's code. It validates that 100% of the new columns are explicitly selected, corrects misplaced folder paths, validates `${ref()}` syntax, and confirms Data Quality Assertions are present.

### 6. 🕵️‍♀️ Schema QA Verifier (Agent 6)
* **Role:** Data Governance Lead.
* **Function:** Intercepts the generated JSON schema before commit to ensure absolute compliance with the `STRING`/`NULLABLE` mandate and verifies audit column inclusion.

### 7. 🕵️‍♀️ Terraform QA Verifier (Agent 7)
* **Role:** DevOps QA Lead.
* **Function:** Parses the generated HCL code for missing brackets, quotes, and strictly enforces that a `_hist` table resource is generated whenever a `_raw` table is processed.

---

## 🚀 Event-Driven Trigger Mechanism (Sentinel-Ingestor)

Sentinel-Forge operates dynamically, reacting to live pipeline pain points rather than running on a static schedule. The primary tripwire for this Multi-Agent system is the **Sentinel-Ingestor**.

### The Sentinel-Ingestor Pipeline
The `sentinel-ingestor` is the frontline component of the data platform, responsible for monitoring the Cloud Storage landing zone and executing metadata-driven loads into BigQuery based on `sentinel_audit.ingestion_master`. 

Before loading a file, the Ingestor acts as a schema validation gateway:
1. **Header Extraction:** Reads the header row of the incoming raw file (e.g., CSV).
2. **Schema Comparison:** Compares incoming headers against the existing BigQuery raw table schema. 
3. **Anomaly Detection:** If the target table does not exist, or if the file contains new schema drifts (new columns), the Ingestor safely halts the load for that specific file.
4. **The Trigger:** The Ingestor immediately publishes a structured JSON payload (containing `table_ref`, `new_column_headers`, and `sample_data_rows`) to the `schema_drift_events` Pub/Sub topic. 

This Pub/Sub message wakes up **Sentinel-Forge** to begin the autonomous self-healing process.

---

## 📦 Orchestration & Git Injection

Once the Multi-Agent system is triggered and has successfully generated and peer-reviewed the necessary infrastructure and transformation code, the **Git Injector** finalizes the workflow:

1. **Branching:** Creates a dynamically named, isolated branch from `master` (e.g., `ai/ops-orders_raw-a1b2c3`).
2. **Committing:** Commits the generated JSON schemas, Terraform HCL, and Dataform SQLX files using standard conventional commits (`feat:`, `fix:`).
3. **Pull Request:** Opens a highly detailed Pull Request on GitHub. The PR body is dynamically generated to include Markdown tables outlining the exact schema changes, specific paths modified, and data quality assertions added.

Once the PR is merged by a human engineer, the standard CI/CD pipeline (e.g., Terraform Cloud/GitHub Actions) applies the changes, and the Dataform workflows resume naturally.

---

## 📊 Observability & Telemetry

Sentinel-Forge employs strict "Black Box" observability to ensure absolute transparency into the AI's decision-making process.

* **Cloud Logging:** Every single function entry and exit is logged using strict `▶️` (Start) and `⏹️` (End) indicators. All logs share a unified `trace_id` for perfect chronological debugging in Google Cloud Logging.
* **BigQuery Audit Ledger:** The agent records every successful PR generation or critical system failure into the `sentinel_audit.ai_ops_log` table. The payload includes a highly detailed, nested JSON object (`change_payload`) detailing exactly which columns were added, which files were touched, whether a history table was synced, and the direct URL to the GitHub PR.

---

## 🛠️ Environment Configuration

To deploy this Gen 2 Cloud Function, the following Environment Variables must be configured:

| Variable | Description | Example |
| :--- | :--- | :--- |
| `GCP_PROJECT` | Your Google Cloud Project ID | `sentinel-486707` |
| `GCP_REGION` | Vertex AI & BigQuery Region | `us-central1` |
| `REPO_NAME` | Target GitHub Repository | `Vijayrajdev/sentinel` |
| `GITHUB_TOKEN` | PAT with `repo` & `pull_request` access stored securely | `ghp_...` |
| `AI_AUDIT_TABLE` | BigQuery destination for AI operations logs | `sentinel_audit.ai_ops_log` |
| `SCHEMA_BASE_PATH` | Repository path for Schema JSON definitions | `infra/bigquery/tables/json/` |
| `TF_BASE_PATH` | Repository path for Terraform HCL configurations | `infra/bigquery/tables/` |

## 📄 `requirements.txt`

```text
functions-framework==3.8.1
google-cloud-vertexai==1.71.1
PyGithub==2.3.0
google-cloud-bigquery==3.20.1
google-cloud-storage==2.9.0
google-cloud-aiplatform>=1.38.0
google-cloud-pubsub==2.18.0
pandas>=2.1.0
numpy<2.0.0
gcsfs>=2023.9.2
pyarrow>=13.0.0
openpyxl>=3.1.2
db-dtypes>=1.2.0
```
*Built with ❤️ for the Sentinel Data Platform.*