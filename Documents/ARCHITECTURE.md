# Architecture — Sentinel

Authoritative technical reference for the Sentinel platform. Covers design philosophy,
component internals, data flows, agent architecture, deployment pipeline, and key engineering decisions.

> All infrastructure and Dataform deployments happen exclusively through GitHub Actions workflows.
> Nothing is created or modified via the GCP Console or CLI.

---

## Table of Contents

1. [Design Philosophy](#1-design-philosophy)
2. [System Overview](#2-system-overview)
3. [Component: sentinel-ingestor](#3-component-sentinel-ingestor)
4. [Component: sentinel-forge Agent Swarm](#4-component-sentinel-forge-agent-swarm)
5. [Agent Deep-Dives](#5-agent-deep-dives)
6. [The Dataform Layer](#6-the-dataform-layer)
7. [The Terraform Layer](#7-the-terraform-layer)
8. [Deployment: GitHub Actions CI/CD](#8-deployment-github-actions-cicd)
9. [GitOps and GitHub App Authentication](#9-gitops-and-github-app-authentication)
10. [Resilience and Quota Protection](#10-resilience-and-quota-protection)
11. [Data Contract Specification](#11-data-contract-specification)
12. [Metadata Table Schemas](#12-metadata-table-schemas)
13. [Design Decisions and Trade-offs](#13-design-decisions-and-trade-offs)

---

## 1. Design Philosophy

**Intent over Implementation.** Engineers define what data means and where it belongs.
The platform synthesizes how it gets there. Data Contracts replace hand-coded ETL.

**Maker / Checker by default.** No agent output reaches production without independent review.
Every synthesis agent is paired with a dedicated QA agent. This is structural, not optional.

**Pipeline-first deployment.** No human touches the GCP Console or CLI to deploy infrastructure
or pipelines. Every change flows through a Pull Request and a GitHub Actions workflow.
The merge is the deployment trigger.

**Humans at the merge gate only.** The platform maximises autonomous throughput while preserving
one human decision point: the Pull Request merge. Everything before that is machine-managed.

---

## 2. System Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                      GCS LANDING ZONE                            │
│                Raw CSV / JSON / Parquet files                    │
└──────────────────────────────┬───────────────────────────────────┘
                               │ google.storage.object.finalize
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                    sentinel-ingestor                             │
│               Cloud Function · Python 3.10                       │
│                                                                  │
│  1. Match file against ingestion_master routing rules            │
│  2. Validate incoming schema vs live BigQuery table              │
│  3a. MATCH  → load to BigQuery · write to ingestion_log          │
│  3b. DRIFT  → quarantine to archive bucket                       │
│            → publish drift payload to Pub/Sub                    │
│            → write DRIFT_DETECTED to ingestion_log               │
└──────────────────────────────┬───────────────────────────────────┘
                               │ Pub/Sub: schema-drift topic
                               │ Payload: {table, domain, sample_rows,
                               │           schema_diff, source_path}
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                      sentinel-forge                              │
│               Cloud Function · Python 3.10                       │
│               Vertex AI Gemini 2.5 Agent Swarm                   │
│                                                                  │
│  Contract Engine → Scanners → Architects → QA → GitOps Bot      │
│  → writes outcome to ai_ops_log                                  │
└──────────────────────────────┬───────────────────────────────────┘
                               │ GitHub App API
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                    GitHub Pull Request                           │
│              Authored by sentinel-forge[bot]                     │
│                                                                  │
│  Files committed:                                                │
│  ├── data_contracts/<domain>/<table>.yml                         │
│  ├── infra/bigquery/tables/json/<table>.json                     │
│  ├── infra/bigquery/tables/<domain>.tf   (injected)              │
│  └── definitions/<layer>/<domain>/<entity>/<table>.sqlx          │
└──────────────────────────────┬───────────────────────────────────┘
                               │ Human reviews and merges
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                  GitHub Actions CI/CD Pipeline                   │
│              Triggered automatically on merge to master          │
│                                                                  │
│  · Dataform compile check (on every PR)                          │
│  · Dataform run → deploys models to BigQuery (on merge)          │
│  · Terraform plan/apply → deploys infrastructure (on merge)      │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. Component: `sentinel-ingestor`

### Trigger
`google.storage.object.finalize` on the GCS landing bucket.

### Routing
Queries `ingestion_master` matching the uploaded filename against `file_pattern`. The matched
rule provides `target_dataset`, `target_table`, `domain`, `file_format`, `delimiter`,
`quote_char`, `skip_header_rows`, and `write_disposition`.

### Schema Validation
1. Read sample rows from the incoming file
2. Introspect column names and types
3. Fetch the live BigQuery table schema via the API
4. Compute diff: added columns, removed columns, type changes

### Decision Logic

```
schema_diff == empty AND table exists
    → LOAD:      load to BigQuery using write_disposition from ingestion_master
    → LOG:       write SUCCESS record to ingestion_log
                 (ingestion_id, file_name, file_uri, status, row_count,
                  start_time, end_time, domain, target_table,
                  total_records, processed_records, good_records, bad_records)

schema_diff != empty OR table does not exist
    → QUARANTINE: move file to archive bucket
    → PUBLISH:    send drift payload to Pub/Sub
    → LOG:        write FAILED/SKIPPED to ingestion_log
```

### Pub/Sub Drift Payload

```json
{
  "table_name":      "crm_contacts",
  "domain":          "crm",
  "target_dataset":  "sentinel_raw_landing",
  "schema_diff": {
    "added":   [{"name": "opt_in_flag", "type": "STRING"}],
    "removed": [],
    "changed": [{"name": "phone", "from": "STRING", "to": "INTEGER"}]
  },
  "sample_rows":     [...],
  "source_gcs_path": "gs://sentinel-landing/crm/crm_contacts_20260322.csv",
  "event_timestamp": "2026-03-22T10:45:00Z"
}
```

---

## 4. Component: `sentinel-forge` Agent Swarm

### Trigger
Pub/Sub push subscription on the `schema-drift` topic.

### Orchestration Flow

```
Pub/Sub message received
    │
    ├─► Contract Engine          → fetch or generate YAML contract from GCS
    │       └─► inject rules into downstream agent context
    │
    ├─► Scanner                  → read GitHub repo: domains, datasets, SQLX styles
    │       └─► Dataset Analyst  → map table to correct raw/staging/mart datasets
    │
    ├─► [PARALLEL SYNTHESIS]
    │       ├─► Schema Designer  → BigQuery JSON schema
    │       ├─► TF Architect     → Terraform HCL (adaptive locals injection)
    │       └─► DF Architect     → Dataform SQLX (source + staging + mart)
    │
    ├─► [PARALLEL QA REVIEW]
    │       ├─► Schema QA Gatekeeper
    │       ├─► TF QA Gatekeeper
    │       └─► DF QA Gatekeeper
    │
    └─► GitOps Bot               → branch, commit all artifacts, open PR
            └─► write to ai_ops_log
                (operation_id, timestamp, ai_agent_id, resource_group,
                 resource_type, resource_id, action_type, change_payload,
                 outcome_status, reference_link)
```

### Model Tier Routing

| Agent | Tier | Model |
|---|---|---|
| Contract Engine (generate) | Heavy | `gemini-2.5-flash` |
| Contract Engine (fetch/validate) | Lite | `gemini-2.5-flash-lite` |
| Scanner / Dataset Analyst | Lite | `gemini-2.5-flash-lite` |
| Schema Designer | Lite | `gemini-2.5-flash-lite` |
| TF Architect | Heavy | `gemini-2.5-flash` |
| DF Architect | Heavy | `gemini-2.5-flash` |
| QA Gatekeepers ×3 | Lite | `gemini-2.5-flash-lite` |
| GitOps Bot | — | GitHub API only |

---

## 5. Agent Deep-Dives

### 📜 Contract Engine

**Fetch path:** Checks `gs://sentinel-function-code/data_contracts/<domain>/<table>.yml`.
If found, parses and injects type mappings and assertions into downstream agent context.

**Generate path:** Uses `gemini-2.5-flash` with the sample payload to produce a YAML contract
defining explicit BigQuery native types, assertions, and SLOs.

```yaml
version: "1.0"
table: crm_contacts
domain: crm
owner: data-platform-team
freshness_slo_hours: 24
completeness_threshold: 0.99
columns:
  - name: contact_id
    bq_type: STRING
    nullable: false
    assertions: [not_null, unique]
  - name: created_at
    bq_type: TIMESTAMP
    nullable: false
    assertions: [not_null]
```

---

### 🕵️ Scanners and Dataset Analyst

**Scanner:** Reads the repository via GitHub Contents API to map active domain folders,
existing `target_dataset` names in SQLX configs, and code style patterns.

**Dataset Analyst:** Uses the domain from the Pub/Sub payload to determine which raw, staging,
and mart datasets the table belongs to — preventing agents from hallucinating new dataset names.

---

### 🧩 Schema Designer

**STRING-first design:** All raw columns are typed as `STRING` regardless of inferred source type.
Type coercion happens in staging. A STRING never causes a raw load failure; a type mismatch can.

Every column gets a `defaultValueExpression` and `mode: NULLABLE` at the raw layer.

```json
[
  {
    "name": "contact_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Unique identifier for the CRM contact",
    "defaultValueExpression": "'UNKNOWN'"
  }
]
```

---

### 🏗️ Terraform Architect

**Adaptive injection:** Reads the existing `.tf` file for the target domain via GitHub API,
identifies the `locals` map structure (e.g., `tables_raw_crm = { ... }`), and injects only
the new map entry — not a full resource block. Prevents duplicate `resource` blocks.

Also provisions a `_hist` archive entry for every new table, reusing the same JSON schema
file (`file("json/<table>.json")`) — enforcing DRY.

**TF QA enforcements:**
- No standalone `resource "google_bigquery_table"` blocks outside a `for_each`
- `deletion_protection = true` on all tables
- `_hist` entry present for every raw table
- Schema reference uses `file()` not inline JSON
- No hardcoded project IDs — must use `var.project_id`

---

### 🪄 Dataform Architect

**Folder routing:**
```
definitions/sources/<domain>/src_<table>.sqlx
definitions/staging/<domain>/<entity>/stg_<table>.sqlx
definitions/marts/<domain>/<entity>/fct_<table>.sqlx
```

**Dynamic casting:** Every column from the Data Contract gets the correct safe cast:
- `SAFE_CAST(col AS STRING)`
- `SAFE.PARSE_DATE('%Y-%m-%d', col)`
- `SAFE_CAST(col AS FLOAT64)`

`SAFE_CAST` is always used over bare `CAST` — a failed `SAFE_CAST` returns NULL rather
than crashing the pipeline.

**Staging incremental pattern:**
```sql
config {
  type: "incremental",
  schema: "sentinel_staging",
  tags: ["crm", "contacts", "staging"],
  bigquery: {
    partitionBy: "batch_date",
    clusterBy: ["contact_id"]
  },
  assertions: {
    nonNull: ["contact_id", "batch_date"],
    uniqueKey: ["contact_id", "batch_date"]
  }
}

SELECT
  SAFE_CAST(contact_id AS STRING)           AS contact_id,
  SAFE_CAST(email AS STRING)                AS email,
  SAFE.PARSE_DATE('%Y-%m-%d', created_date) AS created_date,
  CURRENT_DATE()                            AS batch_date
FROM ${ref("src_crm_contacts")}
${ when(incremental(), `WHERE batch_date > (SELECT MAX(batch_date) FROM ${self()})`) }
QUALIFY ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY batch_date DESC) = 1
```

**DF QA enforcements:**
- 3-part tag array `[domain, entity, layer]` on every config block
- No bare `CAST` — only `SAFE_CAST` or `SAFE.PARSE_DATE`
- `QUALIFY ROW_NUMBER()` deduplication present in all staging models
- `CURRENT_DATE() AS batch_date` present
- `type: "declaration"` files contain no `tags` property (illegal in Dataform Core v3)
- `${ when(incremental(), ...) }` present in all incremental models

---

### 🐙 GitOps Bot

**Branch naming:** `sentinel-forge/<table>/<YYYYMMDD-HHMMSS>`

**Commit sequence:**
1. Create branch from `master`
2. Commit contract YAML → `data_contracts/<domain>/<table>.yml`
3. Commit JSON schema → `infra/bigquery/tables/json/<table>.json`
4. Patch Terraform → inject into `locals` map in `infra/bigquery/tables/<domain>.tf`
5. Commit source declaration SQLX
6. Commit staging model SQLX
7. Commit mart model SQLX
8. Open Pull Request — structured description with table, domain, schema diff, artifacts list,
   contract SLOs, and review checklist

**After PR is opened:** writes a record to `ai_ops_log` with `operation_id`, `timestamp`,
`ai_agent_id`, `resource_group`, `resource_type`, `resource_id`, `action_type`,
`change_payload`, `outcome_status`, and `reference_link` (the PR URL).

---

## 6. The Dataform Layer

`definitions/` is the Dataform project. All SQLX follows a strict 3-layer architecture.

```
definitions/
├── sources/          # type: "declaration" — no SQL, no tags
│   └── <domain>/
├── staging/          # incremental, SAFE_CAST, QUALIFY dedup
│   └── <domain>/<entity>/
└── marts/            # incremental, business aggregations from staging
    └── <domain>/<entity>/
```

| Layer | Type | Schema | Tags |
|---|---|---|---|
| sources | `declaration` | `raw_*` | **none** (illegal in Dataform Core v3) |
| staging | `table` (incremental) | `sentinel_staging` | `[domain, entity, staging]` |
| marts | `table` (incremental) | `sentinel_mart` | `[domain, entity, mart]` |

**Dataform project settings (`workflow_settings.yaml`):**
```yaml
defaultProject:          sentinel-486707
defaultDataset:          sentinel_staging
defaultLocation:         us-central1
defaultAssertionDataset: sentinel_assertions
```

---

## 7. The Terraform Layer

```
infra/bigquery/tables/
├── <domain>.tf         # Per-domain resource files using locals + for_each
└── json/
    ├── ingestion_master.json
    ├── ingestion_log.json
    └── ai_ops_log.json
```

**Standard `locals` + `for_each` pattern:**
```hcl
locals {
  tables_raw_crm = {
    crm_contacts = {
      schema    = file("json/crm_contacts.json")
      partition = "ingestion_date"
      cluster   = ["contact_id"]
    }
    crm_contacts_hist = {
      schema    = file("json/crm_contacts.json")   # DRY — same schema file
      partition = "ingestion_date"
      cluster   = ["contact_id"]
    }
  }
}

resource "google_bigquery_table" "raw_crm" {
  for_each            = local.tables_raw_crm
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.raw_crm.dataset_id
  table_id            = each.key
  deletion_protection = true
  schema              = each.value.schema

  time_partitioning {
    type  = "DAY"
    field = each.value.partition
  }

  clustering = each.value.cluster
}
```

The TF Architect injects **only new `locals` map entries**. It never regenerates the resource block.

**All Terraform changes are deployed via GitHub Actions — not `terraform apply` from a local machine.**

---

## 8. Deployment: GitHub Actions CI/CD

All deployments happen exclusively through the pipelines in `.github/workflows/`.
No infrastructure or pipeline changes are applied via the GCP Console or CLI.

### Pull Request: Compile Check

Triggered on every PR targeting `master`.

```
PR opened / updated
    │
    └─► dataform compile
        · Validates all SQLX syntax
        · Validates all ref() resolution
        · Must pass — PR is blocked from merge on failure
```

### Merge to master: Deploy

Triggered automatically when a PR is merged to `master`.

```
Merge to master
    │
    ├─► Dataform deployment
    │   · dataform run (scoped to changed domains via tags)
    │   · Deploys updated models to BigQuery
    │
    └─► Terraform deployment
        · terraform plan → terraform apply
        · Deploys any new or modified BigQuery tables from infra/
```

This means the full lifecycle for every agent-generated PR is:

```
sentinel-forge[bot] opens PR
    → Engineer reviews SQLX, HCL, JSON schema, contract YAML
    → Engineer merges
    → GitHub Actions deploys automatically
    → BigQuery tables and Dataform models are live
```

---

## 9. GitOps and GitHub App Authentication

**Authentication flow:**
```
Cloud Function starts
    │
    ├─► Load GITHUB_APP_ID + GITHUB_INSTALLATION_ID from environment
    ├─► Load RSA private key from Google Secret Manager
    ├─► Generate JWT signed with RSA key (10-minute expiry)
    ├─► POST /app/installations/{id}/access_tokens → 1-hour installation token
    └─► Use token for all GitHub API calls in this execution
```

**Why not PATs:** GitHub App auth provides a verified `sentinel-forge[bot]` identity in the
PR audit trail, scoped permissions (`contents: write`, `pull_requests: write`, `metadata: read`),
and short-lived 1-hour tokens. PATs provide none of these.

---

## 10. Resilience and Quota Protection

### Traffic Smoothing
3-second base pacing between sequential LLM calls. Parallel synthesis calls share the
Vertex AI quota pool and are concurrency-limited.

### Exponential Backoff with Jitter

```python
def call_llm_with_backoff(prompt: str, model: str, max_retries: int = 5) -> str:
    base_delay = 5
    for attempt in range(max_retries):
        try:
            return vertex_ai_generate(prompt, model)
        except ResourceExhausted:
            if attempt == max_retries - 1:
                raise
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            time.sleep(delay)
            # Retry delays: ~5s, ~10s, ~20s, ~40s, ~80s
```

Jitter prevents thundering-herd retry storms when multiple instances hit quota simultaneously.

---

## 11. Data Contract Specification

Stored in GCS at: `gs://sentinel-function-code/data_contracts/<domain>/<table>.yml`

Referenced in `ingestion_master.data_contracts` (e.g., `Sales.yaml`).

```yaml
version: "1.0"
table: <table_name>
domain: <domain>                          # matches ingestion_master.domain
owner: <team>
freshness_slo_hours: <integer>
completeness_threshold: <float 0.0–1.0>

columns:
  - name: <column_name>
    bq_type: STRING | INT64 | FLOAT64 | BOOL | DATE | TIMESTAMP | NUMERIC
    nullable: true | false
    description: <description>
    assertions:
      - not_null
      - unique
      - <custom_sql>
```

**Assertion mapping to Dataform:**

| Contract assertion | Dataform translation |
|---|---|
| `not_null` | `nonNull: ["column"]` in config block |
| `unique` | `uniqueKey: ["column"]` in config block |
| Custom SQL | Standalone `assert` SQLX block |

---

## 12. Metadata Table Schemas

These are the exact BigQuery schemas from `infra/bigquery/tables/json/`.
All three tables are provisioned via the GitHub Actions pipeline.

### `ingestion_master`

| Column | Type | Mode | Description |
|---|---|---|---|
| `file_pattern` | STRING | REQUIRED | Regex to match incoming files (e.g., `^sales_.*\.csv$`) |
| `target_dataset` | STRING | REQUIRED | Destination BigQuery dataset |
| `target_table` | STRING | REQUIRED | Destination table name |
| `domain` | STRING | REQUIRED | Business domain (e.g., `Sales`) |
| `data_contracts` | STRING | NULLABLE | Associated data contract filename (e.g., `Sales.yaml`) |
| `file_format` | STRING | NULLABLE | CSV, JSON, AVRO, PARQUET, XLSX (default: CSV) |
| `delimiter` | STRING | NULLABLE | Field delimiter for CSVs (default: `,`) |
| `quote_char` | STRING | NULLABLE | Quote character for CSVs |
| `skip_header_rows` | INTEGER | NULLABLE | Rows to skip (default: 1) |
| `write_disposition` | STRING | NULLABLE | `WRITE_APPEND` (default) or `WRITE_TRUNCATE` |
| `is_active` | BOOLEAN | REQUIRED | Enable/disable this routing rule |
| `created_at` | TIMESTAMP | NULLABLE | When this rule was created |

### `ingestion_log`

| Column | Type | Mode | Description |
|---|---|---|---|
| `ingestion_id` | STRING | REQUIRED | Unique UUID (Cloud Function Execution ID) |
| `file_name` | STRING | REQUIRED | Name of the file processed |
| `file_uri` | STRING | REQUIRED | Full GCS path (`gs://bucket/file`) |
| `status` | STRING | REQUIRED | `SUCCESS`, `FAILED`, or `SKIPPED` |
| `row_count` | INTEGER | NULLABLE | Rows loaded into BigQuery |
| `error_message` | STRING | NULLABLE | Full error trace if status is `FAILED` |
| `start_time` | TIMESTAMP | REQUIRED | When the function started |
| `end_time` | TIMESTAMP | NULLABLE | When the function finished |
| `domain` | STRING | NULLABLE | Where the data tried to go |
| `target_table` | STRING | NULLABLE | Domain the table belonged to |
| `total_records` | STRING | NULLABLE | Rows present in the source file |
| `processed_records` | STRING | NULLABLE | Rows processed into BigQuery |
| `good_records` | STRING | NULLABLE | Good records loaded into BigQuery |
| `bad_records` | STRING | NULLABLE | Bad records rejected from BigQuery |

### `ai_ops_log`

| Column | Type | Mode | Description |
|---|---|---|---|
| `operation_id` | STRING | REQUIRED | Unique UUID for this AI operation |
| `timestamp` | TIMESTAMP | REQUIRED | When the AI took action |
| `ai_agent_id` | STRING | NULLABLE | Identity of the agent (e.g., `Gemini-Raw-Architect`) |
| `resource_group` | STRING | NULLABLE | Category: Terraform, Dataform, Airflow |
| `resource_type` | STRING | NULLABLE | Type: Table, Schema, Model |
| `resource_id` | STRING | NULLABLE | Full path to the resource |
| `action_type` | STRING | NULLABLE | `CREATE_PR`, `ABORT`, `ERROR` |
| `change_payload` | JSON | NULLABLE | Exact data/diff the AI generated |
| `outcome_status` | STRING | NULLABLE | `SUCCESS` or `FAILED` |
| `reference_link` | STRING | NULLABLE | URL to the GitHub PR or log |

---

## 13. Design Decisions and Trade-offs

**Why STRING-first at the raw layer?**
Type inference from CSV/JSON is unreliable. A column that looks like an integer today may arrive
as a zero-padded string tomorrow. Typing everything as STRING at the raw boundary eliminates an
entire class of ingestion failures. Explicit casting in staging is more verbose but far more reliable.

**Why Pub/Sub between ingestor and forge?**
Decoupling provides retry durability (Pub/Sub at-least-once delivery), back-pressure management,
and independent scalability. A direct function call loses the event on forge failure.

**Why GitHub Actions for all deployments?**
Console or CLI deployments are unaudited, unrepeatable, and unreviewed. GitHub Actions provides
a full audit trail, peer review via PR, and repeatable deployment runs. The PR merge is the
single human approval gate for all infrastructure and pipeline changes.

**Why GitHub App auth over PATs?**
GitHub App auth provides a verified `sentinel-forge[bot]` identity, scoped permissions, and
short-lived tokens. PATs provide none of these.

**Why Maker/Checker rather than a single synthesis agent?**
A single agent asked to generate and review its own output reliably misses its own errors.
Independent QA agents focused on finding problems — not generating solutions — catch significantly
more hallucinations, syntax errors, and constraint violations. The cost (doubled QA LLM calls)
is mitigated by routing QA agents to the Lite model tier.

**Why adaptive Terraform injection over full file regeneration?**
Regenerating a complete `.tf` file for a domain with many existing tables requires perfectly
reproducing all existing resources — high hallucination risk. Injecting only the new `locals`
map entry minimises the error surface and keeps the diff human-reviewable at a glance.

---

*For setup and contribution guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md).*
*For the project overview, see [README.md](/README.md).*