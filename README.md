# 🛡️ Sentinel: Autonomous Agentic Data Engineering

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Dataform](https://img.shields.io/badge/Dataform-Core_3.0.47-6B4FBB?style=flat-square)](https://cloud.google.com/dataform)
[![Terraform](https://img.shields.io/badge/Terraform-HCL-7B42BC?style=flat-square&logo=terraform&logoColor=white)](https://terraform.io)
[![Vertex AI](https://img.shields.io/badge/Vertex_AI-Gemini_2.5-FF6F00?style=flat-square&logo=google-cloud&logoColor=white)](https://cloud.google.com/vertex-ai)
[![BigQuery](https://img.shields.io/badge/BigQuery-Data_Warehouse-4285F4?style=flat-square&logo=google-cloud&logoColor=white)](https://cloud.google.com/bigquery)
[![License](https://img.shields.io/badge/License-Apache_2.0-green?style=flat-square)](LICENSE)

> *"Stop writing pipelines. Start defining intent."*
>
> In 2026, if you are still manually mapping `source_field_a` to `target_field_b`,
> you aren't an engineer — you're a human compiler.
> The era of Agentic Data Engineering is here.

---

## What Is Sentinel?

**Sentinel** is a fully autonomous, self-healing data engineering platform built on Google Cloud. When upstream schemas drift or new datasets arrive, a swarm of specialized AI agents analyzes the payload, synthesizes Terraform HCL infrastructure, generates Dataform `.sqlx` pipelines with dynamic type casting, and opens a validated GitHub Pull Request — with zero manual intervention.

The role of the Senior Data Engineer has officially moved from **Plumber** to **Agent Controller**.

---

## The Problem It Solves

| Pain Point | What Happens Today | What Sentinel Does |
|---|---|---|
| Schema drift | Manual BigQuery + Terraform + Dataform triage. Hours of MTTR. | Detects, quarantines, synthesizes all three, opens a PR. Minutes. |
| New table onboarding | Weeks of boilerplate staging/mart models per source. | Reverse-engineers contracts, generates full pipeline stack autonomously. |
| Human bottleneck | Engineers in the critical path for every infrastructure change. | Agents handle synthesis + QA. Humans review at the merge gate only. |

---

## Repository Structure

```
sentinel/
├── .github/
│   └── workflows/                  # GitHub Actions — all deployments via pipeline
│
├── definitions/                    # Dataform SQLX definitions
│   ├── sources/                    # type: "declaration" — raw layer source tables
│   ├── staging/                    # stg_* models — cleansing, casting, deduplication
│   └── marts/                      # fct_* / dim_* models — business metric layer
│
├── infra/
│   └── bigquery/
│       └── tables/
│           ├── *.tf                # Per-domain Terraform resource files
│           └── json/               # BigQuery JSON schema files
│               ├── ingestion_master.json
│               ├── ingestion_log.json
│               └── ai_ops_log.json
│
├── workflow_settings.yaml          # Dataform project configuration
├── package.json                    # @dataform/core 3.0.47
├── ARCHITECTURE.md
├── CONTRIBUTING.md
└── README.md
```

**Language breakdown:** Python 68.2% · HCL 15.8% · HTML 15.9% · Dockerfile 0.1%

> **Note:** The two AI microservices (`sentinel-ingestor` and `sentinel-forge`) are deployed as
> Google Cloud Functions and are not stored in this repository. This repo is the **target data platform**
> that those services write to via Pull Requests.
>
> **All infrastructure and pipeline deployments happen exclusively through GitHub Actions workflows.**
> Nothing is created or modified via the GCP Console.

---

## System Architecture

Two decoupled microservices communicate over a Pub/Sub event mesh. Every change to this repository
is deployed via the CI/CD pipeline in `.github/workflows/`.

```mermaid
   %%{init: {'theme': 'base', 'themeVariables': { 'edgeLabelBackground': '#FFFFFF', 'lineColor': '#B0BEC5', 'textColor': '#2C3E50' }}}%%
graph TD
    %% Global Link Styling (Removed color hex to prevent parser crash)
    linkStyle default stroke:#B0BEC5,stroke-width:2px;

    %% Node Definitions
    A[🪣 GCS Landing Bucket<br>Raw CSV/JSON/Parquet]:::storage
    B(🛡️ Sentinel Ingestor<br>Cloud Function / Python):::compute
    C[(🏛️ BigQuery Raw Tables<br>Data Warehouse)]:::database
    D{📬 Pub/Sub Drift Topic<br>Event Mesh}:::pubsub
    
    E((🧠 Sentinel-Forge AI Swarm<br>Vertex AI Gemini 2.5)):::ai_brain

    %% The Contract Agent
    X[📜 Agent: Contract Engine<br>YAML Fetch/Generate]:::contract

    %% The Makers
    F[🧩 Agent: Schema Designer<br>BigQuery JSON Schema]:::schema_ai
    G[🏗️ Agent: TF Architect<br>Terraform HCL Code]:::tf_ai
    H[🪄 Agent: DF Architect<br>Dataform SQLX Code]:::df_ai

    %% The Checkers
    Q1[🕵️‍♀️ Agent: Schema QA<br>Enforce STRING & Defaults]:::schema_ai
    Q2[🕵️‍♀️ Agent: TF QA<br>Enforce DRY & Locals]:::tf_ai
    Q3[🕵️‍♀️ Agent: DF QA<br>Enforce Tags & Syntax]:::df_ai

    I[🐙 GitHub API<br>Branch & Commit Injector]:::git
    J[🏆 Pull Request Created<br>Self-Healed Pipeline]:::git

    %% Connections with Rich Action Labels
    A -->| 📤 Raw File Uploaded | B
    B -->| ✅ Schema Match: Load Data | C
    B -->| 🚨 Drift/Missing Detected | D
    D -->| 📡 Dispatch Payload & Domain | E
    
    E -->| 🔍 Fetches/Generates YAML | X
    X -->| 📜 Injects Strict Rules | E

    E -->| 🏗️ Synthesizes Schema | F
    E -->| 🏗️ Synthesizes Infra | G
    E -->| 🏗️ Synthesizes Pipelines | H

    F -->| ⚖️ Audits Types | Q1
    G -->| ⚖️ Audits State | Q2
    H -->| ⚖️ Audits Logic | Q3

    %% Committing to Git (Including the Contract)
    Q1 & Q2 & Q3 -->| 📦 Stages Validated Code | I
    X -->| 📦 Stages Contract YAML | I
    I -->| 🚀 Opens Governance PR | J

    %% System Class Definitions (Light, Beautiful, Modern Colors)
    classDef storage fill:#E3F2FD,stroke:#64B5F6,stroke-width:2px,color:#0D47A1,rx:8px,ry:8px;
    classDef compute fill:#FFF8E1,stroke:#FBC02D,stroke-width:2px,color:#F57F17,rx:8px,ry:8px;
    classDef database fill:#E0F2F1,stroke:#4DB6AC,stroke-width:2px,color:#004D40,rx:8px,ry:8px;
    classDef pubsub fill:#FFF3E0,stroke:#FFB74D,stroke-width:2px,color:#E65100,rx:8px,ry:8px;
    classDef git fill:#F5F5F5,stroke:#9E9E9E,stroke-width:2px,color:#212121,rx:8px,ry:8px;

    %% Specialized Agent Swarm Colors
    classDef ai_brain fill:#F3E5F5,stroke:#BA68C8,stroke-width:2px,color:#4A148C,rx:8px,ry:8px;
    classDef contract fill:#FFF9C4,stroke:#FBC02D,stroke-width:2px,color:#795548,rx:8px,ry:8px;
    classDef schema_ai fill:#E0F7FA,stroke:#4DD0E1,stroke-width:2px,color:#006064,rx:8px,ry:8px;
    classDef tf_ai fill:#E8EAF6,stroke:#7986CB,stroke-width:2px,color:#283593,rx:8px,ry:8px;
    classDef df_ai fill:#FCE4EC,stroke:#F06292,stroke-width:2px,color:#880E4F,rx:8px,ry:8px;
```

For the complete component deep-dive, see [ARCHITECTURE.md](Documents/ARCHITECTURE.md).

---

## The Agent Swarm

Sentinel-Forge uses a **Maker / Checker** multi-agent architecture. Every synthesis agent is paired
with an independent QA agent that audits output before anything touches Git.

Tasks are routed across two model tiers:

| Tier | Model | Used For |
|---|---|---|
| Heavy | `gemini-2.5-flash` | TF Architect, DF Architect, Contract Engine (generate) |
| Lite | `gemini-2.5-flash-lite` | Scanners, Schema Designer, all QA Gatekeepers |

| Agent | Role |
|---|---|
| 📜 Contract Engine | Fetches or generates YAML Data Contracts from GCS before any code synthesis |
| 🕵️ Scanners & Routers | Reads live repo via GitHub API to infer domain, datasets, code style |
| 🧩 Schema Designer | Produces STRING-first BigQuery JSON schemas with `defaultValueExpression` |
| 🏗️ TF Architect | Synthesizes HCL with adaptive `locals` injection — no duplicate resource blocks |
| 🪄 DF Architect | Generates layered SQLX with dynamic casting, incremental models, temporal lineage |
| 🕵️ QA Gatekeepers ×3 | Independent Schema, Terraform, and Dataform review before Git commit |
| 🐙 GitOps Bot | Creates branch, commits all artifacts, opens PR as `sentinel-forge[bot]` |

---

## Metadata Tables

Three BigQuery tables in the `sentinel_audit` dataset power the platform's routing and observability.
All three are provisioned via the GitHub Actions pipeline — not the GCP Console.

### `ingestion_master` — Routing Rules

| Column | Type | Mode | Description |
|---|---|---|---|
| `file_pattern` | STRING | REQUIRED | Regex to match incoming files (e.g., `^sales_.*\.csv$`) |
| `target_dataset` | STRING | REQUIRED | Destination BigQuery dataset (e.g., `sentinel_raw_landing`) |
| `target_table` | STRING | REQUIRED | Destination table name (e.g., `raw_sales`) |
| `domain` | STRING | REQUIRED | Business domain (e.g., `Sales`) |
| `data_contracts` | STRING | NULLABLE | Associated data contract file (e.g., `Sales.yaml`) |
| `file_format` | STRING | NULLABLE | Expected format: CSV, JSON, AVRO, PARQUET, XLSX (default: CSV) |
| `delimiter` | STRING | NULLABLE | Field delimiter for CSVs (default: `,`) |
| `quote_char` | STRING | NULLABLE | Quote character for CSVs (e.g., `"`) |
| `skip_header_rows` | INTEGER | NULLABLE | Rows to skip (default: 1) |
| `write_disposition` | STRING | NULLABLE | `WRITE_APPEND` (default) or `WRITE_TRUNCATE` |
| `is_active` | BOOLEAN | REQUIRED | Enable/disable this routing rule |
| `created_at` | TIMESTAMP | NULLABLE | When this rule was created |

### `ingestion_log` — Ingestion Audit Ledger

| Column | Type | Mode | Description |
|---|---|---|---|
| `ingestion_id` | STRING | REQUIRED | Unique UUID (Cloud Function Execution ID) |
| `file_name` | STRING | REQUIRED | Name of the file processed (e.g., `orders_v1_20260213.csv`) |
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

### `ai_ops_log` — Agent Operations Audit

| Column | Type | Mode | Description |
|---|---|---|---|
| `operation_id` | STRING | REQUIRED | Unique UUID for this AI operation |
| `timestamp` | TIMESTAMP | REQUIRED | When the AI took action |
| `ai_agent_id` | STRING | NULLABLE | Identity of the agent (e.g., `Gemini-Raw-Architect`) |
| `resource_group` | STRING | NULLABLE | Category: Terraform, Dataform, Airflow |
| `resource_type` | STRING | NULLABLE | Type: Table, Schema, Model |
| `resource_id` | STRING | NULLABLE | Full path to the resource |
| `action_type` | STRING | NULLABLE | Action taken: `CREATE_PR`, `ABORT`, `ERROR` |
| `change_payload` | JSON | NULLABLE | Exact data/diff the AI generated |
| `outcome_status` | STRING | NULLABLE | `SUCCESS` or `FAILED` |
| `reference_link` | STRING | NULLABLE | URL to the GitHub PR or log |

---

## Dataform Configuration

```yaml
# workflow_settings.yaml
defaultProject:          sentinel-486707
defaultDataset:          sentinel_staging
defaultLocation:         us-central1
defaultAssertionDataset: sentinel_assertions
```

Dataform Core: `@dataform/core 3.0.47`

---

## Deployment

**All deployments happen exclusively through GitHub Actions.** Nothing is created or modified
via the GCP Console or CLI.

The CI/CD pipeline in `.github/workflows/` handles:
- Dataform compilation check on every Pull Request
- Dataform model deployment on merge to `master`
- Terraform plan/apply for infrastructure changes in `infra/`

See [ARCHITECTURE.md](Documents/ARCHITECTURE.md) for the full deployment flow.

---

## Enterprise Features

| Feature | Detail |
|---|---|
| Contract-driven synthesis | YAML Data Contracts are the source of truth for all type mapping and assertions |
| Anti-hallucination guardrails | Locked file paths, adaptive Terraform injection, strict template adherence |
| Quota protection | 3s traffic smoothing + exponential backoff with jitter (5s→10s→20s, 5 retries) |
| GitHub App auth | `sentinel-forge[bot]` identity, 1-hour tokens, RSA keys from Secret Manager |
| Domain threading | Business `domain` threaded from `ingestion_master` through Pub/Sub to all agents |

---

## Documentation

| Document | Description |
|---|---|
| [ARCHITECTURE.md](Documents/ARCHITECTURE.md) | Full component breakdown, agent internals, data flows, design decisions |
| [CONTRIBUTING.md](Documents/CONTRIBUTING.md) | Setup, code standards, prompt engineering guidelines, PR workflow |
| [SECURITY.md](Documents/SECURITY.md) | Security model, credential handling, responsible disclosure |
| [CHANGELOG.md](Documents/CHANGELOG.md) | Version history and release notes |

---

## Contributing

Contributions are welcome. Read [CONTRIBUTING.md](Documents/CONTRIBUTING.md) before opening a PR.

---

## License

[Apache 2.0](LICENSE)

---

*Vertex AI Gemini 2.5 · Google Cloud · BigQuery · Dataform 3.0.47 · Terraform · GitHub Actions*