# Changelog — Sentinel

All notable changes to this project are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

### Planned
- `dataform test` step in CI before mart deployment
- Terraform plan summary as PR comment on `infra/` changes
- SLO Monitor Agent — watches `ingestion_log` for at-risk contracts
- Lineage Agent — generates data lineage graphs from domain metadata

---

## [1.0.0] — 2026-03-22

### Added
- **`sentinel-ingestor`** Cloud Function — GCS-triggered schema validation, drift detection,
  quarantine, and Pub/Sub dispatch
- **`sentinel-forge`** Cloud Function — Vertex AI Gemini 2.5 multi-agent swarm
- **Contract Engine** — fetch or generate YAML Data Contracts from GCS
- **Schema Designer** — STRING-first BigQuery JSON schema synthesis
- **Terraform Architect** — adaptive `locals`-map HCL injection
- **Dataform Architect** — layered SQLX with `SAFE_CAST`, incremental models, temporal lineage
- **QA Gatekeepers ×3** — independent Schema, Terraform, and Dataform review agents
- **GitOps Bot** — `sentinel-forge[bot]` GitHub App authentication, branch/commit/PR workflow
- **Three metadata tables**: `ingestion_master`, `ingestion_log`, `ai_ops_log`
- **Tiered model routing**: `gemini-2.5-flash` (heavy synthesis) + `gemini-2.5-flash-lite` (routing/QA)
- **Quota protection**: 3-second traffic smoothing + exponential backoff with jitter (5 retries)
- **GitHub Actions CI/CD**: Dataform compile check on PR, Dataform deploy + Terraform apply on merge
- **Dataform project**: `sentinel-486707`, `@dataform/core 3.0.47`, `us-central1`
- **3-layer SQLX architecture**: sources → staging (`sentinel_staging`) → marts (`sentinel_mart`)
- **Assertion dataset**: `sentinel_assertions`

---

*For unreleased changes in progress, see open Pull Requests.*