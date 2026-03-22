# Contributing to Sentinel 🛡️

Thank you for your interest in contributing. Contributions of all kinds are welcome — agent prompt
improvements, new capabilities, bug fixes, Dataform models, Terraform patterns, CI/CD enhancements,
and documentation.

> **Important:** All deployments happen exclusively through GitHub Actions workflows. Never apply
> Terraform or run Dataform commands against production from a local machine.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Where to Contribute](#where-to-contribute)
- [Getting Started Locally](#getting-started-locally)
- [Contribution Workflow](#contribution-workflow)
- [Code Standards — Python](#code-standards--python)
- [Dataform Standards](#dataform-standards)
- [Terraform Standards](#terraform-standards)
- [Prompt Engineering Guidelines](#prompt-engineering-guidelines)
- [Testing](#testing)
- [Opening Issues](#opening-issues)
- [Code of Conduct](#code-of-conduct)

---

## Architecture Overview

Before contributing, understand which layer you are working in.

**`sentinel-ingestor`** — Cloud Function, GCS-triggered. Validates incoming files against
`ingestion_master`, detects schema drift, quarantines files, publishes to Pub/Sub, writes
to `ingestion_log`.

**`sentinel-forge`** — Cloud Function, Pub/Sub-triggered. Runs the multi-agent swarm.
Writes outcomes to `ai_ops_log`.

**This repository** — The Dataform project (`definitions/`) and Terraform infrastructure (`infra/`)
that the forge writes to via Pull Requests. GitHub Actions in `.github/workflows/` deploys
everything on merge to `master`.

For the full technical breakdown, see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Where to Contribute

### High-Impact Areas

**Agent Prompt Engineering**
Prompt quality directly determines the reliability of generated Terraform, Dataform, and schema
output. If you have deep expertise in HCL, SQLX, or BigQuery schema design, improving the
Architect or QA Gatekeeper prompts is the highest-leverage contribution.
See [Prompt Engineering Guidelines](#prompt-engineering-guidelines).

**New Agent Capabilities**
The swarm is designed to be extended. Directions being explored:
- **Lineage Agent** — generates and maintains data lineage graphs from domain metadata
- **SLO Monitor Agent** — watches ingestion metrics in `ingestion_log` and flags at-risk contracts
- **Contract Reviewer Agent** — uses `ai_ops_log` patterns to suggest contract refinements

**Terraform Pattern Coverage**
The TF Architect currently handles `locals`-map injection. Contributions extending adaptive
injection to module references, variable files, and workspace-based environments are high value.

**Dataform Layer Coverage**
Improvements to SCD Type 2 patterns, multi-project Dataform configurations, or cross-domain
mart joins are welcome.

**CI/CD Pipeline**
Improvements to `.github/workflows/` — additional Dataform test steps, Terraform plan summaries
on PRs, Slack notifications on merge — are valuable contributions.

**Testing**
Unit tests for individual agent logic (mocking Vertex AI) and integration tests for the
ingestor's schema validation logic are the largest gaps.

**Documentation**
If you find an inaccuracy or gap in `ARCHITECTURE.md`, `CONTRIBUTING.md`, or `README.md`,
a docs PR is just as welcome as a code PR.

---

## Getting Started Locally

### Prerequisites

- Python 3.10+
- Google Cloud SDK authenticated to a non-production GCP project
- Node.js 18+
- Dataform CLI: `npm install -g @dataform/cli`
- Terraform CLI

### Setup

```bash
# Clone
git clone https://github.com/Vijayrajdev/sentinel.git
cd sentinel

# Install Dataform dependencies
npm install

# Verify Dataform compiles cleanly
dataform compile
# Expected: 0 errors

# For Cloud Function development (separate directories)
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Copy env config
cp .env.example .env
# Fill in your non-production GCP project, GitHub App credentials, model names
```

### Verify Dataform Setup

```bash
dataform compile          # must return 0 errors
dataform run --dry-run    # validates against your GCP project without executing
```

---

## Contribution Workflow

1. **Open an issue first** for non-trivial changes — new agents, significant prompt changes,
   new CI steps, architectural changes. Align on approach before investing time implementing.

2. **Fork and branch from `master`:**
   ```bash
   git checkout -b feat/your-feature-name
   git checkout -b fix/your-bug-description
   git checkout -b docs/your-improvement
   ```

3. **Make changes** following the standards in this guide.

4. **Test locally:**
   - Dataform changes: `dataform compile` must pass
   - Terraform changes: `terraform validate` in `infra/`
   - Python changes: run the unit test suite
   - Prompt changes: include before/after generated output examples in your PR

5. **Open a Pull Request against `master`.** Fill out the PR template completely.

6. **One approving review required** before merge.
   - Prompt changes must include before/after output examples
   - New Maker agents must include a corresponding QA Gatekeeper
   - New SQLX models must pass `dataform compile`
   - New Terraform resources must pass `terraform validate`

7. **After merge, GitHub Actions deploys automatically.** Do not run `terraform apply` or
   `dataform run` manually against production.

---

## Code Standards — Python

- **Formatter:** `black` — run before every commit
- **Linter:** `ruff` — run `make lint` to check
- **Type hints:** required on all new functions and methods
- **Docstrings:** Google-style, required on all public functions and classes
- **Logging:** use the `logging` module with structured JSON — no bare `print()` in Cloud Functions
- **No hardcoded credentials:** all secrets must come from environment variables or Secret Manager

```python
def call_llm_with_backoff(
    prompt: str,
    model: str,
    max_retries: int = 5
) -> str:
    """Call the Vertex AI LLM with exponential backoff on quota errors.

    Args:
        prompt: The prompt to send to the model.
        model: The Vertex AI model identifier.
        max_retries: Maximum retry attempts on ResourceExhausted.

    Returns:
        The model's text response.

    Raises:
        ResourceExhausted: If all retry attempts are exhausted.
    """
```

---

## Dataform Standards

All SQLX files in `definitions/` must follow these rules.

### File naming

| Layer | Pattern | Example |
|---|---|---|
| sources | `src_<table>.sqlx` | `src_crm_contacts.sqlx` |
| staging | `stg_<table>.sqlx` | `stg_crm_contacts.sqlx` |
| marts | `fct_<table>.sqlx` / `dim_<table>.sqlx` | `fct_crm_contacts.sqlx` |

### Folder structure

```
definitions/<layer>/<domain>/<entity>/<file>.sqlx
```

### Tag taxonomy — mandatory on all non-declaration files

Every `table` or `incremental` config block must have **exactly 3 tags**: `[domain, entity, layer]`

```javascript
config {
  type: "incremental",
  tags: ["crm", "contacts", "staging"],   // domain · entity · layer — always 3
}
```

`type: "declaration"` files must have **no** `tags` property. It is illegal in Dataform Core v3
and causes compilation failures.

### Casting rules

Always use `SAFE_CAST` — never bare `CAST`.
Always use `SAFE.PARSE_DATE` / `SAFE.PARSE_TIMESTAMP` — never bare parse functions.
A failed `SAFE_CAST` returns NULL. A failed `CAST` crashes the pipeline.

### Staging model checklist

Every staging model must include all of the following:

- [ ] `CURRENT_DATE() AS batch_date` — temporal lineage
- [ ] `QUALIFY ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY batch_date DESC) = 1` — deduplication
- [ ] `${ when(incremental(), \`WHERE batch_date > ...\`) }` — incremental filter
- [ ] `assertions: { nonNull: [...], uniqueKey: [...] }` in config block
- [ ] `partitionBy` and `clusterBy` in the `bigquery` config block

### Compile before submitting

```bash
dataform compile
# Must return 0 errors. PRs with compilation failures will not be merged.
```

---

## Terraform Standards

All HCL in `infra/` must follow these rules.

### Use `locals` maps — never standalone resource blocks

```hcl
# WRONG — do not add standalone resource blocks
resource "google_bigquery_table" "my_new_table" {
  table_id = "my_new_table"
  ...
}

# CORRECT — inject a new entry into the existing locals map
locals {
  tables_raw_crm = {
    # ... existing entries ...
    crm_new_table = {
      schema    = file("json/crm_new_table.json")
      partition = "ingestion_date"
      cluster   = ["id"]
    }
    crm_new_table_hist = {
      schema    = file("json/crm_new_table.json")  # same schema file — DRY
      partition = "ingestion_date"
      cluster   = ["id"]
    }
  }
}
```

### Every table must have a `_hist` counterpart

The `_hist` entry must reference the same JSON schema file as the main table — not a duplicate.

### Schema must use `file()` not inline JSON

```hcl
# WRONG
schema = jsonencode([{ name = "id", type = "STRING" }])

# CORRECT
schema = file("json/crm_new_table.json")
```

### Required attributes

- `deletion_protection = true` on all tables
- `project = var.project_id` — no hardcoded project IDs
- `time_partitioning` block present
- `clustering` set

### Validate before submitting

```bash
cd infra/
terraform init
terraform validate
# Must return "Success!" — PRs with validation failures will not be merged.
```

---

## Prompt Engineering Guidelines

When modifying or adding agent prompts:

**Be explicit about output format.** Always specify the exact structure required — JSON schema,
HCL map entry, SQLX file. Ambiguous format instructions are the primary cause of QA failures.

**State constraints as clearly as instructions.**
```
CONSTRAINTS:
- Do NOT add new resource blocks. Inject ONLY into the existing locals map.
- Do NOT add a tags property to type: "declaration" files. It is illegal in Dataform Core v3.
- Use SAFE_CAST for every cast. Never use bare CAST.
```

**Include a negative example for high-risk patterns.** Showing the wrong pattern alongside
the correct one significantly reduces hallucination rates for Terraform and Dataform generation.

**Include all context in every prompt.** Agents have no memory between executions.
Every prompt must carry the Data Contract, domain, schema diff, and relevant existing code.

**Test against edge cases.**
- Simple payload: 5 columns, clean types, existing domain
- Complex payload: 30+ columns, mixed types, nested JSON, new domain
- Edge case: empty payload, all-null columns, duplicate column names

**Document your reasoning in the PR.** Explain *why* the change improves output quality,
not just *what* changed. Include before/after generated code samples.

**Prompt structure to follow:**
```
ROLE: You are a [specific role].

CONTEXT:
- Domain: {domain}
- Table: {table_name}
- Data Contract: {contract_yaml}
- Existing repo structure: {repo_map}

TASK:
[Clear, specific task description]

CONSTRAINTS:
- [What to do]
- [What NOT to do]
- [Format requirement]

OUTPUT FORMAT:
[Exact format specification]
Return ONLY the output. No preamble, no explanation, no markdown fences.
```

---

## Testing

### Dataform

```bash
dataform compile                          # always required before PR
dataform run --dry-run                    # validates against GCP without executing
dataform run --tags=crm --dry-run         # tag-scoped dry run
```

### Terraform

```bash
cd infra/
terraform init
terraform validate                        # always required before PR
terraform plan                            # requires GCP credentials
```

### Python

```bash
pytest tests/unit/ -v                     # unit tests — no GCP credentials needed
pytest tests/integration/ -v \
  --gcp-project=your-dev-project-id       # integration tests — requires GCP
```

### Prompt testing

Include test payloads and generated outputs in the PR description for every prompt change.
Test against at minimum: a simple payload, a complex payload, and one edge case.

---

## Opening Issues

### Bug Reports

Please include:
- The Pub/Sub payload that triggered the swarm (sanitize sensitive data)
- Which agent produced incorrect output
- Actual output vs. expected output
- Values of `AI_MODEL_HEAVY` and `AI_MODEL_LITE` in use
- Relevant entries from `ingestion_log` or `ai_ops_log` (sanitized)

### Feature Requests

Describe the specific use case and why the current architecture doesn't support it.
Reference the relevant table schema columns (`ingestion_master`, `ingestion_log`, `ai_ops_log`)
where applicable — this helps frame the request in terms of what the platform already tracks.

---

## Code of Conduct

This project follows the [Contributor Covenant](https://www.contributor-covenant.org/).

Be respectful. Be constructive. Assume good faith.
Technical disagreements are welcome — personal attacks are not.

---

*For architecture details, see [ARCHITECTURE.md](ARCHITECTURE.md).*
*For setup and deployment, see [README.md](README.md).*