# Security Policy — Sentinel 🛡️

## Supported Versions

| Version | Supported |
|---|---|
| `master` branch | ✅ Active |
| Older branches | ❌ Not supported |

---

## Credential Handling

Sentinel is designed with zero-trust credential principles. The following rules are
**non-negotiable** and enforced at code review:

### GitHub App Authentication
- No Personal Access Tokens (PATs) anywhere in the codebase or CI configuration
- GitHub App private RSA keys are stored exclusively in **Google Secret Manager**
- Private keys are loaded at Cloud Function runtime — never written to disk, logged, or committed
- Installation tokens are short-lived (1 hour) and generated per execution

### GCP Credentials
- Cloud Functions use a dedicated **Service Account** with least-privilege IAM roles
- No `gcloud auth login` credentials or service account key JSON files in the repository
- Workload Identity Federation is used for GitHub Actions authentication to GCP —
  no service account key files in GitHub Secrets

### No Secrets in Code or Config
- No credentials, API keys, tokens, or passwords in any `.py`, `.tf`, `.yml`, `.yaml`, or `.json` file
- Environment variables reference Secret Manager paths — they do not contain secret values
- `.gitignore` excludes `.env`, `*.pem`, `*.json` key files, and `terraform.tfstate`

---

## IAM Roles — Least Privilege

### `sentinel-ingestor` Service Account

| Role | Reason |
|---|---|
| `roles/bigquery.dataEditor` | Read table schema, load data, write to audit tables |
| `roles/storage.objectViewer` | Read files from the GCS landing bucket |
| `roles/storage.objectCreator` | Write quarantined files to the archive bucket |
| `roles/pubsub.publisher` | Publish drift events to the schema-drift topic |

### `sentinel` Service Account

| Role | Reason |
|---|---|
| `roles/aiplatform.user` | Call Vertex AI Gemini models |
| `roles/bigquery.dataEditor` | Write to `ai_ops_log` |
| `roles/storage.objectViewer` | Fetch Data Contracts from GCS |
| `roles/secretmanager.secretAccessor` | Read GitHub App RSA private key |

### GitHub Actions Service Account

| Role | Reason |
|---|---|
| `roles/bigquery.admin` | Deploy BigQuery tables via Terraform |
| `roles/dataform.editor` | Execute Dataform runs |

---

## What to Do If You Find a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Please report security issues by emailing the maintainer directly. Include:
- A description of the vulnerability
- Steps to reproduce
- Potential impact
- Any suggested remediation if known

We will acknowledge receipt within 48 hours and aim to provide a fix or mitigation within 7 days
for critical issues.

---

## Security Anti-Patterns This Project Explicitly Avoids

| Anti-pattern | What we do instead |
|---|---|
| PATs for GitHub automation | GitHub App with scoped permissions and 1-hour tokens |
| Hardcoded project IDs in Terraform | `var.project_id` everywhere |
| Service account key JSON files | Workload Identity Federation for CI, Secret Manager for runtime |
| `terraform apply` from local machines | All applies happen in GitHub Actions |
| Direct BigQuery access from agent code | Scoped service accounts, no admin roles on agent functions |
| Storing secrets in `.env` files in the repo | `.env` is in `.gitignore`; secrets come from Secret Manager |