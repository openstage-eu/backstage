# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

backstage is the operational backbone of the openstage ecosystem. It handles:
- Data collection from EU legislative sources (SPARQL + HTTP)
- Raw notice storage in S3
- Parsing via openbasement and mapping to openstage models
- Dataset packaging via openstage `Dataset.dump()` and publishing to Harvard Dataverse

Completes the theater metaphor: openstage (front stage), openbasement (basement), backstage (operations).

## Project Structure

```
backstage/
  src/backstage/                    # installable package
    collection/                     # data collection by jurisdiction
      eu/
        eurlex/                     # EUR-Lex HTTP downloads
        cellar/sparql/              # SPARQL queries against Cellar
    utils/                          # shared utilities (s3, retry, logging, user_agent, versions)

  flows/                            # operational scripts (NOT in the package)
    run.py                          # CLI entry point + registry + orchestrator
    shared/                         # generic steps shared across cases
      dataset_config.py             # per-case metadata config dicts
      package.py                    # build_dataset_package()
      publish.py                    # publish_dataset() to Dataverse
    eu/                             # EU-specific pipeline steps
      collect.py                    # discover procedures via SPARQL
      download.py                   # download notices from EUR-Lex
      parse.py                      # parse with openbasement -> openstage models
      package.py                    # thin wrapper -> shared.package (+ pipeline versions)
      publish.py                    # thin wrapper -> shared.publish + EU_CONFIG

  deploy/                           # Terraform + cloud-init
  .github/workflows/                # GitHub Actions (monthly pipeline, cleanup, notify)
```

## Setup

```bash
# Install with uv (Python 3.11 required)
uv sync                             # core deps (prefect, boto3, requests, structlog)
uv sync --extra parsing             # adds openbasement + openstage (needed for parse/package)
uv sync --extra dev                 # adds pytest

# Environment
cp .env.example .env                # add your Hetzner S3 credentials
source .env
```

## Running the Pipeline

```bash
# Full pipeline in sample mode
python -m flows.run --case eu --sample

# Incremental update (only download new procedures)
python -m flows.run --case eu --download-mode incremental

# Specific steps only
python -m flows.run --case eu --steps collect download

# All cases
python -m flows.run --case all --sample --dry-run

# Individual step standalone
python -m flows.eu.collect --sample
python -m flows.eu.download --sample
python -m flows.eu.parse --sample
python -m flows.eu.publish --dry-run
```

Key flags: `--sample` (limit to ~10 procedures), `--dry-run` (skip destructive operations), `--steps` (run specific steps only), `--case` (jurisdiction or "all"), `--download-mode` (`full` or `incremental`).

## Testing

```bash
pytest                              # no test files exist yet; pytest is configured
```

## Architecture

**No database. No persistent server.** Monthly full rebuild on ephemeral compute.

### Pipeline Flow

```
GitHub Actions cron (1st of month)
  -> Terraform creates Hetzner CX53
    -> cloud-init installs deps, runs pipeline
      -> For each case (eu, ...):
        1. collect   (SPARQL -> snapshot on S3)
        2. download  (Cellar RDF tree -> {case}/procedures/raw/*.rdf)
        3. parse     (raw data -> openstage models -> {case}/procedures/parsed/*.json)
        4. package   (parsed JSON -> Dataset.dump() -> ZIP on S3)
        5. publish   (S3 -> Harvard Dataverse)
    -> Server self-destructs
```

### STEPS Registry

Each case folder exports a `STEPS` dict and `DEFAULT_ORDER` list. The orchestrator (`flows/run.py`) uses lazy import via a `CASES` registry:

```python
CASES = {
    "eu": "flows.eu",
}
```

Adding a new case = create a folder with `__init__.py` exporting `STEPS` and `DEFAULT_ORDER`, add one line to `CASES`. Package and publish are shared utilities in `flows/shared/` that each case wires in via thin wrappers.

### Inter-step Data Flow via S3

Steps communicate through S3, not in-memory return values. SPARQL snapshots are the coordination mechanism:

```
collect  -> SPARQL query for procedure references
         -> saves {case}/state/sparql_snapshots/{date}.json
download -> reads proc refs from latest SPARQL snapshot, fetches RDF tree from Cellar
         -> writes {case}/procedures/raw/{proc_ref}.rdf
parse    -> always parses every RDF file (no skip logic, monthly full rebuild)
         -> writes {case}/procedures/parsed/{proc_ref}.json (openstage model_dump output)
package  -> reads parsed JSON, validates into openstage models, calls Dataset.dump()
         -> writes {case}/datasets/{release}/openstage-{case}-{release}.zip + metadata.json
publish  -> reads {case}/datasets/{release}/*.zip
         -> publishes to Dataverse
```

The orchestrator just calls steps in order with shared config (case, sample_mode, sample_limit, dry_run, download_mode). No data piping.

### Prefect Local Mode

Flows run as plain Python scripts. Without `PREFECT_API_URL`, Prefect uses an ephemeral in-memory backend. No server, no database, no Docker.

You get: retries, `.map()` parallelism, structured logging.

### Task Runners and Batching

| Step | Task Runner | Batch Size | Rationale |
|------|-------------|-----------|-----------|
| collect | (none) | -- | Sequential: SPARQL query + snapshot write |
| download | ThreadPoolTaskRunner(16) | 200 | I/O-bound (HTTP + S3) |
| parse | ProcessPoolTaskRunner(16) | 500 | CPU-bound (RDF parsing); bypasses GIL |

Batched `.map()` calls prevent Prefect's ephemeral SQLite from returning 503 errors at high concurrency. Each ProcessPool subprocess spawns its own ephemeral SQLite server (expected behavior).

### Dependency Version Tracking

`backstage.utils.versions.get_pipeline_versions()` resolves git commit SHAs for openbasement and openstage (via PEP 610 `direct_url.json`). The package step records these in the dataset-level `metadata.json` for reproducibility.

### Primary Key: Procedure Reference

Procedure references (YYYY_NNNN format, e.g. 2024_0176) are the primary identifier throughout the pipeline. The SPARQL query extracts them from Cellar's `owl:sameAs` links to `/resource/procedure/` URIs. Cellar UUIDs are stored in metadata but not used as keys.

### S3 Key Structure

```
s3://openstage-data/{case}/
  procedures/raw/{proc_ref}.rdf     # RDF tree from Cellar
  procedures/parsed/{proc_ref}.json # openstage model_dump() output
  datasets/{release}/openstage-{case}-{release}.zip   # release = YYYY.MM
  datasets/{release}/metadata.json
  state/sparql_snapshots/{date}.json  # raw SPARQL results for reproducibility
  state/last_run.json               # run summary
  state/publication.json            # Dataverse publication audit record
```

### Import Paths

| What | Import |
|------|--------|
| S3 utilities | `from backstage.utils import s3` |
| Retry decorator | `from backstage.utils.retry import with_retry` |
| SPARQL query | `from backstage.collection.eu.cellar.sparql import query` |
| SPARQL queries | `from backstage.collection.eu.cellar.sparql.queries import get_procedure_references` |
| EUR-Lex download | `from backstage.collection.eu.eurlex.download import download_notice` |
| Pipeline versions | `from backstage.utils.versions import get_pipeline_versions` |
| Dataset config factory | `from backstage.dataset_config import make_dataset_config` |
| Dataset packaging | `from flows.shared.package import build_dataset_package` |
| Dataverse publish | `from flows.shared.publish import publish_dataset` |

### SPARQL Layer

`backstage.collection.eu.cellar.sparql.query()` executes raw SPARQL against `publications.europa.eu/webapi/rdf/sparql`. Pre-built queries live in `queries.py` using CDM ontology prefixes. The query function returns raw response text (CSV by default), parsed by the calling flow.

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| No database | Monthly full rebuild makes state tracking unnecessary |
| Prefect local mode | Flows work as plain scripts with retries and .map() |
| Ephemeral compute | ~0.06 EUR/month vs persistent server costs |
| S3 prefix per case | Simple separation, no plugin architecture |
| Terraform + cloud-init | Server self-destructs after pipeline completes |
| src layout + hatchling | Clean separation of installable package from operational flows |
| uv for env management | Fast, reliable dependency resolution and lockfile |
| Steps communicate via S3 | Each step is independently runnable, no data piping needed |
| STEPS registry per case | Adding a case = one folder + one line in CASES dict |
| ProcessPool for parse | CPU-bound RDF parsing; ThreadPool caps at ~2 effective cores due to GIL |
| Batched .map() | Prevents Prefect ephemeral SQLite 503 errors at high concurrency |
| Dependency versions in metadata | Dataset metadata.json records openbasement/openstage commit SHAs for reproducibility |
| openstage models in parse output | Parse writes model_dump() directly; package validates and builds Dataset |

## Secret Management

Secrets flow: **GitHub Actions Secrets -> Terraform `-var` args -> `templatefile()` -> cloud-init `.env` on server (mode 0600) -> `os.environ` in flows**.

See `.env.example` for all required variables. For local dev, Hetzner S3 credentials are needed. For production, secrets are defined in GitHub Actions and passed through Terraform. The server's own ID for self-destruct comes from Hetzner's metadata service (`169.254.169.254`), not a secret.

`PIPELINE_GITHUB_TOKEN` must be a fine-grained PAT scoped to `contents:write` on backstage only (for `repository_dispatch` callback).

## Task Tracking

- `tasks/todo.md` -- current work items
- `tasks/lessons.md` -- patterns and mistakes to avoid
