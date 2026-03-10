# Running the Pipeline

## Prerequisites

backstage requires Python 3.11+ and uses [uv](https://docs.astral.sh/uv/) for environment management.

```bash
# Install dependencies
uv sync

# For parsing (requires openbasement)
uv sync --extra parsing

# Environment (add your Hetzner S3 credentials)
cp .env.example .env
source .env
```

## Full pipeline

Run all steps for a case:

```bash
python -m flows.run --case eu --sample          # sample mode (~10 procedures)
python -m flows.run --case eu                   # full production run
python -m flows.run --case eu --dry-run         # skip Dataverse publishing
```

## Incremental update

Collect new procedures and download only missing RDFs, then re-parse and package:

```bash
python -m flows.run --case eu --download-mode incremental
```

## Specific steps

Run only selected steps:

```bash
python -m flows.run --case eu --steps collect download
python -m flows.run --case eu --steps parse
python -m flows.run --case eu --steps publish --dry-run   # Dataverse publishing only
```

## Standalone step execution

Each step can also run independently:

```bash
python -m flows.eu.collect --sample
python -m flows.eu.download --sample
python -m flows.eu.parse --sample
python -m flows.eu.publish --dry-run
```

## All cases

Run the pipeline for all configured cases:

```bash
python -m flows.run --case all --sample --dry-run
```

## CLI flags

| Flag | Description |
|------|-------------|
| `--case` | Case to run (`eu`, `all`). Default: `eu` |
| `--steps` | Specific steps to run. Default: all steps for the case |
| `--sample` | Limit to ~10 procedures for testing |
| `--sample-limit N` | Override sample size (default: 10) |
| `--dry-run` | Skip destructive operations (Dataverse publishing) |
| `--download-mode` | `full` (re-download all) or `incremental` (only new). Default: `full` |
