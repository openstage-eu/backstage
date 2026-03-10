# Pipeline Overview

The backstage pipeline runs in discrete steps. Each step reads its input from S3 and writes its output back, so steps can be run independently or as part of a full pipeline run.

## Steps

### 1. Collect

Queries the source data provider to discover the current universe of legislative procedures. Saves a dated snapshot for auditability and creates or updates per-procedure metadata files that track when each procedure was first and last seen.

### 2. Download

Fetches the raw data (typically RDF/XML) for each procedure from the source repository and uploads it to S3. Two modes are available:

- **Full** (default): re-download everything regardless of what exists
- **Incremental**: only download procedures without an existing file in storage

### 3. Parse

Processes downloaded data into typed [openstage](https://github.com/openstage-eu/openstage) models. The extraction method is case-specific (e.g. the EU case uses [openbasement](https://github.com/openstage-eu/openbasement) for RDF extraction). Parsed output on S3 is a flat `model_dump()` of the openstage model.

### 4. Package

Validates parsed procedure JSON into typed openstage models and builds a ZIP dataset using `Dataset.dump()`. The ZIP contains one JSON file per procedure plus a `metadata.json` with dataset-level provenance (name, version, creation date, pipeline dependency versions).

### 5. Publish

Uploads the packaged dataset to [Harvard Dataverse](https://dataverse.harvard.edu/) with structured metadata. Supports creating new datasets or adding new versions to existing ones.

## Pipeline schedule

The pipeline is designed to run monthly as a full rebuild on ephemeral compute. A GitHub Actions workflow triggers on the 1st of each month, provisions a Hetzner server, runs the pipeline, and tears down the server when complete.

## Case-specific logic

Each case (e.g. EU) defines its own collection endpoints, queries, download methods, and parsing templates. The pipeline orchestrator calls each case's steps in order with shared configuration flags (sample mode, dry run, download mode, etc.).

Adding a new case requires one folder with a `STEPS` dict and `DEFAULT_ORDER`, plus one line in the `CASES` registry in `flows/run.py`.

See the [EU case documentation](../cases/eu/data-universe.md) for details on EU-specific collection and parsing.
