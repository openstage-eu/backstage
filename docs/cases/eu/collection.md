# EU Collection Process

This page describes how EU legislative procedure data flows through the backstage pipeline.

## Step 1: Collect

The collect step queries the Cellar SPARQL endpoint to discover the current universe of procedures with interinstitutional reference codes.

### What the query returns

Each result row contains:

- **procedure reference**: The interinstitutional code (e.g., `2016_399`)
- **cellar URI**: The Cellar resource identifier (UUID)

### SPARQL snapshots

Each run saves the raw SPARQL result as a dated snapshot at `{case}/state/sparql_snapshots/{date}.json`, providing an audit trail of what the endpoint returned on a given date. Downstream steps (download, parse) read procedure references from this snapshot.

## Step 2: Download

For each procedure in the latest SPARQL snapshot, the pipeline downloads the RDF/XML tree notice from Cellar.

### Request format

```
POST https://publications.europa.eu/resource/procedure/{proc_ref}
Accept: application/rdf+xml;notice=tree
```

The tree notice format returns the complete procedure record including:

- Procedure metadata (type, title, dates)
- All events in the procedure timeline (committee opinions, plenary votes, Council adoptions, etc.)
- Event metadata (dates, responsible institutions)
- Document references linked to each event

### Download modes

| Mode | Behavior |
|------|----------|
| Full (default) | Download all procedures regardless of existing data |
| Incremental | Only download procedures without an existing RDF file in storage |

## Step 3: Parse

Downloaded RDF notices are parsed by [openbasement](https://github.com/openstage-eu/openbasement), which uses YAML-based extraction templates to convert RDF triples into structured dictionaries. The extracted data is then converted to typed [openstage](https://github.com/openstage-eu/openstage) `EUProcedure` models via the `from_openbasement()` adapter.

Parsed output on S3 is a flat `model_dump()` of the openstage model: `identifiers`, `title`, `events`, `procedure_type`, etc.

## Step 4: Package

Parsed procedure JSON files are validated into typed openstage models and packaged into a ZIP dataset using `Dataset.dump()`. The ZIP contains one JSON file per procedure plus a `metadata.json` with dataset-level provenance (name, version, creation date, pipeline dependency versions).

## Step 5: Publish

The packaged dataset is uploaded to Harvard Dataverse with structured metadata including:

- Dataset title and description
- Author and contact information
- Subject classification and keywords
- Geographic coverage
- Date of deposit

Publishing supports creating new datasets or adding new versions to existing ones. A dry-run mode allows previewing the metadata without actually publishing.
