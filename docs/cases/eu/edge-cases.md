# EU Known Edge Cases

This page documents known edge cases and data quality issues in the EU Cellar procedure data, based on empirical analysis of the SPARQL endpoint conducted in March 2026.

## Suffixed procedure references

52 procedures have references with letter suffixes appended to the base reference number. These represent distinct procedures derived from a common origin.

| Suffix | Meaning | Count |
|--------|---------|-------|
| A, B | Procedure splits (one proposal producing multiple acts) | ~22 |
| M | Modifications | 22 |
| R | Recasts | 8 |

### Example: procedure splits

Procedure `2016/0062(NLE)` (EU accession to the Istanbul Convention) was split into `2016/0062A(NLE)` and `2016/0062B(NLE)` because two separate Council decisions were needed, each covering different legal bases and competence areas.

In Cellar, each suffixed variant has its own cellar entry (distinct URI) and its own RDF tree notice. The base reference (`2016_62`) also retains its own entry. Our pipeline treats each as a separate procedure.

### Implication for researchers

When analyzing procedures, be aware that suffixed references are related to their base procedure. Depending on the research question, you may want to group them or treat them independently.

## Duplicate cellar entries

Three procedure references have two distinct cellar entries each:

| Procedure reference | Cellar entries |
|---|---|
| `2008_92` | 2 |
| `1991_336` | 2 |
| `1990_254` | 2 |

These appear to be legacy data quality issues. The duplicate entries are linked via `owl:sameAs` to the same `/resource/procedure/` URI, meaning they are genuinely duplicate representations of the same procedure. Both entries are typed as `procedure_code_interinstitutional`.

### How the pipeline handles this

The SPARQL query uses `SELECT DISTINCT` on the procedure reference, so duplicates are deduplicated at collection time. The pipeline stores one entry per procedure reference in the manifest.

## One cellar entry with two references

Cellar entry `bec28c55-69d7-4f0b-afdd-65272f6403e9` maps to both `2011_901` and `2011_901B`. This appears to be a case where a procedure split was not given its own cellar entry.

### How the pipeline handles this

Since we key on procedure reference, both `2011_901` and `2011_901B` will attempt to download RDF from their respective `/resource/procedure/` URLs. The resulting data may be identical or near-identical.

## Procedures without codes (~30,400)

Approximately 30,400 entries in Cellar are typed as `cdm:procedure_without_code_interinstitutional`. These are legacy records from the PEGASE database (pre-digital era), identified only by internal PEGASE IDs. They are excluded from our dataset. See [Data Universe](data-universe.md) for details.

## Empty or minimal RDF responses

Some procedure references may return empty or minimal RDF content from the Cellar endpoint. This can happen when:

- A procedure was recently initiated and metadata has not yet been fully ingested
- A procedure was withdrawn and its Cellar record was partially cleaned up

The pipeline records download failures in the manifest and tracks failure rates. A run is flagged if more than 10% of downloads fail.
