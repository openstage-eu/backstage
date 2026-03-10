# EU Data Universe

This page documents which EU legislative procedures are included in the openstage dataset, how they are identified, and what is excluded.

## Source

All procedure data is collected from the [EU Cellar repository](https://op.europa.eu/en/web/cellar), the semantic database operated by the EU Publications Office that stores metadata and content for all EU publications. Cellar exposes a public [SPARQL endpoint](https://publications.europa.eu/webapi/rdf/sparql) and an HTTP API for downloading individual resources.

The data model is defined by the [Common Data Model (CDM)](https://op.europa.eu/en/web/eu-vocabularies/cdm), the EU's formal ontology for describing official documents and legislative decision-making.

## What we collect

We collect **interinstitutional legislative procedures with a procedure reference code**. These are procedures that have been assigned a formal interinstitutional reference number in the format `YYYY/NNNN/TYPE` (e.g., `2016/0399/COD`).

In CDM terms, these are instances of `cdm:procedure_code_interinstitutional`, a subclass of `cdm:procedure_interinstitutional`.

### SPARQL query

The collection step runs a SPARQL query against the Cellar endpoint that retrieves all procedure reference codes. The query:

1. Selects all resources typed as `cdm:procedure_interinstitutional` or any of its subclasses
2. Follows `owl:sameAs` links to `/resource/procedure/` URIs
3. Extracts the procedure reference from the URI

Only procedures that have such a link are included. This filter naturally excludes the legacy entries described below.

### Universe size

As of March 2026, the query returns approximately **12,500 distinct procedure references**. This number grows slowly as new procedures are initiated.

## What we download

For each procedure reference, we download the **RDF/XML tree notice** from Cellar:

```
GET https://publications.europa.eu/resource/procedure/{proc_ref}
Accept: application/rdf+xml;notice=tree
```

The tree notice format includes the full procedure metadata with inline event data (dates, types, institutions) in a single request. This is the same format used by the [openbasement](https://github.com/openstage-eu/openbasement) extraction library.

## Identification: procedure references

Each procedure is identified by its **interinstitutional procedure reference**, stored in the format `YYYY_NNNN` (e.g., `2016_399`). This corresponds to the display format `YYYY/NNNN/TYPE` used on EUR-Lex (e.g., `2016/0399/COD`).

We use the procedure reference rather than the Cellar UUID as the primary identifier because:

- It is human-readable and meaningful (year + sequence number + procedure type)
- It is stable (once assigned, it never changes)
- It maps nearly 1:1 to Cellar entries (see [edge cases](edge-cases.md))
- It is the identifier used by the European Parliament's [Legislative Observatory (OEIL)](https://oeil.europarl.europa.eu/) and by EUR-Lex's procedure display pages

The Cellar UUID is stored alongside the procedure reference in the pipeline manifest for cross-referencing.

## What is excluded

### Legacy PEGASE entries (~30,400 procedures)

The CDM ontology defines a subclass `cdm:procedure_without_code_interinstitutional` for procedures that do not have a formal interinstitutional reference code. There are approximately 30,400 such entries in Cellar.

These are historical records migrated from the **PEGASE** database, a pre-digital system used by the EU institutions. They are identified only by internal PEGASE IDs (e.g., `pegase:123383`), date from the 1980s onward, and many are marked `do_not_index`. They lack the structured metadata available for modern procedures and are not relevant to the openstage research scope.

These entries are excluded automatically because our SPARQL query filters for `/resource/procedure/` links, which only coded procedures have.

### Split procedure class (0 instances)

The CDM ontology defines `cdm:procedure_interinstitutional-split` as a subclass for split procedures, but as of March 2026, no instances of this class exist in Cellar. Procedure splits are instead represented through suffix conventions on the procedure reference (see [edge cases](edge-cases.md)).
