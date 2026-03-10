"""
Pre-built SPARQL queries for Cellar operations.

All queries use the CDM ontology and target the EU Publications Office
SPARQL endpoint at publications.europa.eu/webapi/rdf/sparql.
"""

from typing import Optional


PREFIXES = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
"""


def get_procedure_references(limit: Optional[int] = None) -> str:
    """Get SPARQL query for all interinstitutional procedure references.

    Returns unique procedure references in YYYY_NNNN format extracted from
    Cellar's owl:sameAs links to /resource/procedure/ URIs. Only procedures
    with such a link are included, which excludes ~30K legacy PEGASE entries.

    Cellar URIs are not included in the query results because they are not
    needed for downloading (the download step fetches by proc_ref directly
    from publications.europa.eu/resource/procedure/{proc_ref}). Cellar UUIDs
    are available in the downloaded RDF tree notice itself. Excluding them
    also avoids duplicate rows caused by procedures that map to multiple
    Cellar resources.
    """
    return f"""
    {PREFIXES}

    SELECT DISTINCT ?procedure
    WHERE {{
      {{
        ?procURI rdf:type cdm:procedure_interinstitutional .
      }}
      UNION
      {{
        ?subClass rdfs:subClassOf cdm:procedure_interinstitutional .
        ?procURI rdf:type ?subClass .
      }}

      ?procURI owl:sameAs ?procCodeIRI .
      FILTER(regex(str(?procCodeIRI), "/resource/procedure/"))

      BIND(
        REPLACE(
          STR(?procCodeIRI),
          ".*/resource/procedure/",
          ""
        ) AS ?procedure
      )
    }}
    ORDER BY DESC(?procedure)
    {f"LIMIT {limit}" if limit else ""}
    """
