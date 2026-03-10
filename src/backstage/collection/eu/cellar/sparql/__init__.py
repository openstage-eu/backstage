"""
Cellar SPARQL Interface
"""

import requests

from backstage.utils.user_agent import get_user_agent


def query(sparql_query: str,
         endpoint: str = "https://publications.europa.eu/webapi/rdf/sparql",
         timeout: int = 60,
         result_format: str = "csv") -> str:
    """
    Execute SPARQL query against Cellar endpoint.

    Args:
        sparql_query: SPARQL query string
        endpoint: SPARQL endpoint URL
        timeout: Request timeout in seconds
        result_format: Result format (json, xml, csv, tsv)

    Returns:
        Raw response content as string
    """

    format_map = {
        "json": "application/sparql-results+json",
        "xml": "application/sparql-results+xml",
        "csv": "text/csv",
        "tsv": "text/tab-separated-values"
    }

    accept_header = format_map.get(result_format, format_map["csv"])

    headers = {
        'User-Agent': get_user_agent(),
        'Accept': accept_header,
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.post(
        endpoint,
        data={'query': sparql_query},
        headers=headers,
        timeout=timeout
    )
    response.raise_for_status()

    return response.text


execute_sparql_query = query
