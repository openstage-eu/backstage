"""
EU Legislative Data Collection Tools

Collection utilities for requesting data from European Union sources:
- EUR-Lex: Direct HTTP downloads of notices and documents
- Cellar: SPARQL queries against the EU Publications Office endpoint
"""

from __future__ import annotations

from .eurlex import download_notice, build_cellar_resource_url
from .cellar import get_procedure_references, execute_sparql_query

__all__ = [
    "download_notice",
    "build_cellar_resource_url",
    "get_procedure_references",
    "execute_sparql_query",
]
